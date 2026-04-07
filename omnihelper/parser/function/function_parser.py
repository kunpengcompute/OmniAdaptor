"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.
"""
import json
import os
import re
import hashlib
from collections import defaultdict

from omnihelper.parser.function.function_builder import FunctionBuilder
from omnihelper.parser.function.function_checker import FunctionChecker
from omnihelper.parser.type_matcher import TypeMatcher
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.func_util import NOT_SUPPORTED_TYPE

class FunctionParser:

    DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "omni_function_dictionary.json")
    UDF_DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "udf_dictionary.json")

    def __init__(self):
        self.function_list = []
        self.omni_functions = []
        self.udf_list = []
        self.user_defined_functions = []
        self.partial_func_mapping = {}
        self.all_funcs = []
        self.func_pattern = ""
        self.function_builder = None
        self.load_func_list()

    def load_func_list(self):
        try:
            with open(self.DICTIONARY_PATH, "r", encoding="utf-8") as f:
                self.function_list = json.load(f)
        except Exception as e:
            raise Exception("Failed to load the functions list: " + str(e))

        if os.path.exists(self.UDF_DICTIONARY_PATH):
            try:
                with open(self.UDF_DICTIONARY_PATH, "r", encoding="utf-8") as f:
                    self.udf_list = json.load(f)
            except Exception as e:
                raise Exception("Failed to load the user defined function: " + str(e))
        self.omni_functions = [func.get("func_name").lower() for func in self.function_list]
        self.user_defined_functions = [func.get("func_name").lower() for func in self.udf_list]
        self.all_funcs = self.omni_functions + self.user_defined_functions
        self.func_pattern = re.compile("({})\\((.*)".format("|".join(map(re.escape, self.all_funcs))), re.I)
        for func in self.function_list:
            if func.get("hash_agg_func"):
                self.partial_func_mapping[func["func_name"]] = func["hash_agg_func"]
        self.function_builder = FunctionBuilder(self.func_pattern, self.all_funcs)

    def parse_event(self, event, column_type):
        """
        单事件表达式、函数解析核心逻辑
        :return:
        """
        if not self.function_list:
            return []
        analysis_result = []
        param_type_mapping = {}
        alias_map = {}
        param_type_mapping.update(column_type)
        physical_plan = event.get("physical plan")
        if not physical_plan:
            return []
        if event.get("node metrics"):
            TypeMatcher.extract_param_type(event.get("node metrics"), param_type_mapping)
        update_physical_plan = self.preprocess_physical_plan(physical_plan)
        operator_blocks = self.split_operators(update_physical_plan)

        if not operator_blocks:
            analysis_result = self.parse_physical_plan(update_physical_plan, event, param_type_mapping, alias_map)
        for block in operator_blocks:
            analysis_result.extend(self.parse_physical_plan(block, event, param_type_mapping, alias_map))

        return self.count_func_times(analysis_result)

    def preprocess_physical_plan(self, physical_plan):
        preprocess_phy_plan = []
        split_phy_plan = physical_plan.split("\n")
        for line in split_phy_plan:
            if not line or line.strip().startswith("+- =="):
                continue
            if line.startswith("Condition"):
                # 行内容为condition时，后面会跟着一个括号
                condition_match = re.match(r"Condition\s*:\s*\((.*)\)\s*", line)
                if condition_match:
                    line = condition_match.group(1)
            processed_line = self.extract_bracket_content(line)
            preprocess_phy_plan.append(processed_line)
        return preprocess_phy_plan

    def extract_bracket_content(self, line):
        """
        去除算子参数的名称字段，例如Input、Keys、Output等字段，只取值
        例如：Input [12]: [sum#167]，仅保留sum#167
        """
        line = line.strip()

        # pattern: 任意前缀 + 数字 + : + [内容]，非贪婪匹配最外层方括号内容
        m = re.search(r"\[[^\[\]]*\]\s*:\s*\[(.*)\]$", line)
        if m:
            return m.group(1).strip()

        return line

    def split_operators(self, physical_plan):
        result = []
        current_block = []

        for line in physical_plan:
            if re.match(r"^\(\d+\)", line.strip()):
                if current_block:
                    result.append(current_block)
                    current_block = []
            current_block.append(line)

        if current_block:
            result.append(current_block)

        return result

    def parse_physical_plan(self, physical_plan, event, param_type_mapping, alias_map):
        analysis_result = []
        for line in physical_plan:
            if "ReadSchema" in line:
                # 更新参数类型映射表
                TypeMatcher.extract_param_type(line, param_type_mapping)
            CommonUtil.extract_alias_map(line, alias_map)
        for line in physical_plan:
            func_pairs = self.function_builder.search_func_expr_pairs(line)
            if not func_pairs:
                continue

            for pair in func_pairs:
                func_name = pair.get("func")
                params = pair.get("params")

                input_type = TypeMatcher.get_input_type(params, param_type_mapping, event, pair,
                                                        self.function_builder, alias_map, 0)
                function_checker = FunctionChecker(self.function_list, self.udf_list)
                is_not_supported_func = function_checker.check_support_status(func_name, params, input_type,
                                                                              event.get("original query"))
                if not is_not_supported_func:
                    continue
                not_supported_func = self.build_not_supported_func(func_name, event, input_type, params, line)
                analysis_result.append(not_supported_func)
        return analysis_result

    def build_not_supported_func(self, func_name, event, input_type, params, line):
        func_name = self.partial_func_mapping[func_name] if func_name in self.partial_func_mapping else func_name
        sql_hash = hashlib.sha256(event.get("original query").encode("utf-8")).hexdigest()[-6:]
        not_supported_params = [
            param
            for idx, param in enumerate(params)
            if input_type[idx] in NOT_SUPPORTED_TYPE
        ]
        return {
            "func_name": func_name,
            "sql_hash": sql_hash,
            "input": input_type,
            "not_supported_line": line if any(param_type in NOT_SUPPORTED_TYPE for param_type in input_type) else "",
            "not_supported_params": not_supported_params
        }

    def count_func_times(self, event_result):
        counter = defaultdict(int)
        not_supported_line = defaultdict(list)
        not_supported_params = defaultdict(list)

        for item in event_result:
            key = (item["func_name"], item["sql_hash"], tuple(item["input"]))
            counter[key] += 1
            if item.get("not_supported_line"):
                not_supported_line[key].append(item["not_supported_line"])
            if item.get("not_supported_params"):
                not_supported_params[key].append(item["not_supported_params"])

        update_event_result = []
        for (func_name, sql_hash, input_type), times in counter.items():
            update_event_result.append({
                "func_name": func_name,
                "sql_hash": sql_hash,
                "input": input_type,
                "times": times,
                "is_udf": True if func_name.lower() in self.user_defined_functions else False,
                "not_supported_line": not_supported_line[(func_name, sql_hash, input_type)],
                "not_supported_params": not_supported_params[(func_name, sql_hash, input_type)]
            })
        return update_event_result