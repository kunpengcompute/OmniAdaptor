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

from omnihelper.util.common_util import CommonUtil

DEFAULT_TYPE = "PARTITION"
NESTED_FUNCTIONS = "NESTED FUNCTIONS"
STRING = "STRING"
INT = "INT"
MAP = "MAP"
DECIMAL64 = "DECIMAL64"
DECIMAL128 = "DECIMAL128"
DECIMAL64_PRECISION = [1, 18]
DECIMAL128_PRECISION = [19, 38]

class FunctionParser:

    DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "omni_function_dictionary.json")

    def __init__(self, app_data_path):
        self.app_data_path = app_data_path
        self.function_list = []
        self.omni_functions = []
        self.partial_func_mapping = {}

    def parse_event_log(self):
        print("Start parsing expr and function...")
        self.load_func_list()
        if not self.function_list:
            return []
        json_files = self.filter_json_files()
        evaluate_result = []
        for file in json_files:
            print("Start parsing file: " + str(file))
            file_analysis_res = self.process_file(file)
            if not file_analysis_res:
                continue
            evaluate_result.extend(file_analysis_res)
            print("Finished parsing file: " + str(file))
        print("Parsing expr and function completed.")

        return evaluate_result

    def load_func_list(self):
        try:
            with open(self.DICTIONARY_PATH, "r", encoding="utf-8") as f:
                self.function_list = json.load(f)
        except Exception as e:
            print("Failed to load the functions list: " + str(e))
            return
        self.omni_functions = [func.get("func_name") for func in self.function_list]
        self.func_pattern = re.compile("({})\\((.*)".format("|".join(map(re.escape, self.omni_functions))))
        self.expr_pattern = re.compile("(.+?)\\s+({})\\s+(.+)".format("|".join(map(re.escape, self.omni_functions))))
        for func in self.function_list:
            if func.get("hash_agg_func"):
                self.partial_func_mapping[func["func_name"]] = func["hash_agg_func"]

    def filter_json_files(self):
        """
        过滤出文件夹内所有json文件
        :return: json文件列表
        """
        json_files = []
        if not os.path.isdir(self.app_data_path):
            raise Exception(f"{self.app_data_path} does not exist.")

        for root, dirs, files in os.walk(self.app_data_path):
            for file in files:
                if file.lower().endswith(".json"):
                    full_path = os.path.join(root, file)
                    json_files.append(os.path.realpath(full_path))
        return json_files

    def process_file(self, file_path):
        """
        函数、表达式解析主流程
        :return: 单文件所有事件分析结果
        """
        file_name = os.path.basename(file_path)
        application_id = file_name.split(".")[0]
        try:
            with open(file_path, "r") as f:
                app_data = json.load(f)
        except Exception as e:
            print(f"Failed to load file: {file_name}, ex: {e}")
            return []
        analysis_result = []
        for event in app_data:
            event_result = self.parse_event(event)
            if not event_result:
                continue
            update_event_result = self.count_func_times(event_result)
            key = application_id + "_" + event.get("executionId")
            analysis_result.append({key: update_event_result})
        return analysis_result

    def parse_event(self, event):
        """
        单事件表达式、函数解析核心逻辑
        :return:
        """
        analysis_result = []
        physical_plan = event.get("physical plan")
        if not physical_plan:
            return ""
        update_physical_plan = self.preprocess_physical_plan(physical_plan)
        func_pairs = []
        for line in update_physical_plan:
            func_pairs.extend(self.match_expr_pattern(line))
        if not func_pairs:
            return ""

        for pair in func_pairs:
            func_name = pair.get("func")
            params = pair.get("params")
            param_type_mapping = self.get_param_type_mapping(event.get("node_metrics"), "".join(update_physical_plan))
            input_type = self.get_input_type(params, param_type_mapping)
            if not input_type:
                continue

            is_not_supported_func = self.evaluate_support_status(func_name, params, input_type)
            if not is_not_supported_func:
                continue
            not_supported_func = self.build_not_supported_func(func_name, event, input_type)
            analysis_result.append(not_supported_func)
        return analysis_result

    def preprocess_physical_plan(self, physical_plan):
        preprocess_res = []
        start_analyze = False
        for line in physical_plan.split("\n")[2:]:
            if not start_analyze:
                if line.startswith("(1)"):
                    start_analyze = True
                else:
                    continue
            if not line:
                continue
            preprocess_res.append(line)
        return preprocess_res

    def match_expr_pattern(self, physical_plan):
        func_pair = []
        if self.func_pattern.search(physical_plan):
            for call in self.extract_function_calls(physical_plan):
                func = self.extract_func_name(call)
                params = self.handle_special_param(self.extract_func_args(call))
                if not func in self.omni_functions:
                    continue
                func_pair.append({"func": func, "params": params})

        for match in self.expr_pattern.finditer(physical_plan):
            func = match.group(2)
            left_param = self.extract_left_param(physical_plan, func)
            right_param = self.extract_right_param(physical_plan, func)
            params = self.handle_special_param([left_param, right_param])
            if not func in self.omni_functions:
                continue
            func_pair.append({"func": func, "params": params})

        return func_pair

    def extract_function_calls(self, s):
        """
        提取行内的所有函数调用
        :return:
        """
        results = []
        i = 0
        n = len(s)

        while i < n:
            m = re.match(r'[a-zA-Z_]\w*', s[i:])
            if not m:
                i += 1
                continue

            func = m.group()
            j = i + len(func)

            if j >= n or s[j] != "(":
                i += 1
                continue

            # 括号配平
            depth = 0
            k = j
            while k < n:
                if s[k] == "(":
                    depth += 1
                elif s[k] == ")":
                    depth -= 1
                    if depth == 0:
                        results.append(s[i:k + 1])
                        i = k + 1
                        break
                k += 1
            else:
                i += 1
        return results

    def extract_func_name(self, call):
        """
        提取函数调用的函数名
        :return: 函数名
        """
        m = re.match(r'\s*([a-zA-Z_]\w*)\s*\(', call)
        return m.group(1) if m else ""

    def extract_func_args(self, call):
        """
        提取函数调用的参数值
        :return: 参数列表
        """
        l = call.find("(")
        r = call.rfind(")")
        if l == -1 or r == -1 or r<= l:
            return []

        args_str = call[l + 1:r]
        args = []
        buf = []
        depth = 0
        for ch in args_str:
            if ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                args.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            args.append("".join(buf).strip())
        return args

    def extract_left_param(self, physical_plan, func):
        """
        返回表达式左边最靠近的参数或函数
        :return: 左边参数
        """
        lt_index = physical_plan.find(func)
        if lt_index == -1:
            return None

        depth = 0
        end = lt_index
        for i in range(lt_index - 1, -1, -1):
            ch = physical_plan[i]
            if ch == ")":
                depth += 1
            elif ch == "(":
                depth -= 1
            if depth < 0:
                return physical_plan[i + 1:end].strip()
        left = physical_plan[:lt_index].rstrip()
        return left.split()[-1]

    def extract_right_param(self, physical_plan, func):
        """
        返回表达式右边最靠近的参数或函数
        :return: 右边参数
        """
        lt_index = physical_plan.find(func)
        if lt_index == -1:
            return None

        physical_plan = physical_plan[lt_index + 1:]
        depth = 0
        buf = []
        for i, ch in enumerate(physical_plan):
            if ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
                if depth == 0:
                    break
            elif ch.isspace() and depth == 0:
                break
            else:
                buf.append(ch)

        if not buf:
            tokens = physical_plan.strip().split()
            return tokens[0] if tokens else None
        return "".join(buf).strip()

    def handle_special_param(self, params):
        process_params = []
        for param in params:
            if "#" in param and not self.func_pattern.match(param):
                param = re.sub(r"#\d+", "", param)
            process_params.append(param.strip())
        return process_params

    def get_param_type_mapping(self, node_metrics, physical_plan):
        node_type = {}
        if node_metrics:
            node_type = self.extract_param_type(node_metrics)
        physical_plan_type = self.extract_param_type(physical_plan)
        return {**node_type, **physical_plan_type}

    def extract_param_type(self, input_data):
        read_schema_pattern = re.compile(r'ReadSchema: struct<(.*?)>')
        # 分割键值对并解析成字典
        param_type = {}
        for schema_match in read_schema_pattern.finditer(input_data):
            inner_content = schema_match.group(1).strip()

            param_pairs = re.split(r',\s*', inner_content)
            for param in param_pairs:
                param = param.strip()
                if not param:
                    continue
                # 按冒号分割（只分割第一个冒号，避免value里有冒号的情况）
                param_spilt = re.split(r':\s*', param, maxsplit=1)
                if len(param_spilt) != 2:
                    continue
                name, par_type = param_spilt
                if par_type.lower().startswith("decimal"):
                    par_type = self.handle_decimal_type(par_type)
                param_type[name] = par_type.upper()
        return param_type

    def handle_decimal_type(self, par_type):
        decimal_pattern = r"decimal\((\d+),\s*(\d+)\)"
        match = re.search(decimal_pattern, par_type, re.I)
        if not match:
            return DECIMAL64
        precision = int(match.group(1))
        if DECIMAL64_PRECISION[1] >= precision >= DECIMAL64_PRECISION[0]:
            return DECIMAL64
        if DECIMAL128_PRECISION[1] >= precision >= DECIMAL128_PRECISION[0]:
            return DECIMAL128
        return DEFAULT_TYPE

    def evaluate_support_status(self, func_name, params, input_type):
        for rule in self.function_list:
            if not rule.get("func_name") == func_name:
                continue
            if not rule.get("is_support_func"):
                # 表示是不支持的函数，需要记录到结果中
                return True
            if rule.get("param_count") and len(params) != rule.get("param_count"):
                # 表示是不支持的参数个数，需要记录到结果中
                return True
            for type in input_type:
                if type in rule.get("no_support_type") or type in [DEFAULT_TYPE, NESTED_FUNCTIONS]:
                    # 只要有一个不支持，就判定为不支持
                    return True
        return False

    def get_input_type(self, params, param_type_mapping):
        input_type = []
        for param in params:
            if param.startswith("\"") and param.endswith("\""):
                input_type.append(STRING)
                continue
            if param.startswith("'") and param.endswith("'"):
                input_type.append(STRING)
                continue
            if re.match(r"^\d+$", param):
                input_type.append(INT)
                continue
            if param.upper().startswith(MAP):
                input_type.append(MAP)
                continue
            if param_type_mapping.get(param):
                input_type.append(param_type_mapping[param])
                continue
            if self.is_nested_function(param):
                input_type.append(NESTED_FUNCTIONS)
                continue
            input_type.append(DEFAULT_TYPE)
        return input_type

    def is_nested_function(self, param):
        param = param.strip()
        return bool(re.match(r'^[a-zA-Z_]\w*\s*\(', param))

    def build_not_supported_func(self, func_name, event, input_type):
        func_name = self.partial_func_mapping[func_name] if func_name in self.partial_func_mapping else func_name
        sql_hash = hashlib.sha256(event.get("original query").encode("utf-8")).hexdigest()[-6:]
        return {
            "func_name": func_name,
            "sql_hash": sql_hash,
            "input": input_type
        }

    def count_func_times(self, event_result):
        counter = defaultdict(int)

        for item in event_result:
            key = (item["func_name"], item["sql_hash"], tuple(item["input"]))
            counter[key] += 1

        update_event_result = []
        for (func_name, sql_hash, input_type), times in counter.items():
            update_event_result.append({
                "func_name": func_name,
                "sql_hash": sql_hash,
                "input": input_type,
                "times": times
            })
        return update_event_result
