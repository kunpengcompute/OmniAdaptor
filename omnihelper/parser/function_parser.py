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
DECIMAL64 = "DECIMAL64"
DECIMAL128 = "DECIMAL128"
DECIMAL64_PRECISION = [1, 18]
DECIMAL128_PRECISION = [19, 38]

class FunctionParser:

    DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "omni_function_dictionary.json")
    FUNC_PATTERN = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)\s*\(([^()]*)\)')

    def __init__(self, app_data_path):
        self.app_data_path = app_data_path
        self.function_list = []
        self.omni_functions = []
        self.partial_func_mapping = {}

    def parse_event_log(self):
        print("Start parsing expr and function...")
        self.load_func_list()
        if not self.load_func_list:
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
        pattern = "(.+?)\\s+({})\\s+(.+)".format("|".join(map(re.escape, self.omni_functions)))
        self.expr_pattern = re.compile(pattern)
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
        func_pair = self.match_expr_pattern(physical_plan)
        if not func_pair:
            return ""

        for pair in func_pair:
            func_name = pair.get("func")
            params = pair.get("params")
            param_type_mapping = self.get_param_type_mapping(event.get("node_metrics"), physical_plan)
            input_type = self.get_input_type(params, param_type_mapping)
            if not input_type:
                continue

            is_not_supported_func = self.evaluate_support_status(func_name, params, input_type)
            if not is_not_supported_func:
                continue
            not_supported_func = self.build_not_supported_func(func_name, event, input_type)
            analysis_result.append(not_supported_func)
        return analysis_result

    def match_expr_pattern(self, physical_plan):
        func_pair = []
        for match in self.FUNC_PATTERN.finditer(physical_plan):
            func = match.group(1)
            params = self.handle_special_param(match.group(2).split(","))
            if not func in self.omni_functions:
                continue
            if any(not self.is_valid_param(param) for param in params):
                continue
            func_pair.append({"func": func, "params": params})

        for match in self.expr_pattern.finditer(physical_plan):
            func = match.group(2)
            params = self.handle_special_param([match.group(1), match.group(3)])
            if not func in self.omni_functions:
                continue
            if any(not self.is_valid_param(param) for param in params):
                continue
            func_pair.append({"func": func, "params": params})

        return func_pair

    def handle_special_param(self, params):
        process_params = []
        for param in params:
            if "#" in param:
                param = param.split("#")[0]
            process_params.append(param.strip())
        return process_params

    def is_valid_param(self, param):
        param_pattern = re.compile(
            r"^('[^']*'|\"[^\"]*\"|-?\d+(\.\d+)?|[A-Za-z_][A-Za-z0-9_#$@]*)$"
        )
        match = param_pattern.match(param)
        return match is not None

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
            if param_type_mapping.get(param):
                input_type.append(param_type_mapping[param])
                continue
            if self.is_nested_function(param):
                input_type.append(NESTED_FUNCTIONS)
                continue
            input_type.append(DEFAULT_TYPE)
        return input_type

    def is_nested_function(self, param):
        param = param.replace(" ", "")
        nested_function_pattern = r'^([a-zA-Z_]\w*)\(([a-zA-Z_]\w*)\(([^)]+)\)\)$'
        match = re.fullmatch(nested_function_pattern, param)
        return match is not None

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
