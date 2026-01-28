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
import re
from enum import Enum

DECIMAL64_PRECISION = [1, 18]
DECIMAL128_PRECISION = [19, 38]

class TypeEnum(Enum):
    STRING = "STRING"
    INT = "INT"
    BYTE = "BYTE"
    LONG = "LONG"
    DOUBLE = "DOUBLE"
    MAP = "MAP"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    NESTED_FUNCTIONS = "NESTED_FUNCTIONS"
    DECIMAL64 = "DECIMAL64"
    DECIMAL128 = "DECIMAL128"
    PARTITION = "PARTITION"


TYPE_PATTERNS = [
    (re.compile(r"^-?\d+$"), TypeEnum.INT.value),
    (re.compile(r"^-?\d+\.\d+$"), TypeEnum.DOUBLE.value),
    (re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)[eE][+-]?\d+"), TypeEnum.DOUBLE.value),
    (re.compile(r".*#\d+L"), TypeEnum.LONG.value),
    (re.compile(r"\d{4}-\d{2}-\d{2}"), TypeEnum.DATE.value),
    (re.compile(r"NULL", re.I), TypeEnum.INT.value),
    (re.compile(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(\.\d{1,3})?$"), TypeEnum.TIMESTAMP.value),
    (re.compile(r"TRUE|FALSE", re.I), TypeEnum.BOOLEAN.value)
]

class TypeMatcher:

    @staticmethod
    def extract_param_type(input_data, param_type_mapping):
        read_schema_pattern = re.compile(r'ReadSchema: struct<(.*?)>')
        # 分割键值对并解析成字典
        for schema_match in read_schema_pattern.finditer(input_data):
            inner_content = schema_match.group(1).strip()

            param_pairs = re.split(r",\s*(?![^()]*\))(?!(?:[^<]*>))", inner_content)
            for param in param_pairs:
                param = param.strip()
                if not param:
                    continue
                # 按冒号分割（只分割第一个冒号，避免value里有冒号的情况）
                param_spilt = re.split(r':\s*', param, maxsplit=1)
                if len(param_spilt) != 2:
                    continue
                name, par_type = param_spilt
                spark_type = TypeMatcher.change_to_spark_type(par_type)
                param_type_mapping[name] = spark_type

    @staticmethod
    def judge_param_type(param, param_type_mapping):
        if (param.startswith('"') and param.endswith('"')) or \
            (param.startswith("'") and param.endswith("'")):
            return TypeEnum.STRING.value
        for pattern, param_type in TYPE_PATTERNS:
            if pattern.fullmatch(param):
                return param_type
        ori_param = re.sub(r"#\d+(L)*", "", param)
        if TypeMatcher.is_nested_function(ori_param):
            return TypeEnum.NESTED_FUNCTIONS.value
        if ori_param in param_type_mapping:
            return param_type_mapping[ori_param]
        return TypeEnum.PARTITION.value

    @staticmethod
    def get_input_type(params, param_type_mapping):
        input_type = []
        for param in params:
            input_type.append(TypeMatcher.judge_param_type(param, param_type_mapping))
        return input_type

    @staticmethod
    def is_nested_function(param):
        param = param.strip()
        # 判断是否为嵌套函数，字母下划线紧跟(
        return bool(re.match(r'^[a-zA-Z_]\w*\s*\(', param))

    @staticmethod
    def change_to_spark_type(par_type):
        if par_type.upper().startswith("MAP"):
            return TypeEnum.MAP.value
        if par_type.upper() == "TINYINT":
            return TypeEnum.BYTE.value
        if par_type.upper() == "BIGINT":
            return TypeEnum.LONG.value
        if par_type.upper().startswith("DECIMAL"):
            return TypeMatcher.handle_decimal_type(par_type.upper())
        return par_type.upper()

    @staticmethod
    def handle_decimal_type(par_type):
        decimal_pattern = r"DECIMAL\((\d+),\s*(\d+)\)"
        match = re.search(decimal_pattern, par_type, re.I)
        if not match:
            return TypeEnum.DECIMAL64.value
        try:
            precision = int(match.group(1))
        except Exception as e:
            print("Change decimal precision error: " + str(e))
            return TypeEnum.PARTITION.value
        if DECIMAL64_PRECISION[1] >= precision >= DECIMAL64_PRECISION[0]:
            return TypeEnum.DECIMAL64.value
        if DECIMAL128_PRECISION[1] >= precision >= DECIMAL128_PRECISION[0]:
            return TypeEnum.DECIMAL128.value
        return TypeEnum.PARTITION.value