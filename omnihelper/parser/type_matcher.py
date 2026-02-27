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

from omnihelper.enum.function_enum import FunctionEnum
from omnihelper.enum.type_enum import TypeEnum
from omnihelper.util.func_util import extract_cast_param

DECIMAL64_PRECISION = [1, 18]
DECIMAL128_PRECISION = [19, 38]

NOT_SUPPORTED_TYPE = [TypeEnum.PARTITION.value, TypeEnum.NESTED_FUNCTIONS.value]

PREDICATE_EXPR = ["<", "<=", "<>", "=", "==", ">", ">=", FunctionEnum.IF.value]

DATE_LITERAL = ["yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss"]

TYPE_PATTERNS = [
    (re.compile(r"^-?\d+$"), TypeEnum.INT.value),
    (re.compile(r"^-?\d+\.\d+$"), TypeEnum.DOUBLE.value),
    (re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)[eE][+-]?\d+"), TypeEnum.DOUBLE.value),
    (re.compile(r".*#\d+L"), TypeEnum.LONG.value),
    (re.compile(r"\d{4}-\d{2}-\d{2}"), TypeEnum.DATE.value),
    (re.compile(r"NULL", re.I), TypeEnum.NULL.value),
    (re.compile(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(\.\d{1,3})?$"), TypeEnum.TIMESTAMP.value),
    (re.compile(r"TRUE|FALSE", re.I), TypeEnum.BOOLEAN.value)
]

class TypeMatcher:

    @staticmethod
    def extract_param_type(input_data, param_type_mapping):
        read_schema_pattern = re.compile(r'ReadSchema: struct<(.*)>')
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
                spark_type = TypeMatcher.switch_param_type(par_type)
                param_type_mapping[name.lower()] = spark_type

    @staticmethod
    def judge_param_type(param, param_type_mapping):
        if TypeMatcher.is_string_literal(param):
            return TypeEnum.STRING.value

        ori_param = re.sub(r"#\d+(L)*", "", param)
        if TypeMatcher.is_nested_function(ori_param):
            return TypeEnum.NESTED_FUNCTIONS.value

        ori_param = re.sub(r"^-\s*", "", ori_param)
        if ori_param.lower() in param_type_mapping:
            return param_type_mapping[ori_param.lower()]
        return TypeEnum.PARTITION.value

    @staticmethod
    def get_input_type(params, param_type_mapping, ori_sql, pair):
        input_type = []
        func_name = pair.get("func")
        for param in params:
            param_type = TypeMatcher.judge_param_type(param, param_type_mapping)

            if not param_type in NOT_SUPPORTED_TYPE:
                input_type.append(param_type)
                continue

            if TypeMatcher.is_string_in_ori_sql(param, ori_sql):
                # 判断是否在原SQL中为字符串，physical plan中将其优化
                input_type.append(TypeEnum.STRING.value)
                continue

            is_match = False
            for pattern, match_type in TYPE_PATTERNS:
                # 判断参数是否为默认参数类型
                if pattern.fullmatch(param):
                    is_match = True
                    input_type.append(match_type)

            if is_match:
                # 如果已经匹配到则不执行后续逻辑
                continue

            if param_type == TypeEnum.NESTED_FUNCTIONS.value and TypeMatcher.is_pure_cast(param):
                # 判断参数是否为cast嵌套函数，如果是，参数类型可以判断为as后的类型
                cast_params = extract_cast_param(param)
                if cast_params:
                    param_type = TypeMatcher.switch_param_type(cast_params[1])

            m = re.search(r"input\[\s*[^,]+,\s*([^,\]]+)", param)
            if m:
                # 匹配到形如input[0, int, true]的input参数
                param_type = TypeMatcher.switch_param_type(m.group(1))

            input_type.append(param_type)

        if func_name in PREDICATE_EXPR:
            # 判断参数是否为比较表达式或if函数，如果是且有一侧类型不确定，则将类型判断为确定的类型
            input_type = TypeMatcher.replace_predicate_partition(input_type)

        if func_name == FunctionEnum.CAST.value and len(params) == 2:
            # 如果函数本身为嵌套函数，则第二个参数的类型是它本身
            input_type[1] = TypeMatcher.switch_param_type(params[1])

        return input_type

    @staticmethod
    def is_string_literal(param):
        return (param.startswith('"') and param.endswith('"')) or \
            (param.startswith("'") and param.endswith("'"))

    @staticmethod
    def is_string_in_ori_sql(param, ori_sql):
        escaped_single_param = "'" + param.replace("\\", "\\\\") + "'"
        escaped_double_param = '"' + param.replace("\\", "\\\\") + '"'
        param_literal_list = [f"'{param}'", f'"{param}"', f'{escaped_single_param}', f'{escaped_double_param}']
        return any(literal in ori_sql for literal in param_literal_list)

    @staticmethod
    def is_date_literal(param):
        return param in DATE_LITERAL

    @staticmethod
    def is_nested_function(param):
        param = param.strip()
        # 判断是否为嵌套函数，字母下划线紧跟(
        return bool(re.match(r'^[a-zA-Z_]\w*\s*\((.*)\)$', param))

    @staticmethod
    def switch_param_type(par_type):
        if par_type.upper().startswith("ARRAY"):
            return TypeEnum.ARRAY.value
        if par_type.upper().startswith("MAP"):
            return TypeEnum.MAP.value
        if par_type.upper() == "TINYINT":
            return TypeEnum.BYTE.value
        if par_type.upper() == "SMALLINT":
            return TypeEnum.SHORT.value
        if par_type.upper() == "BIGINT":
            return TypeEnum.LONG.value
        if par_type.upper() == "TIMESTAMP_NTZ":
            return TypeEnum.TIMESTAMP.value
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

    @staticmethod
    def replace_predicate_partition(args):
        supported = None
        for arg in args:
            if arg not in NOT_SUPPORTED_TYPE and arg != TypeEnum.NULL.value:
                supported = arg
                break
        if supported is None:
            return args
        return [supported] * len(args)

    @staticmethod
    def is_pure_cast(expr):
        expr = expr.strip()
        if not expr.lower().startswith("cast("):
            return False

        depth = 0
        for i, ch in enumerate(expr[4:], start=4):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    tail = expr[i + 1:].strip()
                    return tail == ""
        return False