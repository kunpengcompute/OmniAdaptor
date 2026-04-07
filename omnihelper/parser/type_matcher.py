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
import copy
import re

from omnihelper.enum.function_enum import FunctionEnum
from omnihelper.enum.type_enum import TypeEnum
from omnihelper.parser.function.expr_tree import ExprTree
from omnihelper.parser.function.return_type_parser import ReturnTypeParser
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.func_util import extract_cast_param, replace_predicate_partition, strip_outer_parens

DECIMAL64_PRECISION = [1, 18]
DECIMAL128_PRECISION = [19, 38]

NOT_SUPPORTED_TYPE = [TypeEnum.PARTITION.value, TypeEnum.NESTED_FUNCTIONS.value]

PREDICATE_EXPR = ["<", "<=", "<>", "=", "==", ">", ">=", "+", "-", "*", "/",
                  FunctionEnum.IF.value, FunctionEnum.COALESCE.value]

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
            # 如果参数开始有截断，则不取，仅取截断前的内容
            i = next((i for i, param in enumerate(param_pairs) if "..." in param), len(param_pairs))
            param_pairs = param_pairs[:i]
            for param in param_pairs:
                param = param.strip()
                if not param:
                    continue
                # 按冒号分割（只分割第一个冒号，避免value里有冒号的情况）
                param_spilt = re.split(r':\s*', param, maxsplit=1)
                if len(param_spilt) != 2:
                    continue
                name, par_type = param_spilt
                param_type_mapping[name.lower()] = par_type

    @staticmethod
    def analyse_op_param_type(param, param_type_mapping, alias_map, event, function_builder):
        if TypeMatcher.is_string_literal(param):
            return TypeEnum.STRING.value

        ori_param = re.sub(r"#\d+(L)*", "", param)
        if TypeMatcher.is_nested_function(ori_param):
            return TypeEnum.NESTED_FUNCTIONS.value

        ori_param = re.sub(r"^-\s*", "", ori_param)
        if ori_param.lower() in param_type_mapping:
            return TypeMatcher.switch_param_type(param_type_mapping[ori_param.lower()])

        alias_param = re.sub(r"\[\d+\]$", "", param)
        if alias_param in alias_map:
            real_param = alias_map[alias_param]
            real_type = TypeMatcher.analyse_function_param_type(real_param, param_type_mapping, event,
                                                                function_builder, alias_map, 0)
            return real_type
        return TypeEnum.PARTITION.value

    @staticmethod
    def get_input_type(params, param_type_mapping, event, pair, function_builder, alias_map, depth):
        if depth > 10:
            return TypeEnum.PARTITION.value
        input_type = []
        func_name = pair.get("func")
        for param in params:
            param_type = TypeMatcher.analyse_function_param_type(param, param_type_mapping, event,
                                                                 function_builder, alias_map, depth + 1)
            input_type.append(param_type)

        if func_name in PREDICATE_EXPR:
            # 判断参数是否为比较表达式或if函数，如果是且有一侧类型不确定，则将类型判断为确定的类型
            input_type = replace_predicate_partition(input_type)

        if func_name == FunctionEnum.CAST.value and len(params) == 2:
            # 如果函数本身为嵌套函数，则第二个参数的类型是它本身
            input_type[1] = TypeMatcher.switch_param_type(params[1])

        return input_type

    @staticmethod
    def analyse_function_param_type(param, param_type_mapping, event, function_builder, alias_map, depth):
        if depth > 10:
            return TypeEnum.PARTITION.value
        ori_sql = event.get("original query")
        if TypeMatcher.is_string_literal(param):
            return TypeEnum.STRING.value

        if TypeMatcher.is_string_in_ori_sql(param, ori_sql):
            # 判断是否在原SQL中为字符串，physical plan中将其优化
            return TypeEnum.STRING.value

        if TypeMatcher.is_date_literal(param):
            return TypeEnum.DATE.value

        if TypeMatcher.is_time_zone_literal(param):
            return TypeEnum.STRING.value

        ori_param = re.sub(r"#\d+(L)*", "", param)
        ori_param = re.sub(r"^(-|distinct)\s*", "", ori_param)

        if ori_param.lower() in param_type_mapping:
            return TypeMatcher.switch_param_type(param_type_mapping[ori_param.lower()])

        for pattern, match_type in TYPE_PATTERNS:
            # 判断参数是否为默认参数类型
            if pattern.fullmatch(param):
                return match_type

        m = re.search(r"input\[\s*[^,]+,\s*([^,\]]+)", param)
        if m:
            # 匹配到形如input[0, int, true]的input参数
            return TypeMatcher.switch_param_type(m.group(1))

        map_match = re.match(r'^([a-zA-Z_]\w*)#(\d+)\[([^\]]+)\]$', param)

        if map_match:
            col = map_match.group(1)
            if col.lower() in param_type_mapping:
                map_type = param_type_mapping[col.lower()]
                if "," in map_type:
                    value_type = map_type[map_type.index(",") + 1: -1].strip()
                    return TypeMatcher.switch_param_type(value_type)

        if TypeMatcher.is_pure_cast(param):
            # 判断参数是否为cast嵌套函数，如果是，参数类型可以判断为as后的类型
            cast_params = extract_cast_param(param)
            if cast_params:
                return TypeMatcher.switch_param_type(cast_params[1])

        nested_function_type = TypeMatcher.get_func_return_type(param, param_type_mapping, event,
                                                                function_builder, alias_map, depth + 1)

        alias_param = re.sub(r"\[\d+\]$", "", param)
        if nested_function_type in NOT_SUPPORTED_TYPE and alias_param in alias_map:
            real_param = alias_map[alias_param]
            real_type = TypeMatcher.analyse_function_param_type(real_param, param_type_mapping, event,
                                                                function_builder, alias_map, depth + 1)
            return real_type

        return nested_function_type

    @staticmethod
    def is_string_literal(param):
        return (param.startswith('"') and param.endswith('"')) or \
            (param.startswith("'") and param.endswith("'"))

    @staticmethod
    def is_string_in_ori_sql(ori_param, ori_sql):
        param_set = [ori_param, f'%{ori_param}', f'%{ori_param}%', f'{ori_param}%']
        for param in param_set:
            escaped_single_param = "'" + param.replace("\\", "\\\\") + "'"
            escaped_double_param = '"' + param.replace("\\", "\\\\") + '"'
            param_literal_list = [f"'{param}'", f'"{param}"', f'{escaped_single_param}', f'{escaped_double_param}']
            if any(literal in ori_sql for literal in param_literal_list):
                return True
        return False

    @staticmethod
    def is_date_literal(param):
        return param in DATE_LITERAL

    @staticmethod
    def is_nested_function(param):
        strip_param = strip_outer_parens(re.sub(r"#\d+(L)*", "", param)).strip()
        # 判断是否为嵌套函数，字母下划线紧跟(
        return (bool(re.match(r'^[a-zA-Z_]\w*\s*\((.*)\)$', strip_param))
                or strip_param.lower().startswith(FunctionEnum.IF.value)
                or strip_param.lower().startswith(FunctionEnum.CASE.value))

    @staticmethod
    def is_time_zone_literal(param):
        return param == "Some(Asia/Shanghai)"

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
        if par_type.upper() == "TIMESTAMP_LTZ":
            return TypeEnum.TIMESTAMP.value
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

    @staticmethod
    def get_func_return_type(ori_param, param_type_mapping, event, function_builder, alias_map, depth):
        is_nested_function = TypeMatcher.is_nested_function(ori_param)
        nested_function_type = TypeEnum.NESTED_FUNCTIONS.value if is_nested_function else TypeEnum.PARTITION.value
        pairs = function_builder.search_func_expr_pairs(ori_param)
        if not pairs:
            return nested_function_type

        for pair in pairs:
            params = pair.get("params", [])
            input_type = TypeMatcher.get_input_type(params, param_type_mapping, event, pair,
                                                    function_builder, alias_map, depth + 1)
            pair["input_type"] = input_type

        stack = []
        return_type_parser = ReturnTypeParser()
        duplicate_return_types = []
        for pair in pairs:
            input_type = pair.get("input_type", [])
            if TypeEnum.NESTED_FUNCTIONS.value in input_type:
                # 如果函数内包含嵌套函数，先入栈
                stack.append(pair)
                continue
            return_type = return_type_parser.analyse_return_type(pair)
            if not return_type:
                continue
            if "/" in return_type:
                parts = return_type.split("/")
                pair["return_type"] = parts[0]
                for re_type in parts[1:]:
                    new_pair = copy.deepcopy(pair)
                    new_pair["return_type"] = re_type
                    duplicate_return_types.append(new_pair)
            else:
                pair["return_type"] = return_type

        base_funcs = [pair for pair in pairs if pair.get("return_type")]
        while stack:
            pair = stack.pop()
            params = pair.get("params", [])
            input_type = pair.get("input_type", [])
            for idx, param_type in enumerate(input_type):
                if param_type != TypeEnum.NESTED_FUNCTIONS.value:
                    continue
                nested_param = params[idx]
                for func in base_funcs:
                    if func.get("type") == "expr" and func.get("func") == FunctionEnum.IN.value:
                        continue
                    if func.get("func") in nested_param and all(param in nested_param for param in func.get("params")):
                        # 通过已经获取到返回值的函数参数中，找到包含该函数的嵌套函数，并将其赋值
                        input_type[idx] = func.get("return_type")
                        break
            return_type = return_type_parser.analyse_return_type(pair)
            if not return_type:
                continue
            if "/" in return_type:
                parts = return_type.split("/")
                pair["return_type"] = parts[0]
                for re_type in parts[1:]:
                    new_pair = copy.deepcopy(pair)
                    new_pair["return_type"] = re_type
                    duplicate_return_types.append(new_pair)
            else:
                pair["return_type"] = return_type

        pairs.extend(duplicate_return_types)

        if is_nested_function:
            # 如果原始参数是函数
            outer_func_name = TypeMatcher.find_outer_func_name(strip_outer_parens(ori_param))
            if outer_func_name:
                filtered = [pair for pair in pairs if pair.get("func") == outer_func_name]
                if filtered:
                    # 匹配到函数名相同，参数名最长的pair
                    filtered_pair = max(filtered, key=lambda x:len("".join(x["params"])))
                    nested_function_type = filtered_pair.get("return_type", TypeEnum.NESTED_FUNCTIONS.value)
        else:
            # 如果原始参数是表达式
            for pair in pairs:
                func_name = pair.get("func")
                params = pair.get("params")
                if pair.get("type") == "expr":
                    if func_name == FunctionEnum.IN.value:
                        continue
                    expr_set = ExprTree(params[0], func_name, params[1]).build()
                    if re.sub(r"#\d+(L)*", "", ori_param).strip() not in expr_set:
                        continue
                    nested_function_type = pair.get("return_type", TypeEnum.PARTITION.value)

        return nested_function_type

    @staticmethod
    def find_outer_func_name(ori_param):
        outer_func_name = ""
        match = re.match(r'^([a-zA-Z_]\w*)\s*\((.*)\)', ori_param)
        if match:
            outer_func_name = match.group(1).lower()
        if ori_param.lower().startswith(FunctionEnum.CASE.value):
            outer_func_name = FunctionEnum.CASE.value
        if ori_param.lower().startswith(FunctionEnum.IF.value):
            outer_func_name = FunctionEnum.IF.value
        return outer_func_name

    @staticmethod
    def parse_param_list(param_match, param_type_mapping, alias_map, event, function_builder):
        """
        解析输入列表，处理包含嵌套括号的复杂表达式
        :param param_match: 正则匹配结果对象
        :param param_type_mapping: 参数类型映射字典
        :return: 解析后的输入列表
        """
        if not param_match:
            return []

        param_list = []
        for item in CommonUtil.split_complex_items(param_match.group(1)):
            stripped_item = item.strip()
            if not stripped_item:
                continue

            # 处理 AS 语法，只取左边部分
            if ' AS ' in stripped_item:
                stripped_item = stripped_item.split(' AS ')[0].strip()

            param_type = TypeMatcher.analyse_op_param_type(stripped_item, param_type_mapping, alias_map, event, function_builder)
            if param_type.upper().startswith("DECIMAL"):
                param_type = TypeEnum.DECIMAL.value
            param_list.append(param_type)
        return param_list