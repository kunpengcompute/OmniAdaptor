# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# OmniAdaptor is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

"""
Flink 函数分析模块

负责分析算子描述中的函数调用，包括：
- 提取函数表达式
- 解析函数参数类型
- 分析不支持的函数
"""

import re

from omnihelper.flink.function.function_parse import FlinkFunctionParser
from omnihelper.flink.operator.utils import ExpressionConverter


class FunctionAnalyzer:
    """
    函数分析器

    核心职责：
    1. 从描述文本中提取函数表达式
    2. 解析函数参数类型
    3. 分析不支持的函数及其参数
    """

    def __init__(self, type_resolver):
        self.function_parser = FlinkFunctionParser()
        self.type_resolver = type_resolver

    @staticmethod
    def _is_operator_func(func_name):
        """判断函数名是否为操作符形式"""
        if not func_name:
            return False
        if func_name.upper() in ('AND', 'OR'):
            return True
        return len(func_name) <= 2 and not any(c.isalpha() for c in func_name)

    @staticmethod
    def _is_prefix_type_func(func_name):
        """判断函数名是否为前缀类型函数（无括号格式）"""
        if not func_name:
            return False
        prefix_types = {"TIMESTAMP", "INTERVAL", "DATE", "TIME", "ROW"}
        return func_name.upper() in prefix_types

    @staticmethod
    def _is_inside_quotes(text, pos):
        """判断指定位置是否在引号内"""
        if pos < 0 or pos >= len(text):
            return False
        before_text = text[:pos]
        single_quotes = before_text.count("'")
        double_quotes = before_text.count('"')
        return single_quotes % 2 == 1 or double_quotes % 2 == 1

    @staticmethod
    def _is_inside_sarg(text, pos):
        """判断指定位置是否在 Sarg[...] 内部"""
        if pos < 0 or pos >= len(text):
            return False

        sarg_count = 0
        i = 0
        while i < pos:
            if i + 4 <= len(text) and text[i:i+4].upper() == 'SARG':
                if i + 5 <= len(text) and text[i+4] == '[':
                    sarg_count += 1
                    i += 5
                    continue
            if text[i] == ']':
                sarg_count -= 1
            i += 1
        return sarg_count > 0

    def _extract_operator_operands_from_desc(self, func_name, description):
        """从描述中提取操作符的左右操作数"""
        if not description or not func_name:
            return None, None

        clean_desc = re.sub(r'<[^>]+>[\s:*\+\-]*', ' ', description)
        is_logical_op = func_name.upper() in ('AND', 'OR')

        if is_logical_op:
            op_pattern = re.compile(r'\b' + re.escape(func_name) + r'\b', re.I)
        else:
            op_pattern = re.compile(re.escape(func_name), re.I)

        for match in op_pattern.finditer(clean_desc):
            pos = match.start()
            op_end = match.end()

            if func_name == '=' and op_end < len(clean_desc) and clean_desc[op_end] == '[':
                continue
            if func_name == '=' and pos > 0 and clean_desc[pos - 1].isalpha():
                continue
            if func_name in ('=', '<', '>') and pos > 0 and clean_desc[pos - 1] in ('<', '>', '!'):
                continue
            if func_name in ('=', '<', '>') and op_end < len(clean_desc) and clean_desc[op_end] == '=':
                continue
            if func_name == '*' and pos > 0 and clean_desc[pos - 1] == '(' and op_end < len(clean_desc) and clean_desc[op_end] == ')':
                continue
            if FunctionAnalyzer._is_inside_quotes(clean_desc, pos):
                continue
            if FunctionAnalyzer._is_inside_sarg(clean_desc, pos):
                continue

            left_start = pos - 1
            depth = 0
            while left_start >= 0:
                ch = clean_desc[left_start]
                if ch in (')', ']', '}'):
                    depth += 1
                elif ch in ('(', '[', '{'):
                    if depth == 0:
                        left_start += 1
                        break
                    depth -= 1
                elif depth == 0 and ch == '=' and not is_logical_op:
                    if left_start + 1 < len(clean_desc) and clean_desc[left_start + 1] == '[':
                        depth += 1
                        left_start -= 1
                        continue
                    left_start += 1
                    break
                elif depth == 0 and ch == ',':
                    left_start += 1
                    break
                elif depth == 0:
                    if left_start >= 2 and clean_desc[left_start-2:left_start+1].upper() == 'AND':
                        left_start += 1
                        break
                    if left_start >= 1 and clean_desc[left_start-1:left_start+1].upper() == 'OR':
                        left_start += 1
                        break
                left_start -= 1
            else:
                left_start = 0
            if left_start < 0:
                left_start = 0

            right_end = op_end
            depth = 0
            while right_end < len(clean_desc):
                ch = clean_desc[right_end]
                if ch in ('(', '[', '{'):
                    depth += 1
                elif ch in (')', ']', '}'):
                    if depth == 0:
                        break
                    depth -= 1
                elif depth == 0 and ch in (',', ')', ']'):
                    break
                elif depth == 0:
                    if right_end + 3 <= len(clean_desc) and clean_desc[right_end:right_end+3].upper() == 'AND':
                        break
                    if right_end + 2 <= len(clean_desc) and clean_desc[right_end:right_end+2].upper() == 'OR':
                        break
                right_end += 1

            left_expr = clean_desc[left_start:pos].strip()
            right_expr = clean_desc[op_end:right_end].strip()
            if left_expr and right_expr:
                return left_expr, right_expr
        return None, None

    def _extract_function_args_text_from_desc(self, func_name, description):
        """从描述中提取函数的参数文本"""
        if not description or not func_name:
            return None

        if FunctionAnalyzer._is_operator_func(func_name):
            left, right = self._extract_operator_operands_from_desc(func_name, description)
            if left is not None and right is not None:
                return f"{left},{right}"
            return None

        func_pattern = re.compile(re.escape(func_name) + r'\s*\(', re.I)
        match = func_pattern.search(description)

        if not match:
            if FunctionAnalyzer._is_prefix_type_func(func_name):
                prefix_pattern = re.compile(r"(?<![a-zA-Z])" + re.escape(func_name) + r"\s+", re.I)
                prefix_match = prefix_pattern.search(description)
                if prefix_match:
                    start = prefix_match.end()
                    prefix_content = description[start:]
                    end_idx = len(prefix_content)
                    for i, c in enumerate(prefix_content):
                        if c in ',);':
                            end_idx = i
                            break
                    return prefix_content[:end_idx].strip()
            return None

        start = match.end()
        depth = 1
        for i in range(start, len(description)):
            if description[i] == "(":
                depth += 1
            elif description[i] == ")":
                depth -= 1
                if depth == 0:
                    return description[start:i]
        return description[start:]

    @staticmethod
    def _split_function_args(text):
        """按逗号分割函数参数（忽略括号内的逗号）"""
        args = []
        current = []
        paren_depth = 0
        bracket_depth = 0

        for char in text:
            if char == "(":
                paren_depth += 1
                current.append(char)
            elif char == ")":
                paren_depth -= 1
                current.append(char)
            elif char == "[":
                bracket_depth += 1
                current.append(char)
            elif char == "]":
                bracket_depth -= 1
                current.append(char)
            elif char == "," and paren_depth == 0 and bracket_depth == 0:
                args.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            args.append("".join(current))
        return args

    def _extract_func_expression(self, func_name, description):
        """提取完整函数表达式"""
        if not description:
            return ""

        if FunctionAnalyzer._is_operator_func(func_name):
            left, right = self._extract_operator_operands_from_desc(func_name, description)
            if left is not None and right is not None:
                return f"{left} {func_name} {right}"
            return func_name

        func_pattern = re.compile(re.escape(func_name) + r'\s*\(', re.I)
        match = func_pattern.search(description)

        if not match:
            if FunctionAnalyzer._is_prefix_type_func(func_name):
                prefix_pattern = re.compile(r"(?<![a-zA-Z])" + re.escape(func_name) + r"\s+", re.I)
                prefix_match = prefix_pattern.search(description)
                if prefix_match:
                    start = prefix_match.start()
                    prefix_content = description[start:]
                    end_idx = len(prefix_content)
                    for i, c in enumerate(prefix_content):
                        if c in ',);':
                            end_idx = i
                            break
                    return prefix_content[:end_idx].strip()
            elif ' ' in func_name:
                keyword_pattern = re.compile(r'\b' + re.escape(func_name) + r'\b', re.I)
                match = keyword_pattern.search(description)
                if match:
                    pos = match.start()
                    op_end = match.end()
                    left_start = pos - 1
                    depth = 0
                    while left_start >= 0:
                        ch = description[left_start]
                        if ch in (')', ']', '}'):
                            depth += 1
                        elif ch in ('(', '[', '{'):
                            if depth == 0:
                                left_start += 1
                                break
                            depth -= 1
                        elif depth == 0 and ch in (',', '('):
                            left_start += 1
                            break
                        left_start -= 1
                    else:
                        left_start = 0
                    left_expr = description[left_start:pos].strip()
                    if left_expr:
                        return f"{left_expr} {func_name}"
            return func_name

        start = match.start()
        depth = 0
        for i in range(match.end() - 1, len(description)):
            if description[i] == "(":
                depth += 1
            elif description[i] == ")":
                depth -= 1
                if depth == 0:
                    return description[start:i + 1]
        return description[start:]

    def _extract_all_func_expressions(self, func_name, description):
        """提取所有匹配的函数表达式"""
        if not description:
            return []

        expressions = []

        if FunctionAnalyzer._is_operator_func(func_name):
            clean_desc = re.sub(r'<[^>]+>[\s:*\+\-]*', ' ', description)
            is_logical_op = func_name.upper() in ('AND', 'OR')
            if is_logical_op:
                func_pattern = re.compile(r'\b' + re.escape(func_name) + r'\b', re.I)
            else:
                func_pattern = re.compile(re.escape(func_name), re.I)

            for match in func_pattern.finditer(clean_desc):
                pos = match.start()
                op_end = match.end()

                if func_name == '=' and op_end < len(clean_desc) and clean_desc[op_end] == '[':
                    continue
                if func_name == '=' and pos > 0 and clean_desc[pos - 1].isalpha():
                    continue
                if func_name in ('=', '<', '>') and pos > 0 and clean_desc[pos - 1] in ('<', '>', '!'):
                    continue
                if func_name in ('=', '<', '>') and op_end < len(clean_desc) and clean_desc[op_end] == '=':
                    continue
                if func_name == '*' and pos > 0 and clean_desc[pos - 1] == '(' and op_end < len(clean_desc) and clean_desc[op_end] == ')':
                    continue
                if FunctionAnalyzer._is_inside_quotes(clean_desc, pos):
                    continue
                if FunctionAnalyzer._is_inside_sarg(clean_desc, pos):
                    continue
                if pos > 0 and (clean_desc[pos - 1].isalnum() or clean_desc[pos - 1] == '_'):
                    continue

                left_start = pos - 1
                depth = 0
                while left_start >= 0:
                    ch = clean_desc[left_start]
                    if ch in (')', ']', '}'):
                        depth += 1
                    elif ch in ('(', '[', '{'):
                        if depth == 0:
                            left_start += 1
                            break
                        depth -= 1
                    elif depth == 0 and ch == '=' and not is_logical_op:
                        if left_start + 1 < len(clean_desc) and clean_desc[left_start + 1] == '[':
                            depth += 1
                            left_start -= 1
                            continue
                        left_start += 1
                        break
                    elif depth == 0 and ch == ',':
                        left_start += 1
                        break
                    elif depth == 0:
                        if left_start >= 2 and clean_desc[left_start - 2:left_start + 1].upper() == 'AND':
                            left_start += 1
                            break
                        if left_start >= 1 and clean_desc[left_start - 1:left_start + 1].upper() == 'OR':
                            left_start += 1
                            break
                    left_start -= 1
                else:
                    left_start = 0
                if left_start < 0:
                    left_start = 0

                right_end = op_end
                depth = 0
                while right_end < len(clean_desc):
                    ch = clean_desc[right_end]
                    if ch in ('(', '[', '{'):
                        depth += 1
                    elif ch in (')', ']', '}'):
                        if depth == 0:
                            break
                        depth -= 1
                    elif depth == 0 and ch in (',', ')', ']'):
                        break
                    elif depth == 0:
                        if right_end + 3 <= len(clean_desc) and clean_desc[right_end:right_end + 3].upper() == 'AND':
                            break
                        if right_end + 2 <= len(clean_desc) and clean_desc[right_end:right_end + 2].upper() == 'OR':
                            break
                    right_end += 1

                left_expr = clean_desc[left_start:pos].strip()
                right_expr = clean_desc[op_end:right_end].strip()
                if left_expr and right_expr:
                    expressions.append(f"{left_expr} {func_name} {right_expr}")
            return expressions if expressions else [func_name]

        func_pattern = re.compile(re.escape(func_name) + r'\s*\(', re.I)
        for match in func_pattern.finditer(description):
            start = match.start()
            depth = 0
            for i in range(match.end() - 1, len(description)):
                if description[i] == "(":
                    depth += 1
                elif description[i] == ")":
                    depth -= 1
                    if depth == 0:
                        expressions.append(description[start:i + 1])
                        break

        if not expressions:
            if FunctionAnalyzer._is_prefix_type_func(func_name):
                prefix_pattern = re.compile(r"(?<![a-zA-Z])" + re.escape(func_name) + r"\s+", re.I)
                for prefix_match in prefix_pattern.finditer(description):
                    start = prefix_match.start()
                    prefix_content = description[start:]
                    end_idx = len(prefix_content)
                    for i, c in enumerate(prefix_content):
                        if c in ',);':
                            end_idx = i
                            break
                    expressions.append(prefix_content[:end_idx].strip())
            elif ' ' in func_name:
                keyword_pattern = re.compile(r'\b' + re.escape(func_name) + r'\b', re.I)
                for match in keyword_pattern.finditer(description):
                    pos = match.start()
                    op_end = match.end()
                    left_start = pos - 1
                    depth = 0
                    while left_start >= 0:
                        ch = description[left_start]
                        if ch in (')', ']', '}'):
                            depth += 1
                        elif ch in ('(', '[', '{'):
                            if depth == 0:
                                left_start += 1
                                break
                            depth -= 1
                        elif depth == 0 and ch in (',', '('):
                            left_start += 1
                            break
                        left_start -= 1
                    else:
                        left_start = 0
                    left_expr = description[left_start:pos].strip()
                    if left_expr:
                        expressions.append(f"{left_expr} {func_name}")

        return expressions if expressions else [func_name]

    def _resolve_func_param_types(self, func_name, description, input_schema=None):
        """解析函数的参数类型"""
        input_types = []
        nested_content = ""
        func_name_lower = func_name.lower()

        dict_entry = self.type_resolver.return_type_dict.get(func_name_lower)
        if dict_entry:
            return_type = dict_entry.get("return_type", "unknown")
            if return_type == "unknown":
                nested_content = self._extract_func_expression(func_name, description)

        json_desc = self._find_json_desc_in_text(description)
        if json_desc:
            input_types, nested_content = self._resolve_func_types_from_json(
                func_name, json_desc, input_schema, nested_content
            )
        else:
            input_types, nested_content = self._resolve_func_types_from_text(
                func_name, description, input_schema, nested_content
            )

        return input_types, nested_content

    def _find_json_desc_in_text(self, description):
        """在描述中查找 JSON 对象"""
        if not description:
            return None
        if isinstance(description, dict):
            return description
        if isinstance(description, list):
            json_descs = self.type_resolver.find_json_descriptions(description)
            if json_descs:
                return json_descs[0]
            return None
        if isinstance(description, str):
            from omnihelper.flink.operator.op_parse import FlinkParser
            desc_data = []
            for line in re.split(r"<br/>[\s:*\+\-]*|\n", description):
                parsed = FlinkParser.parse_single_description_line(line)
                if parsed is not None:
                    desc_data.append(parsed)
            json_descs = self.type_resolver.find_json_descriptions(desc_data)
            if json_descs:
                return json_descs[0]
        return None

    def _resolve_func_types_from_json(self, func_name, json_desc, input_schema, existing_nested):
        """从 JSON 描述中解析函数类型"""
        input_types = []
        nested_content = existing_nested
        has_unknown = False
        found_expr = None

        indices = json_desc.get("indices", [])
        condition = json_desc.get("condition")
        all_exprs = list(indices)
        if condition:
            all_exprs.append(condition)

        for expr in all_exprs:
            if isinstance(expr, dict):
                expr_type = expr.get("exprType", "")
                if expr_type == "FUNCTION" and expr.get("function_name", "").lower() == func_name.lower():
                    found_expr = expr
                    arguments = expr.get("arguments", [])
                    if func_name.lower() in ("cast", "try_cast") and len(arguments) >= 2:
                        source_type = self.type_resolver.resolve_expression_type(arguments[0], input_schema)
                        if source_type == "unknown":
                            has_unknown = True
                        input_types.append(source_type)

                        target_arg = arguments[1]
                        if isinstance(target_arg, dict):
                            target_type_str = target_arg.get("value", "").strip()
                        else:
                            target_type_str = str(target_arg).strip()

                        if target_type_str:
                            normalized_target = re.sub(r'\([^)]*\)', '', target_type_str).upper()
                            input_types.append(normalized_target)
                        else:
                            input_types.append("unknown")
                    else:
                        for arg in arguments:
                            t = self.type_resolver.resolve_expression_type(arg, input_schema)
                            if t == "unknown":
                                has_unknown = True
                            input_types.append(t)
                elif expr_type == "BINARY" and expr.get("operator", "").lower() == func_name.lower():
                    found_expr = expr
                    left_expr = expr.get("left", {})
                    right_expr = expr.get("right", {})
                    left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
                    right_type = self.type_resolver.resolve_expression_type(right_expr, input_schema)
                    if left_type == "unknown":
                        has_unknown = True
                    if right_type == "unknown":
                        has_unknown = True
                    input_types.append(left_type)
                    input_types.append(right_type)

        if has_unknown and not nested_content:
            if found_expr:
                nested_content = self._expr_to_string(found_expr)
            else:
                nested_content = "NEED_FULL_EXPR"

        return input_types, nested_content

    def _resolve_func_types_from_text(self, func_name, description, input_schema, existing_nested):
        """从文本描述中解析函数类型"""
        input_types = []
        nested_content = existing_nested

        if self._is_operator_func(func_name):
            input_types, nested_content = self._resolve_operator_param_types_from_text(
                func_name, description, input_schema, existing_nested
            )
        elif func_name.lower() in ('is true', 'is false', 'is not true', 'is not false',
                                   'is null', 'is not null', 'is distinct from', 'is not distinct from',
                                   'between', 'not between', 'like', 'not like',
                                   'similar to', 'not similar to', 'not in', 'exists'):
            input_types, nested_content = self._resolve_is_boolean_types_from_text(
                func_name, description, input_schema, existing_nested
            )
        else:
            args_str = self._extract_function_args_text_from_desc(func_name, description)
            if args_str:
                if func_name.lower() == "case":
                    input_types, nested_content = self._resolve_case_param_types_from_text(
                        args_str, input_schema, nested_content
                    )
                elif func_name.lower() in ("cast", "try_cast"):
                    input_types, nested_content = self._resolve_cast_param_types_from_text(
                        args_str, input_schema, nested_content
                    )
                else:
                    has_unknown = False
                    args = self._split_function_args(args_str)
                    for arg in args:
                        arg = arg.strip()
                        if not arg:
                            continue
                        t = self.type_resolver.resolve_expression_type(arg, input_schema)
                        if t == "unknown":
                            has_unknown = True
                        input_types.append(t)
                    if has_unknown and not nested_content:
                        nested_content = "NEED_FULL_EXPR"

        if not nested_content or nested_content == "NEED_FULL_EXPR":
            full_expr = self._extract_func_expression(func_name, description)
            if full_expr and full_expr != func_name:
                nested_content = full_expr
            else:
                nested_content = ""

        return input_types, nested_content

    def _resolve_operator_param_types_from_text(self, func_name, description, input_schema, existing_nested):
        """解析操作符的参数类型"""
        input_types = []
        nested_content = existing_nested
        has_unknown = False

        left_expr, right_expr = self._extract_operator_operands_from_desc(func_name, description)
        if left_expr is not None and right_expr is not None:
            left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
            right_type = self.type_resolver.resolve_expression_type(right_expr, input_schema)
            if left_type == "unknown":
                has_unknown = True
            if right_type == "unknown":
                has_unknown = True
            input_types.append(left_type)
            input_types.append(right_type)

        if has_unknown and not nested_content:
            nested_content = "NEED_FULL_EXPR"

        return input_types, nested_content

    def _resolve_is_boolean_types_from_text(self, func_name, description, input_schema, existing_nested):
        """解析 IS TRUE/IS FALSE/IS NULL 等关键字的参数类型"""
        input_types = []
        nested_content = existing_nested
        has_unknown = False
        func_lower = func_name.lower()

        pattern = re.compile(re.escape(func_name), re.I)
        match = pattern.search(description)

        if match:
            pos = match.start()
            op_end = match.end()

            left_start = pos - 1
            depth = 0
            while left_start >= 0:
                ch = description[left_start]
                if ch in (')', ']', '}'):
                    depth += 1
                elif ch in ('(', '[', '{'):
                    if depth == 0:
                        left_start += 1
                        break
                    depth -= 1
                elif depth == 0 and ch in (',', '(', '[', ')'):
                    left_start += 1
                    break
                left_start -= 1
            else:
                left_start = 0

            left_expr = description[left_start:pos].strip()

            if func_lower in ('is null', 'is not null', 'is true', 'is false', 'is not true', 'is not false'):
                if left_expr:
                    left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
                    if left_type == "unknown":
                        has_unknown = True
                    input_types.append(left_type)
                    nested_content = f"{left_expr} {func_name}"

            elif func_lower in ('is distinct from', 'is not distinct from', 'like', 'not like',
                                'similar to', 'not similar to'):
                if left_expr:
                    left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
                    if left_type == "unknown":
                        has_unknown = True
                    input_types.append(left_type)

                right_start = op_end
                while right_start < len(description) and description[right_start].isspace():
                    right_start += 1

                depth = 0
                right_end = right_start
                while right_end < len(description):
                    ch = description[right_end]
                    if ch in ('(', '[', '{'):
                        depth += 1
                    elif ch in (')', ']', '}'):
                        if depth == 0:
                            break
                        depth -= 1
                    elif depth == 0 and ch in (',', ')'):
                        break
                    right_end += 1

                right_expr = description[right_start:right_end].strip()
                if right_expr:
                    right_type = self.type_resolver.resolve_expression_type(right_expr, input_schema)
                    if right_type == "unknown":
                        has_unknown = True
                    input_types.append(right_type)

                full_expr = description[left_start:right_end].strip()
                if full_expr:
                    nested_content = full_expr

            elif func_lower in ('between', 'not between'):
                if left_expr:
                    left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
                    if left_type == "unknown":
                        has_unknown = True
                    input_types.append(left_type)

                right_start = op_end
                while right_start < len(description) and description[right_start].isspace():
                    right_start += 1

                depth = 0
                and_pos = -1
                temp_pos = right_start
                while temp_pos < len(description):
                    ch = description[temp_pos]
                    if ch in ('(', '[', '{'):
                        depth += 1
                    elif ch in (')', ']', '}'):
                        depth -= 1
                    elif depth == 0 and temp_pos + 2 < len(description):
                        if description[temp_pos:temp_pos+3].upper() == 'AND':
                            and_pos = temp_pos
                            break
                    temp_pos += 1

                if and_pos > 0:
                    first_expr = description[right_start:and_pos].strip()
                    if first_expr:
                        first_type = self.type_resolver.resolve_expression_type(first_expr, input_schema)
                        if first_type == "unknown":
                            has_unknown = True
                        input_types.append(first_type)

                    second_start = and_pos + 3
                    while second_start < len(description) and description[second_start].isspace():
                        second_start += 1

                    depth = 0
                    second_end = second_start
                    while second_end < len(description):
                        ch = description[second_end]
                        if ch in ('(', '[', '{'):
                            depth += 1
                        elif ch in (')', ']', '}'):
                            if depth == 0:
                                break
                            depth -= 1
                        elif depth == 0 and ch in (',', ')'):
                            break
                        second_end += 1

                    second_expr = description[second_start:second_end].strip()
                    if second_expr:
                        second_type = self.type_resolver.resolve_expression_type(second_expr, input_schema)
                        if second_type == "unknown":
                            has_unknown = True
                        input_types.append(second_type)

                full_end = right_start
                if and_pos > 0:
                    full_end = second_end
                full_expr = description[left_start:full_end].strip()
                if full_expr:
                    nested_content = full_expr

            elif func_lower == 'not in':
                if left_expr:
                    left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
                    if left_type == "unknown":
                        has_unknown = True
                    input_types.append(left_type)

                right_start = op_end
                while right_start < len(description) and description[right_start].isspace():
                    right_start += 1

                while right_start < len(description) and description[right_start] != '(':
                    right_start += 1

                if right_start < len(description) and description[right_start] == '(':
                    depth = 1
                    right_end = right_start + 1
                    while right_end < len(description):
                        ch = description[right_end]
                        if ch == '(':
                            depth += 1
                        elif ch == ')':
                            depth -= 1
                            if depth == 0:
                                right_end += 1
                                break
                        right_end += 1

                    args_str = description[right_start:right_end].strip()
                    if args_str:
                        nested_content = f"{left_expr} {func_name} {args_str}"

            elif func_lower == 'exists':
                right_start = op_end
                while right_start < len(description) and description[right_start].isspace():
                    right_start += 1

                while right_start < len(description) and description[right_start] != '(':
                    right_start += 1

                if right_start < len(description) and description[right_start] == '(':
                    depth = 1
                    right_end = right_start + 1
                    while right_end < len(description):
                        ch = description[right_end]
                        if ch == '(':
                            depth += 1
                        elif ch == ')':
                            depth -= 1
                            if depth == 0:
                                right_end += 1
                                break
                        right_end += 1

                    subquery = description[right_start:right_end].strip()
                    if subquery:
                        nested_content = f"{func_name} {subquery}"
                        input_types.append("unknown")

        if has_unknown and not nested_content:
            nested_content = "NEED_FULL_EXPR"

        return input_types, nested_content

    def _resolve_case_param_types_from_text(self, args_str, input_schema, existing_nested):
        """解析 CASE 语句的参数类型"""
        input_types = []
        nested_content = existing_nested
        has_unknown = False

        from omnihelper.flink.schema.type_resolver import FlinkTypeResolver
        args = FlinkTypeResolver._wrap_split_function_args(args_str)
        i = 0
        while i < len(args):
            if i + 1 < len(args):
                condition_arg = args[i].strip()
                condition_arg = self._strip_outer_parens(condition_arg)
                t = self.type_resolver.resolve_expression_type(condition_arg, input_schema)
                if t == "unknown":
                    has_unknown = True
                input_types.append(t)

                value_arg = args[i + 1].strip()
                value_arg = self._strip_outer_parens(value_arg)
                t = self.type_resolver.resolve_expression_type(value_arg, input_schema)
                if t == "unknown":
                    has_unknown = True
                input_types.append(t)

                i += 2
            else:
                value_arg = args[i].strip()
                value_arg = self._strip_outer_parens(value_arg)
                t = self.type_resolver.resolve_expression_type(value_arg, input_schema)
                if t == "unknown":
                    has_unknown = True
                input_types.append(t)
                break

        if has_unknown and not nested_content:
            nested_content = "NEED_FULL_EXPR"

        return input_types, nested_content

    def _resolve_cast_param_types_from_text(self, args_str, input_schema, existing_nested):
        """解析 CAST 函数的参数类型"""
        input_types = []
        nested_content = existing_nested
        has_unknown = False

        from omnihelper.flink.schema.type_resolver import FlinkTypeResolver
        original_expr, target_type_str = FlinkTypeResolver._wrap_split_alias_from_expr(args_str)

        if original_expr == args_str and " AS " in args_str.upper():
            parts = re.split(r'\s+AS\s+', args_str, maxsplit=1, flags=re.I)
            if len(parts) == 2:
                original_expr = parts[0].strip()
                target_type_str = parts[1].strip()

        source_type = self.type_resolver.resolve_expression_type(original_expr, input_schema)
        if source_type == "unknown":
            has_unknown = True
        input_types.append(source_type)

        if target_type_str:
            from omnihelper.flink.schema.type_normalizer import TypeNormalizer
            normalized_target = TypeNormalizer.normalize_type(target_type_str.strip())
            input_types.append(normalized_target)

        if has_unknown and not nested_content:
            nested_content = "NEED_FULL_EXPR"

        return input_types, nested_content

    @staticmethod
    def _strip_outer_parens(expr_str):
        """去除表达式最外层的成对括号"""
        expr_str = expr_str.strip()
        while expr_str.startswith("(") and expr_str.endswith(")"):
            depth = 0
            matched = True
            for idx, ch in enumerate(expr_str):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                if depth == 0 and idx < len(expr_str) - 1:
                    matched = False
                    break
            if matched:
                expr_str = expr_str[1:-1].strip()
            else:
                break
        return expr_str

    @staticmethod
    def _expr_to_string(expr):
        """将表达式对象转换为字符串表示"""
        return ExpressionConverter.expr_to_string(expr)

    def _build_func_param_types_map(self, description, input_schema=None):
        """构建函数参数类型映射"""
        param_types_map = {}
        all_funcs = self.function_parser.parse_plan_description(description)
        processed_exprs = set()

        for func_info in all_funcs:
            func_name = func_info.get("func", "")
            if not func_name:
                continue

            all_exprs = self._extract_all_func_expressions(func_name, description)
            if not all_exprs or all_exprs == [func_name]:
                all_exprs = [func_name]

            for expr in all_exprs:
                if (func_name, expr) in processed_exprs:
                    continue
                processed_exprs.add((func_name, expr))

                types, _ = self._resolve_func_param_types(func_name, expr, input_schema)
                if types:
                    param_types_map[(func_name, expr)] = types

        return param_types_map

    def analyze_operators_functions(self, description, task_id, input_schema=None):
        """分析算子描述中的函数"""
        if not description:
            return []

        param_types_map = self._build_func_param_types_map(description, input_schema)

        unsupported_funcs = self.function_parser.analyze_unsupported_functions(
            description, param_types_map
        )

        result = []
        for func in unsupported_funcs:
            func_name = func["func_name"]

            all_exprs = self._extract_all_func_expressions(func_name, description)
            if not all_exprs or all_exprs == [func_name]:
                all_exprs = [func_name]

            seen = set()
            unique_exprs = []
            for expr in all_exprs:
                if expr not in seen:
                    seen.add(expr)
                    unique_exprs.append(expr)
            all_exprs = unique_exprs

            for i, expr in enumerate(all_exprs):
                input_types = []
                if param_types_map:
                    key = (func_name, expr)
                    if key in param_types_map:
                        input_types = param_types_map[key]
                    elif func_name in param_types_map:
                        input_types = param_types_map[func_name]

                if not input_types:
                    input_types, _ = self._resolve_func_param_types(func_name, expr, input_schema)

                # 对每个表达式单独检查支持性
                is_supported, unsupported_types = self.function_parser.is_func_type_supported(func_name, input_types)

                # 只有不支持的表达式才添加到结果中
                if not is_supported:
                    entry = {
                        "func_name": func_name,
                        "task_id": task_id,
                        "input": input_types,
                        "nested_content": expr,
                        "times": func["times"] if i == 0 else ""
                    }
                    if unsupported_types:
                        entry["unsupported_types"] = unsupported_types
                    result.append(entry)

        return result
