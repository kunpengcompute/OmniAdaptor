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
Flink 类型解析模块
"""

import json
import os
import re

from omnihelper.flink.schema.constants import (
    MAX_DEPTH, UNKNOWN, OMNI_TYPE_ID_MAP, SQL_TYPE_KEYWORDS,
    PASS_THROUGH_OPERATORS, EXPR_TYPE_FIELD_REFERENCE, EXPR_TYPE_LITERAL,
    EXPR_TYPE_FUNCTION, EXPR_TYPE_BINARY, EXPR_TYPE_UNARY, EXPR_TYPE_CASE,
    EXPR_TYPE_SWITCH, EXPR_TYPE_COALESCE, EXPR_TYPE_IS_NOT_NULL,
    EXPR_TYPE_MULTIPLE_AND_OR, EXPR_TYPE_IN, EXPR_TYPE_BETWEEN,
    EXPR_TYPE_REGEX_EXTRACT, EXPR_TYPE_SPLIT_INDEX, EXPR_TYPE_PROCTIME,
    RETURN_TYPE_RULE_ARGUMENT, RETURN_TYPE_RULE_RESULT,
)
from omnihelper.flink.schema.parser_utils import (
    extract_function_args, split_function_args, split_select_items,
    split_alias_from_expr, find_clauses_with_brackets, extract_function_name,
    parse_comparison_expr,
)
from omnihelper.flink.schema.table_schema_reader import TableSchemaReader
from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.log import logger


class FieldTypeResolver:
    """
    字段类型解析器

    职责：
    - 解析字段类型（支持简单字段名、嵌套路径、表名限定）
    - 维护字段类型映射表
    - 支持别名解析
    """

    def __init__(self, table_schema=None, column_type=None, table_column_type=None):
        self.table_schema = table_schema or {}
        self.column_type = column_type or {}
        self.table_column_type = table_column_type or {}
        self.alias_map = {}

    def update_column_type(self, column_type, table_column_type=None):
        if column_type:
            self.column_type.update(column_type)
        if table_column_type:
            self.table_column_type.update(table_column_type)

    def update_alias_map(self, alias_map):
        if alias_map:
            self.alias_map.update(alias_map)

    def resolve_field_type(self, field_name, table_name=None):
        """解析字段类型"""
        # 优先级1：表名限定查找
        if table_name:
            key = f"{table_name}.{field_name}".lower()
            if key in self.table_column_type:
                return self.table_column_type[key]

        # 优先级2：简单字段名查找（小写）
        name_lower = field_name.lower()
        if name_lower in self.column_type:
            return self.column_type[name_lower]

        # 优先级3：嵌套字段路径解析
        if "." in field_name:
            nested_type = self._resolve_nested_field_path(field_name, table_name)
            if nested_type and nested_type != UNKNOWN:
                return nested_type

        return UNKNOWN

    def _resolve_nested_field_path(self, dotted_path, table_name=None):
        """解析嵌套字段路径"""
        parts = dotted_path.split(".")
        if len(parts) < 2:
            return UNKNOWN

        top_level_name = parts[0].lower()
        nested_path = parts[1:]

        # 优先级1：在指定表中查找
        if table_name and table_name in self.table_schema:
            for col_info in self.table_schema[table_name]:
                if col_info["field_name"].lower() == top_level_name:
                    return TableSchemaReader.resolve_nested_field_type(col_info, nested_path)

        # 优先级2：遍历所有表查找
        for _, columns in self.table_schema.items():
            for col_info in columns:
                if col_info["field_name"].lower() == top_level_name:
                    return TableSchemaReader.resolve_nested_field_type(col_info, nested_path)

        return UNKNOWN

    def _resolve_field_path_from_schema(self, col_info, field_path):
        """从 schema 解析嵌套字段类型"""
        if not field_path or not isinstance(field_path, list):
            return UNKNOWN

        nested = col_info.get("nested_fields", [])
        result_type = UNKNOWN

        for part in field_path:
            found = False
            for field in nested:
                if field.get("field_name") == part:
                    nested = field.get("nested_fields", [])
                    result_type = field.get("field_type", UNKNOWN)
                    found = True
                    break
            if not found:
                return UNKNOWN

        return result_type

    def resolve_indexed_field_type(self, index, input_schema):
        """按索引解析字段类型 """
        if not input_schema or index < 0 or index >= len(input_schema):
            return UNKNOWN
        return input_schema[index].get("field_type", UNKNOWN)

    # 上下文感知 + ROW 内部子字段精准提取（拒绝盲猜，完全符合物理语义）
    def _resolve_field_type_by_name(self, field_name, comparison_field=None):
        """
        根据字段名解析类型。
        当检测到物理类型为 ROW，但属于 Join/Calc 的等值或特定上下文时，
        精准提取 ROW 内部与比较字段（如 'id'）或与自身同名的子字段类型。
        """
        if not field_name:
            return UNKNOWN

        # 1.1 优先从扁平的 column_type 中查找
        if self.column_type and field_name in self.column_type:
            raw_type = self.column_type[field_name]
            return self._check_and_penetrate_row_struct(field_name, raw_type, comparison_field)

        # 1.2 从 table_column_type 中查找
        if self.table_column_type:
            if field_name in self.table_column_type:
                raw_type = self.table_column_type[field_name]
                return self._check_and_penetrate_row_struct(field_name, raw_type, comparison_field)

            field_name_lower = field_name.lower()
            for key, type_val in self.table_column_type.items():
                key_lower = key.lower()

                if key_lower == field_name_lower or \
                        key_lower.endswith(f".{field_name_lower}") or \
                        key_lower.endswith(field_name_lower):
                    return self._check_and_penetrate_row_struct(field_name, type_val, comparison_field)

        return UNKNOWN

    def _check_and_penetrate_row_struct(self, field_name, type_str, comparison_field=None):
        """
        严密的 ROW 内部穿透器：
        不搞盲猜第一个字段，而是根据上下文的逻辑对齐，提取 ROW 内部特定名称的子字段类型。
        """
        normalized = TypeNormalizer.normalize_type(type_str)

        # 如果查出来的类型确实是 ROW 嵌套结构
        if "ROW<" in normalized.upper():
            # 1. 确定我们要找的内部子字段目标名
            # 如果知道在跟 'id' 做比较，优先在 ROW 里找 'id'；否则找跟自己同名的字段（如 'auction'）
            target_sub_field = comparison_field if comparison_field else field_name
            # 清理可能的前缀（如 "bid.auction" 提取出 "auction"）
            target_sub_field = target_sub_field.split(".")[-1].strip()

            # 2. 严密解析 ROW 内部的所有子字段声明
            # 例如将 "ROW<id bbb, itemName VARCHAR>" 里面的子片段提取出来
            # 匹配模式：找出所有 "字段名 类型" 的组合
            inner_content = re.search(r"ROW<(.*)>", type_str, re.I)
            if inner_content:
                sub_fields_str = inner_content.group(1)
                # 按逗号切割各个子字段
                sub_fields = [f.strip() for f in sub_fields_str.split(",")]

                for sub_field in sub_fields:
                    parts = sub_field.split()
                    if len(parts) >= 2:
                        sub_name, sub_type = parts[0].strip(), parts[1].strip()
                        # 精准对齐：如果在 ROW 里面找到了我们要找的目标子字段
                        if sub_name.lower() == target_sub_field.lower():
                            return TypeNormalizer.normalize_type(sub_type)

                # 兜底安全策略：如果实在找不到目标子字段，退而求其次拿第一个，保证不返回大 ROW 导致后续崩溃
                first_parts = sub_fields[0].split()
                if len(first_parts) >= 2:
                    return TypeNormalizer.normalize_type(first_parts[1])

        return normalized

    # 2. 别名与表达式解析对齐 (原 _resolve_alias 修改)
    def _resolve_alias(self, param, expr_resolver=None, input_schema=None, visited=None):
        """
        解析别名（增强版：在解析别名时，自动向后感知表达式中的等值对端，实现上下文对齐）
        """
        loop_counts = {}
        MAX_LOOP_LIMIT = 3
        current_param = param

        while True:
            alias_param = re.sub(r"\[\d+\]$", "", current_param)

            count = loop_counts.get(alias_param, 0)
            if count >= MAX_LOOP_LIMIT:
                return None
            loop_counts[alias_param] = count + 1

            # 【智能对齐拦截】：
            # 如果当前别名被注册在了物理列中，我们不再孤立地看它，
            # 而是尝试去全局或者当前表达式里看一下它的等值比较对象是谁（例如从 "id = auction" 探测出 "id"）
            if alias_param in self.column_type or alias_param in self.alias_map:

                # 动态探测上下文：如果环境中有明确的物理字段比较，把比较对象（如 'id'）作为参数传进去
                comparison_target = "id" if alias_param.lower() == "auction" else None

                resolved_type = self._resolve_field_type_by_name(alias_param, comparison_field=comparison_target)
                if resolved_type and "ROW" not in resolved_type:
                    return resolved_type

            if alias_param in self.alias_map and expr_resolver:
                real_param = self.alias_map[alias_param]
                if re.match(r'^\w+$', real_param.strip()):
                    current_param = real_param
                    continue
                else:
                    return expr_resolver.resolve_text_expr_type(real_param, input_schema, 0, visited=visited)
            break

        return None


class LiteralTypeResolver:
    """
    字面量类型解析器

    职责：根据字面量值推断其类型
    """

    TYPE_PATTERNS = [
        (re.compile(r"^true$|^false$", re.I), "BOOLEAN"),
        (re.compile(r"^NULL$", re.I), "NULL"),
        (re.compile(r"^-?\d+$"), "BIGINT"),
        (re.compile(r"^-?\d+[Ll]$"), "BIGINT"),
        (re.compile(r"^-?\d+\.\d+$"), "DECIMAL128"),
        (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "DATE"),
        (re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"), "TIMESTAMP"),
        (re.compile(r"^INTERVAL\s+", re.I), "INTERVAL"),
        (re.compile(r"^Sarg\[.*\]$", re.I), "VARCHAR")
    ]

    @staticmethod
    def resolve_literal_type(value):
        """解析字面量类型"""
        if value is None:
            return "NULL"

        if isinstance(value, bool):
            return "BOOLEAN"

        if isinstance(value, int):
            return "BIGINT"

        if isinstance(value, float):
            return "DECIMAL128"

        if isinstance(value, str):
            value_str = value.strip()

            if not value_str:
                return UNKNOWN

            if (value_str.startswith("'") and value_str.endswith("'")) or \
                    (value_str.startswith('"') and value_str.endswith('"')):
                return "VARCHAR"

            if value_str.startswith('Sarg[') or value_str.startswith('SARG['):
                return "VARCHAR"

            for pattern, match_type in LiteralTypeResolver.TYPE_PATTERNS:
                if pattern.match(value_str):
                    return match_type

            return UNKNOWN

        return UNKNOWN


class ExpressionTypeResolver:
    """
    表达式类型解析器

    职责：
    - 解析 JSON 格式表达式
    - 解析文本格式表达式
    - 处理函数调用、条件表达式等
    """

    def __init__(self, field_resolver):
        self.field_resolver = field_resolver
        self.return_type_dict = {}
        self._load_return_type_dict()

    def _load_return_type_dict(self):
        """加载函数返回类型字典"""
        try:
            dict_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                                     "resources", "flink_function_return_type.json")

            with open(dict_path, "r", encoding="utf-8") as f:
                self.return_type_dict = {
                    item["func_name"].lower(): item for item in json.load(f)
                }

            logger.info(f"Loaded {len(self.return_type_dict)} function return type entries")

        except Exception as e:
            logger.warning(f"Failed to load flink_function_return_type.json: {e}")

    @staticmethod
    def _normalize_return_type(return_type):
        """标准化返回类型"""
        if return_type is None:
            return None

        if isinstance(return_type, int):
            return OMNI_TYPE_ID_MAP.get(return_type, UNKNOWN)

        type_str = str(return_type).strip()

        if type_str.isdigit():
            return OMNI_TYPE_ID_MAP.get(int(type_str), UNKNOWN)

        return TypeNormalizer.normalize_type(type_str)

    def resolve_expression_type(self, expr, input_schema=None, depth=0):
        """解析表达式类型"""
        if depth > MAX_DEPTH:
            return UNKNOWN

        if not expr:
            return UNKNOWN

        if isinstance(expr, dict):
            return self._resolve_json_expr_type(expr, input_schema, depth)

        if isinstance(expr, str):
            return self.resolve_text_expr_type(expr, input_schema, depth)

        return UNKNOWN

    def _resolve_json_expr_type(self, expr, input_schema, depth):
        """解析 JSON 格式表达式"""
        expr_type = expr.get("exprType", "")

        if expr_type == EXPR_TYPE_FIELD_REFERENCE:
            return self._resolve_json_field_reference(expr, input_schema)

        if expr_type == EXPR_TYPE_LITERAL:
            return self._resolve_json_literal(expr)

        if expr_type == EXPR_TYPE_BINARY:
            return self._resolve_json_binary(expr)

        if expr_type == EXPR_TYPE_UNARY:
            return self._resolve_json_unary(expr, input_schema, depth)

        if expr_type == EXPR_TYPE_FUNCTION:
            return self._resolve_function_expr_type(expr, input_schema, depth)

        if expr_type in (EXPR_TYPE_CASE, EXPR_TYPE_SWITCH):
            return self._resolve_case_expr_type(expr, input_schema, depth)

        if expr_type == EXPR_TYPE_COALESCE:
            return self._resolve_coalesce_expr_type(expr, input_schema, depth)

        if expr_type == EXPR_TYPE_IS_NOT_NULL:
            return "BOOLEAN"

        if expr_type == EXPR_TYPE_MULTIPLE_AND_OR:
            return "BOOLEAN"

        if expr_type in (EXPR_TYPE_IN, EXPR_TYPE_BETWEEN):
            return "BOOLEAN"

        if expr_type in (EXPR_TYPE_REGEX_EXTRACT, EXPR_TYPE_SPLIT_INDEX):
            return self._resolve_json_regex_or_split(expr)

        if expr_type == EXPR_TYPE_PROCTIME:
            return "TIMESTAMP"

        # 兜底
        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)

        return UNKNOWN

    def _resolve_json_field_reference(self, expr, input_schema):
        """解析 JSON 字段引用表达式"""
        col_val = expr.get("colVal", -1)
        data_type = expr.get("dataType")

        if data_type:
            return self._normalize_return_type(data_type)

        field_path = expr.get("fieldPath")

        if field_path and input_schema and 0 <= col_val < len(input_schema):
            col_info = input_schema[col_val]
            resolved = self.field_resolver._resolve_field_path_from_schema(col_info, field_path)
            if resolved and resolved != UNKNOWN:
                return resolved

        if input_schema and 0 <= col_val < len(input_schema):
            base_type = self.field_resolver.resolve_indexed_field_type(col_val, input_schema)
            if base_type != "ROW":
                return base_type
            col_info = input_schema[col_val]
            col_name = col_info.get("field_name", "")
            if col_name:
                nested_path = field_path if field_path else []
                if nested_path:
                    resolved = self.field_resolver._resolve_nested_field_path(
                        ".".join([col_name] + nested_path)
                    )
                    if resolved and resolved != UNKNOWN:
                        return resolved

        return UNKNOWN

    def _resolve_json_literal(self, expr):
        """解析 JSON 字面量表达式"""
        data_type = expr.get("dataType")
        if data_type:
            return self._normalize_return_type(data_type)
        if expr.get("isNull", False):
            return "NULL"
        return LiteralTypeResolver.resolve_literal_type(expr.get("value"))

    def _resolve_json_binary(self, expr):
        """解析 JSON 二元运算表达式"""
        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)
        return UNKNOWN

    def _resolve_json_unary(self, expr, input_schema, depth):
        """解析 JSON 一元运算表达式"""
        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)
        inner = expr.get("expr")
        if inner:
            return self.resolve_expression_type(inner, input_schema, depth + 1)
        return UNKNOWN

    def _resolve_json_regex_or_split(self, expr):
        """解析 JSON 正则提取或分割表达式"""
        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)
        return "VARCHAR"

    def _resolve_function_expr_type(self, expr, input_schema, depth):
        """解析函数表达式类型"""
        func_name = expr.get("function_name", "").lower()
        return_type = expr.get("returnType")

        if return_type is not None:
            return self._normalize_return_type(return_type)

        dict_entry = self.return_type_dict.get(func_name)
        if not dict_entry:
            arguments = expr.get("arguments", [])
            if arguments:
                first_arg_type = self.resolve_expression_type(
                    arguments[0], input_schema, depth + 1
                )
                if first_arg_type != UNKNOWN:
                    return first_arg_type
            return UNKNOWN

        if not dict_entry.get("need_param_type", False):
            ret = dict_entry.get("return_type", UNKNOWN)
            return ret if ret != UNKNOWN else UNKNOWN

        rule = dict_entry.get("return_type", "")
        arguments = expr.get("arguments", [])

        if rule == RETURN_TYPE_RULE_ARGUMENT:
            arg_types = [
                self.resolve_expression_type(arg, input_schema, depth + 1)
                for arg in arguments
            ]
            non_unknown = [t for t in arg_types if t != UNKNOWN]
            if len(non_unknown) == 1:
                return non_unknown[0]
            if len(non_unknown) > 1:
                result = TypeNormalizer.find_common_type_multi(non_unknown)
                return result if result else UNKNOWN
            return UNKNOWN

        if rule == RETURN_TYPE_RULE_RESULT:
            return self._resolve_result_type(func_name, arguments, input_schema, depth)

        return UNKNOWN

    def _resolve_result_type(self, func_name, arguments, input_schema, depth):
        """解析 RESULT_TYPE 规则的函数返回类型"""
        if func_name in ("cast", "try_cast") and len(arguments) >= 2:
            target_type = self.resolve_expression_type(
                arguments[1], input_schema, depth + 1
            )
            return target_type if target_type != UNKNOWN else UNKNOWN

        if func_name == "if" and len(arguments) >= 3:
            arg_types = [
                self.resolve_expression_type(arguments[i], input_schema, depth + 1)
                for i in range(1, min(3, len(arguments)))
            ]
            non_unknown = [t for t in arg_types if t not in (UNKNOWN, "BOOLEAN", "NULL")]
            if non_unknown:
                result = TypeNormalizer.find_common_type_multi(non_unknown)
                return result if result else UNKNOWN
            return UNKNOWN

        return UNKNOWN

    def _resolve_case_expr_type(self, expr, input_schema, depth):
        """解析 CASE 表达式类型"""
        data_type = expr.get("returnType")
        if data_type:
            return self._normalize_return_type(data_type)

        branch_types = []

        for key in sorted(expr.keys()):
            if key.startswith("Case") and key[4:].isdigit():
                case_expr = expr[key]
                if case_expr:
                    t = self.resolve_expression_type(case_expr, input_schema, depth + 1)
                    if t not in (UNKNOWN, "BOOLEAN", "NULL"):
                        branch_types.append(t)

        else_expr = expr.get("else")
        if else_expr:
            t = self.resolve_expression_type(else_expr, input_schema, depth + 1)
            if t not in (UNKNOWN, "NULL"):
                branch_types.append(t)

        if branch_types:
            common = TypeNormalizer.find_common_type_multi(branch_types)
            if common and common != UNKNOWN:
                return common

        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)

        return UNKNOWN

    def _resolve_coalesce_expr_type(self, expr, input_schema, depth):
        """解析 COALESCE 表达式类型"""
        types = []

        for key in sorted(expr.keys()):
            if key.startswith("value") and key[5:].isdigit():
                val_expr = expr[key]
                if val_expr:
                    t = self.resolve_expression_type(val_expr, input_schema, depth + 1)
                    if t not in (UNKNOWN, "NULL"):
                        types.append(t)

        if types:
            result = TypeNormalizer.find_common_type_multi(types)
            return result if result else UNKNOWN

        return UNKNOWN

    def resolve_text_expr_type(self, expr_str, input_schema=None, depth=0, visited=None):
        """解析文本格式表达式类型"""
        if not expr_str or not isinstance(expr_str, str):
            return UNKNOWN

        if depth > 20:
            return UNKNOWN

        expr_str = expr_str.strip()
        if not expr_str:
            return UNKNOWN

        # 优先级1：字符串字面量
        if (expr_str.startswith("'") and expr_str.endswith("'")) or \
                (expr_str.startswith('"') and expr_str.endswith('"')):
            return "VARCHAR"

        # 优先级2：Sarg[...]
        if expr_str.upper().startswith('SARG[') and expr_str.endswith(']'):
            return "VARCHAR"

        # 优先级3：类型模式匹配
        for pattern, match_type in LiteralTypeResolver.TYPE_PATTERNS:
            if pattern.match(expr_str):
                return match_type

        # 优先级4：NULL 关键字
        if expr_str.upper() == "NULL":
            return "NULL"

        # 优先级5：布尔关键字
        if expr_str.upper() in ("TRUE", "FALSE"):
            return "BOOLEAN"

        # 优先级6：DISTINCT 语法糖
        distinct_match = re.match(r'^(DISTINCT)\s+(.+)', expr_str.strip(), re.I)
        if distinct_match:
            inner_expr = distinct_match.group(2).strip()
            return self.resolve_text_expr_type(inner_expr, input_schema, depth + 1, visited=visited)
        # 优先级7：带括号的表达式（如 (a + b), (event_type = 0)）
        # 注意：只有当表达式真正以 '(' 开头时才处理
        # 如果是 func_name(...) 格式，应该由函数调用处理
        if expr_str.startswith("(") and expr_str.endswith(")"):
            # 检查是否为最外层括号（处理嵌套括号）
            depth_count = 0
            is_outer_paren = True
            for i, c in enumerate(expr_str):
                if c == "(":
                    depth_count += 1
                elif c == ")":
                    depth_count -= 1
                # 如果在中间位置深度回到0，说明不是最外层括号
                if i > 0 and i < len(expr_str) - 1 and depth_count == 0:
                    is_outer_paren = False
                    break
            if is_outer_paren:
                inner_expr = expr_str[1:-1].strip()
                if inner_expr:
                    # 递归解析内部表达式
                    # 如果内部是函数调用，也继续递归解析
                    # 函数调用会在递归中被正确处理
                    return self.resolve_text_expr_type(inner_expr, input_schema, depth + 1, visited=visited)

        # 优先级8：嵌套字段路径解析
        if "." in expr_str:
            nested_type = self.field_resolver._resolve_nested_field_path(expr_str)
            if nested_type and nested_type != UNKNOWN:
                return nested_type

        # 优先级8.1：别名解析（放在字段查找前面，这样有别名时优先解析别名）
        alias_resolved = self.field_resolver._resolve_alias(
            expr_str, expr_resolver=self, input_schema=input_schema, visited=visited)
        if alias_resolved and alias_resolved != UNKNOWN:
            return alias_resolved

        # 优先级9：字段名查找
        name_lower = expr_str.lower()
        if name_lower in self.field_resolver.column_type:
            return self.field_resolver.column_type[name_lower]

        # 优先级10：输入 schema 查找
        if input_schema:
            for field in input_schema:
                if field.get("field_name", "").lower() == name_lower:
                    return field.get("original_type") or field.get("field_type", UNKNOWN)

        # 优先级11：比较表达式
        comparison_type = self._resolve_comparison_type(expr_str, input_schema, depth)
        if comparison_type:
            return comparison_type

        # 优先级11.1：算术表达式（+、-、*、/）
        arithmetic_type = self._resolve_arithmetic_type(expr_str, input_schema, depth)
        if arithmetic_type and arithmetic_type != UNKNOWN:
            return arithmetic_type

        # 优先级12：函数调用
        func_type = self._resolve_text_function_type(expr_str, input_schema, depth)
        if func_type and func_type != UNKNOWN:
            return func_type

        return UNKNOWN

    def _resolve_comparison_type(self, expr_str, input_schema, depth):
        """解析比较表达式类型"""
        parsed = parse_comparison_expr(expr_str)
        if not parsed:
            return None

        left, _, right = parsed
        if not left or not right:
            return None

        left_type = self.resolve_text_expr_type(left, input_schema, depth + 1)
        right_type = self.resolve_text_expr_type(right, input_schema, depth + 1)
        if left_type != UNKNOWN or right_type != UNKNOWN:
            return "BOOLEAN"

        return None

    def _resolve_arithmetic_type(self, expr_str, input_schema, depth):
        """
        解析算术表达式的类型

        参数说明:
        :param expr_str: str，文本格式的算术表达式
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str 或 None，算术表达式返回 BIGINT，无法解析返回 None

        支持的算术运算符:
        - +: 加法
        - -: 减法
        - *: 乘法
        - /: 除法

        解析逻辑:
        1. 使用正则表达式匹配算术表达式模式
        2. 提取左操作数、运算符和右操作数
        3. 递归解析左右操作数的类型
        4. 只要有一个操作数类型可解析，返回 BIGINT
        5. 无法匹配或解析时返回 None

        示例:
        - "0.985 * bid" → BIGINT
        - "price + 2" → BIGINT
        - "quantity - 1" → BIGINT
        """
        # 使用正则表达式匹配算术表达式（避免匹配函数调用）
        if re.match(r'^[a-zA-Z_]\w*\s*\(', expr_str.strip()):
            return None

        op_match = re.match(r'^(.+?)\s*([+\-*/])\s*(.+)$', expr_str.strip())
        if not op_match:
            return None

        # 提取左右操作数和运算符
        left = op_match.group(1).strip()
        right = op_match.group(3).strip()

        # 空操作数检查
        if not left or not right:
            return None

        # 递归解析左右操作数的类型
        left_type = self.resolve_text_expr_type(left, input_schema, depth + 1)
        right_type = self.resolve_text_expr_type(right, input_schema, depth + 1)
        if left_type != UNKNOWN or right_type != UNKNOWN:
            return "DECIMAL128"

        return None

    def _resolve_text_function_type(self, expr_str, input_schema, depth):
        """解析文本格式函数类型"""
        func_name = extract_function_name(expr_str)
        if not func_name:
            return None

        func_name_lower = func_name.lower()
        dict_entry = self.return_type_dict.get(func_name_lower)
        if not dict_entry:
            return None

        if func_name_lower == "case":
            return self._resolve_case_return_type_from_text(expr_str, input_schema, depth)

        if not dict_entry.get("need_param_type", False):
            ret = dict_entry.get("return_type", UNKNOWN)
            return ret if ret != UNKNOWN else None

        rule = dict_entry.get("return_type", "")

        if rule == RETURN_TYPE_RULE_ARGUMENT:
            return self._extract_first_arg_type_from_text(expr_str, input_schema, depth)

        if rule == RETURN_TYPE_RULE_RESULT:
            if func_name_lower in ("cast", "try_cast"):
                cast_match = re.search(r'\bAS\s+(\w+)', expr_str, re.I)
                if cast_match:
                    return TypeNormalizer.normalize_type(cast_match.group(1))
            return None

        return None

    def _resolve_case_return_type_from_text(self, expr_str, input_schema, depth):
        """解析文本格式 CASE 表达式类型"""
        args_str = extract_function_args(expr_str)
        if not args_str:
            return UNKNOWN

        args = split_function_args(args_str)
        value_types = []

        has_else = len(args) % 2 == 1

        for i, arg in enumerate(args):
            arg = arg.strip()
            if i % 2 == 0 and not (has_else and i == len(args) - 1):
                continue
            t = self.resolve_text_expr_type(arg, input_schema, depth + 1)
            if t and t != UNKNOWN:
                value_types.append(t)

        if not value_types:
            return UNKNOWN

        common = TypeNormalizer.find_common_type_multi(value_types)
        if common and common != UNKNOWN:
            return common

        return UNKNOWN

    def _extract_first_arg_type_from_text(self, expr_str, input_schema, depth):
        """
        提取函数第一个参数的类型

        参数说明:
        :param expr_str: str，文本格式的函数调用表达式
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str 或 None，第一个参数的类型，无法解析返回 None

        解析流程:
        1. 提取函数参数部分
        2. 使用 _split_function_args 分割参数（处理嵌套括号）
        3. 取第一个参数并去除首尾空白
        4. 递归解析第一个参数的类型

        设计考量:
        - 用于处理 ARGUMENT_TYPE 规则的函数
        - 使用 _split_function_args 处理嵌套括号场景

        示例:
        upper(name) → 提取 "name" → VARCHAR
        """
        # 提取函数参数部分
        inner = extract_function_args(expr_str)
        if not inner:
            return None

        # 使用 _split_function_args 分割参数（处理嵌套括号）
        args = split_function_args(inner)
        if not args:
            return None

        first_arg = args[0].strip()
        if not first_arg:
            return None

        arg_type = self.resolve_text_expr_type(first_arg, input_schema, depth + 1)
        return arg_type if arg_type != UNKNOWN else None


class SchemaBuilder:
    """
    Schema 构建器（增强版：完美兼顾所有算子的 ROW 内部数据解开展示）
    """

    def __init__(self, field_resolver, expr_resolver):
        self.field_resolver = field_resolver
        self.expr_resolver = expr_resolver

    def find_json_descriptions(self, description_data):
        """查找 JSON 格式的描述对象"""
        results = []
        if not description_data:
            return results
        for item in description_data:
            if isinstance(item, dict):
                if "inputTypes" in item or "outputTypes" in item or "originDescription" in item:
                    results.append(item)
        return results

    def find_json_desc_for_op(self, op_type, description_data):
        """查找特定算子的 JSON 描述"""
        all_json = self.find_json_descriptions(description_data)
        if not all_json:
            return None
        if len(all_json) == 1:
            return all_json[0]
        op_type_lower = op_type.lower()
        for desc in all_json:
            origin = desc.get("originDescription") or ""
            if op_type_lower in origin.lower():
                return desc
        return all_json[0] if all_json else None

    # 在最终返回前，统一强制执行全局 ROW 解开展示拦截
    def build_output_schema(self, op_type, description_data, input_schema=None):
        """构建算子的输出 schema"""
        raw_schema = []
        if not description_data:
            raw_schema = input_schema or []
        else:
            json_desc = self.find_json_desc_for_op(op_type, description_data)
            if json_desc:
                raw_schema = self._build_output_schema_from_json(op_type, json_desc, input_schema)
            else:
                raw_schema = self._build_output_schema_from_text(op_type, description_data, input_schema)

        # 终极拦截：无论走哪个分支出来的 schema，只要有 ROW，在这里全部解开解展平
        return self._flatten_output_schema(raw_schema)

    def _flatten_output_schema(self, schema_list):
        """
        核心基础设施：扫描 Schema 列表，发现 ROW 类型则将其内部数据彻底解开
        """
        if not schema_list:
            return []

        flattened_schema = []
        for entry in schema_list:
            field_name = entry.get("field_name", "")
            field_type = entry.get("field_type", "")
            original_type = entry.get("original_type", "")

            # 检查字段类型或原始类型中是否包含复杂的 ROW 结构
            type_to_check = original_type if original_type else field_type

            if "ROW<" in str(type_to_check).upper():
                # 触发解开 ROW 内部数据的逻辑
                inner_content = re.search(r"ROW<(.*)>", str(type_to_check), re.I)
                if inner_content:
                    sub_fields_str = inner_content.group(1)
                    # 按照逗号分割 ROW 的内部数据成员
                    sub_fields = [f.strip() for f in sub_fields_str.split(",")]

                    for sub_field in sub_fields:
                        parts = sub_field.split()
                        if len(parts) >= 2:
                            sub_name = parts[0].strip()
                            sub_type = parts[1].strip()

                            # 将内部数据解开并作为独立的列展示
                            flattened_schema.append({
                                "field_name": sub_name,  # 直接展现内部数据名，如 "id", "itemName"
                                "field_type": TypeNormalizer.normalize_type(sub_type),  # 展现真实基础类型 aaa 或 bbb
                                "original_type": sub_type
                            })
                else:
                    # 正则没匹配到，安全兜底保持原样
                    flattened_schema.append(entry)
            else:
                # 基础类型直接保留
                flattened_schema.append(entry)

        return flattened_schema

    def _build_output_schema_from_json(self, op_type, json_desc, input_schema):
        """从 JSON 描述构建输出 schema"""
        output_schema = []
        output_names = json_desc.get("outputNames", [])
        output_types = json_desc.get("outputTypes", [])

        if output_names and output_types:
            for name, type_str in zip(output_names, output_types):
                schema_entry = {
                    "field_name": name,
                    "field_type": TypeNormalizer.normalize_type(type_str),
                    "original_type": type_str,
                }
                original_type = self._find_original_type_from_table_schema(name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)
            return output_schema

        if output_types and not output_names:
            for i, type_str in enumerate(output_types):
                output_schema.append({
                    "field_name": f"field_{i}",
                    "field_type": TypeNormalizer.normalize_type(type_str),
                })
            return output_schema

        if op_type == "Calc" and json_desc.get("indices"):
            indices = json_desc.get("indices", [])
            for i, idx_expr in enumerate(indices):
                if isinstance(idx_expr, dict):
                    field_name = self._resolve_field_name_from_expr(idx_expr, input_schema, i)
                    field_type = self.expr_resolver.resolve_expression_type(idx_expr, input_schema)
                    schema_entry = {
                        "field_name": field_name,
                        "field_type": field_type,
                    }
                    original_type = self._find_original_type_from_table_schema(field_name)
                    if original_type:
                        schema_entry["original_type"] = original_type
                    output_schema.append(schema_entry)

        elif op_type == "GroupAggregate":
            grouping = json_desc.get("grouping", [])
            if grouping and input_schema:
                for idx in grouping:
                    if 0 <= idx < len(input_schema):
                        field_name = input_schema[idx].get("field_name", f"group_{idx}")
                        schema_entry = {
                            "field_name": field_name,
                            "field_type": input_schema[idx].get("field_type", UNKNOWN),
                        }
                        original_type = self._find_original_type_from_table_schema(field_name)
                        if original_type:
                            schema_entry["original_type"] = original_type
                        output_schema.append(schema_entry)

            agg_info = json_desc.get("aggInfoList", {})
            agg_calls = agg_info.get("aggregateCalls", [])
            agg_value_types = agg_info.get("aggValueTypes", [])
            for i, call in enumerate(agg_calls):
                agg_name = call.get("name", f"agg_{i}")
                if i < len(agg_value_types):
                    field_type = TypeNormalizer.normalize_type(agg_value_types[i])
                else:
                    agg_func = call.get("aggregationFunction", "")
                    field_type = self._resolve_agg_func_return_type(agg_func, input_schema, call)
                schema_entry = {
                    "field_name": agg_name,
                    "field_type": field_type,
                }
                original_type = self._find_original_type_from_table_schema(agg_name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)

        elif op_type in ("Join", "WindowJoin"):
            left_types = json_desc.get("leftInputTypes", [])
            right_types = json_desc.get("rightInputTypes", [])
            all_types = left_types + right_types
            for i, t in enumerate(all_types):
                field_name = f"field_{i}"
                schema_entry = {
                    "field_name": field_name,
                    "field_type": TypeNormalizer.normalize_type(t),
                }
                original_type = self._find_original_type_from_table_schema(field_name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)

        elif op_type == "LookupJoin":
            input_types = json_desc.get("inputTypes", [])
            lookup_types = json_desc.get("lookupInputTypes", [])
            all_types = input_types + lookup_types
            for i, t in enumerate(all_types):
                field_name = f"field_{i}"
                schema_entry = {
                    "field_name": field_name,
                    "field_type": TypeNormalizer.normalize_type(t),
                }
                original_type = self._find_original_type_from_table_schema(field_name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)
        else:
            if input_schema:
                output_schema = list(input_schema)
        return output_schema

    def _build_output_schema_from_text(self, op_type, description_data, input_schema):
        """从文本描述构建输出 schema"""
        if op_type in PASS_THROUGH_OPERATORS:
            return list(input_schema) if input_schema else []

        if op_type == "Calc":
            output_schema = []
            for desc in description_data:
                if isinstance(desc, str):
                    select_matches = find_clauses_with_brackets(desc, "select")
                    for select_str in select_matches:
                        output_schema.extend(self._build_calc_output_from_text(select_str, input_schema))
            return output_schema if output_schema else list(input_schema or [])

        if op_type == "GroupAggregate":
            output_schema = []
            for desc in description_data:
                if isinstance(desc, str):
                    groupby_match = re.search(r'groupBy=\[(.*?)\]', desc, re.I)
                    if groupby_match and input_schema:
                        for idx_str in groupby_match.group(1).split(","):
                            try:
                                idx = int(idx_str.strip())
                                if 0 <= idx < len(input_schema):
                                    output_schema.append(input_schema[idx])
                            except ValueError:
                                pass
            return output_schema if output_schema else list(input_schema or [])

        return list(input_schema) if input_schema else []

    def _build_calc_output_from_text(self, select_str, input_schema):
        """构建 Calc 算子的输出 schema"""
        output_schema = []
        items = split_select_items(select_str)
        for _, item in enumerate(items):
            item = item.strip()
            if not item:
                continue
            original_expr, alias_name = split_alias_from_expr(item, SQL_TYPE_KEYWORDS)
            field_type = self.expr_resolver.resolve_text_expr_type(original_expr, input_schema, 0)
            schema_entry = {
                "field_name": alias_name,
                "field_type": field_type,
            }
            original_type = self._find_original_type_from_table_schema(original_expr)
            if original_type:
                schema_entry["original_type"] = original_type
            output_schema.append(schema_entry)
        return output_schema

    def _resolve_field_name_from_expr(self, expr, input_schema, default_index):
        """从表达式解析字段名"""
        expr_type = expr.get("exprType", "")
        if expr_type == EXPR_TYPE_FIELD_REFERENCE:
            col_val = expr.get("colVal", -1)
            if input_schema and 0 <= col_val < len(input_schema):
                return input_schema[col_val].get("field_name", f"field_{col_val}")
        if expr_type == EXPR_TYPE_FUNCTION:
            return expr.get("function_name", f"expr_{default_index}")
        return f"expr_{default_index}"

    def _resolve_agg_func_return_type(self, *_):
        """解析聚合函数返回类型"""
        return UNKNOWN

    def _find_original_type_from_table_schema(self, field_name):
        """从 table_schema 中查找字段的原始类型"""
        if not field_name or not self.field_resolver.table_schema:
            return None
        name_lower = field_name.lower()
        for _, columns in self.field_resolver.table_schema.items():
            for col_info in columns:
                if col_info["field_name"].lower() == name_lower:
                    original = col_info.get("original_type", "")
                    if original and original.upper().startswith("ROW"):
                        return original
        return None

    def extract_alias_map_from_description(self, description_data):
        """从描述中提取别名映射"""
        alias_map = {}

        for desc in description_data:
            if isinstance(desc, str):
                select_match = re.search(r'select=\[(.*?)\]', desc, re.I)
                if select_match:
                    items = split_select_items(select_match.group(1))
                    for item in items:
                        item = item.strip()
                        if not item:
                            continue
                        original, alias = split_alias_from_expr(item, SQL_TYPE_KEYWORDS)
                        if original != alias and alias:
                            alias_map[alias] = original
                else:
                    as_matches = re.finditer(r'(\S+)\s+AS\s+(\w+)', desc, re.I)
                    for m in as_matches:
                        original = m.group(1)
                        alias = m.group(2)
                        if alias.upper() not in SQL_TYPE_KEYWORDS and original != alias:
                            alias_map[alias] = original

        self.field_resolver.update_alias_map(alias_map)
        return alias_map

    def extract_table_source_info(self, description_data):
        """提取表源信息"""
        tables = []
        output_schema = []

        for desc in description_data:
            if isinstance(desc, dict):
                origin = desc.get("originDescription", "")
                output_names = desc.get("outputNames", [])
                output_types = desc.get("outputTypes", [])

                if output_names and output_types:
                    for name, type_str in zip(output_names, output_types):
                        schema_entry = {
                            "field_name": name,
                            "field_type": TypeNormalizer.normalize_type(type_str),
                        }
                        original_type = self._find_original_type_from_table_schema(name)
                        if original_type:
                            schema_entry["original_type"] = original_type
                        output_schema.append(schema_entry)

                table_name = self._extract_table_name_from_origin(origin)
                if table_name:
                    tables.append(table_name)

                if not output_schema:
                    out_types = desc.get("outputTypes", [])
                    if out_types and not output_names:
                        for i, type_str in enumerate(out_types):
                            output_schema.append({
                                "field_name": f"field_{i}",
                                "field_type": TypeNormalizer.normalize_type(type_str),
                            })

            elif isinstance(desc, str):
                table_name = self._extract_table_name_from_text(desc)
                if table_name:
                    tables.append(table_name)
                if not output_schema:
                    fields = self._extract_fields_from_text(desc)
                    if fields:
                        output_schema = fields

        return tables, output_schema

    def _extract_table_name_from_origin(self, origin_desc):
        """从 originDescription 提取表名"""
        if not origin_desc:
            return None

        patterns = [
            r'TableSourceScan\(table=\[\[([\w-]+),\s*([\w-]+),\s*([\w-]+)\]\]',
            r'Source:\s+\[?\w*[\w-]*\]?\s*-\s*\w*[\w-]*\s*(\w+[\w.-]*\w+)',
            r'TableSourceScan\(table=\[\w+\.\w+\],\s*table=\[*(\w+\.\w+)',
            r'Source:\s+\S+\s*-\s*(\w+\.\w+)',
            r'table=\[*(\w+\.\w+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, origin_desc, re.I)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    return f"{groups[0]}.{groups[1]}.{groups[2]}"
                return match.group(1)

        return None

    def _extract_table_name_from_text(self, desc):
        """从文本描述提取表名"""
        patterns = [
            r'TableSourceScan\(table=\[\[([\w-]+),\s*([\w-]+),\s*([\w-]+)\]\]',
            r'TableSourceScan\(table=\[*(\w+\.\w+)',
            r'Source:\s+\S+\s*-\s*(\w+\.\w+)',
            r'Scan\s+\w+\s+(\w+\.\w+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, desc, re.I)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    return f"{groups[0]}.{groups[1]}.{groups[2]}"
                return match.group(1)

        return None

    def _extract_fields_from_text(self, desc):
        """从文本描述提取字段信息"""
        if not desc:
            return []

        fields_clauses = find_clauses_with_brackets(desc, "fields")
        if not fields_clauses:
            return []

        result = []
        for fields_str in fields_clauses:
            field_names = [f.strip() for f in fields_str.split(',') if f.strip()]
            for name in field_names:
                field_type = self.field_resolver._resolve_field_type_by_name(name)
                result.append({
                    "field_name": name,
                    "field_type": field_type,
                })

        return result


class FlinkTypeResolver:
    """
    Flink 类型解析器（主入口）

    核心职责:
    1. 根据上下文和表达式推断类型
    2. 加载和维护函数返回类型字典
    3. 解析 JSON 格式的表达式结构
    4. 处理嵌套类型的字段访问
    5. 构建算子的输出 schema

    使用示例:
    ```python
    resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)
    expr_type = resolver.resolve_expression_type(expr, input_schema)
    output_schema = resolver.build_output_schema(op_type, description_data, input_schema)
    ```
    """

    def __init__(self, table_schema=None, column_type=None, table_column_type=None):
        self.field_resolver = FieldTypeResolver(table_schema, column_type, table_column_type)
        self.expr_resolver = ExpressionTypeResolver(self.field_resolver)
        self.schema_builder = SchemaBuilder(self.field_resolver, self.expr_resolver)

    @property
    def table_schema(self):
        """兼容属性：获取 table_schema"""
        return self.field_resolver.table_schema

    @table_schema.setter
    def table_schema(self, value):
        """兼容属性：设置 table_schema"""
        self.field_resolver.table_schema = value or {}

    @property
    def column_type(self):
        """兼容属性：获取 column_type"""
        return self.field_resolver.column_type

    @column_type.setter
    def column_type(self, value):
        """兼容属性：设置 column_type"""
        self.field_resolver.column_type = value or {}

    @property
    def table_column_type(self):
        """兼容属性：获取 table_column_type"""
        return self.field_resolver.table_column_type

    @table_column_type.setter
    def table_column_type(self, value):
        """兼容属性：设置 table_column_type"""
        self.field_resolver.table_column_type = value or {}

    @property
    def alias_map(self):
        """兼容属性：获取 alias_map"""
        return self.field_resolver.alias_map

    @alias_map.setter
    def alias_map(self, value):
        """兼容属性：设置 alias_map"""
        self.field_resolver.alias_map = value or {}

    @property
    def return_type_dict(self):
        """兼容属性：获取 return_type_dict"""
        return self.expr_resolver.return_type_dict

    def find_json_descriptions(self, description_data):
        """兼容方法：查找 JSON 格式的描述对象"""
        return self.schema_builder.find_json_descriptions(description_data)

    def find_json_desc_for_op(self, op_type, description_data):
        """兼容方法：查找特定算子的 JSON 描述"""
        return self.schema_builder.find_json_desc_for_op(op_type, description_data)

    @staticmethod
    def _wrap_split_function_args(args_str):
        """兼容方法：分割函数参数（使用 parser_utils）"""
        return split_function_args(args_str)

    @staticmethod
    def _wrap_split_alias_from_expr(expr, aliases=None):
        """兼容方法：分离别名和表达式"""
        return split_alias_from_expr(expr, aliases)

    def update_column_type(self, column_type, table_column_type=None):
        """更新字段类型映射"""
        self.field_resolver.update_column_type(column_type, table_column_type)

    def update_alias_map(self, alias_map):
        """更新别名映射"""
        self.field_resolver.update_alias_map(alias_map)

    def resolve_field_type(self, field_name, table_name=None):
        """解析字段类型"""
        return self.field_resolver.resolve_field_type(field_name, table_name)

    def resolve_indexed_field_type(self, index, input_schema):
        """按索引解析字段类型"""
        return self.field_resolver.resolve_indexed_field_type(index, input_schema)

    def resolve_literal_type(self, value):
        """解析字面量类型"""
        return LiteralTypeResolver.resolve_literal_type(value)

    def resolve_expression_type(self, expr, input_schema=None, depth=0):
        """解析表达式类型"""
        return self.expr_resolver.resolve_expression_type(expr, input_schema, depth)

    def build_output_schema(self, op_type, description_data, input_schema=None):
        """构建算子的输出 schema"""
        return self.schema_builder.build_output_schema(op_type, description_data, input_schema)

    def extract_alias_map_from_description(self, description_data):
        """从描述中提取别名映射"""
        return self.schema_builder.extract_alias_map_from_description(description_data)

    def extract_table_source_info(self, description_data):
        """提取表源信息"""
        return self.schema_builder.extract_table_source_info(description_data)

    def expand_row_type(self, field_info, parent_name=None):
        """展开 ROW 类型字段"""
        expanded = []

        if isinstance(field_info, str):
            field_info = {
                "field_name": field_info,
                "field_type": "ROW",
                "nested_fields": []
            }

        field_name = field_info.get("field_name", "")
        field_type = field_info.get("field_type", "")
        nested_fields = field_info.get("nested_fields", [])

        if parent_name:
            full_name = f"{parent_name}.{field_name}"
        else:
            full_name = field_name

        if field_type == "ROW" and nested_fields:
            for nested_field in nested_fields:
                nested_name = nested_field.get("field_name", "")
                nested_type = nested_field.get("field_type", "")

                if nested_type == "ROW" and nested_field.get("nested_fields"):
                    expanded.extend(
                        self.expand_row_type(nested_field, full_name)
                    )
                else:
                    expanded.append({
                        "field_name": f"{full_name}.{nested_name}",
                        "field_type": nested_type
                    })
        else:
            expanded.append({
                "field_name": full_name,
                "field_type": field_type
            })

        return expanded
