import json
import os
import re

from omnihelper.flink.schema.table_schema_reader import TableSchemaReader
from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.log import logger


MAX_DEPTH = 10

UNKNOWN = "unknown"
NESTED_FUNCTION = "NESTED_FUNCTION"

OMNI_TYPE_ID_MAP = {
    1: "INT", 2: "BIGINT", 3: "DOUBLE", 4: "FLOAT", 5: "BOOLEAN",
    6: "TINYINT", 7: "SMALLINT", 8: "DATE", 9: "TIME", 10: "TIMESTAMP",
    11: "TIMESTAMP_LTZ", 12: "DECIMAL", 13: "BINARY", 14: "CHAR",
    15: "VARCHAR", 16: "ARRAY", 17: "MAP", 18: "ROW", 19: "VARBINARY",
    20: "MULTISET",
}

TYPE_PATTERNS = [
    (re.compile(r"^true$|^false$", re.I), "BOOLEAN"),
    (re.compile(r"^NULL$", re.I), "NULL"),
    (re.compile(r"^-?\d+$"), "INT"),
    (re.compile(r"^-?\d+[Ll]$"), "BIGINT"),
    (re.compile(r"^-?\d+\.\d+$"), "DOUBLE"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "DATE"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"), "TIMESTAMP"),
    (re.compile(r"^INTERVAL\s+", re.I), "INTERVAL"),
]


class FlinkTypeResolver:
    def __init__(self, table_schema=None, column_type=None, table_column_type=None):
        self.table_schema = table_schema or {}
        self.column_type = column_type or {}
        self.table_column_type = table_column_type or {}
        self.alias_map = {}
        self.return_type_dict = {}
        self._load_return_type_dict()

    @staticmethod
    def _normalize_return_type(return_type):
        if return_type is None:
            return None
        if isinstance(return_type, int):
            return OMNI_TYPE_ID_MAP.get(return_type, UNKNOWN)
        type_str = str(return_type).strip()
        if type_str.isdigit():
            return OMNI_TYPE_ID_MAP.get(int(type_str), UNKNOWN)
        return TypeNormalizer.normalize_type(type_str)

    def _load_return_type_dict(self):
        try:
            dict_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "resources",
                "flink_function_return_type.json",
            )
            with open(dict_path, "r", encoding="utf-8") as f:
                self.return_type_dict = {
                    item["func_name"].lower(): item for item in json.load(f)
                }
            logger.info(f"Loaded {len(self.return_type_dict)} function return type entries")
        except Exception as e:
            logger.warning(f"Failed to load flink_function_return_type.json: {e}")

    def update_column_type(self, column_type, table_column_type=None):
        if column_type:
            self.column_type.update(column_type)
        if table_column_type:
            self.table_column_type.update(table_column_type)

    def update_alias_map(self, alias_map):
        if alias_map:
            self.alias_map.update(alias_map)

    def resolve_field_type(self, field_name, table_name=None):
        if table_name:
            key = f"{table_name}.{field_name}".lower()
            if key in self.table_column_type:
                return self.table_column_type[key]

        name_lower = field_name.lower()
        if name_lower in self.column_type:
            return self.column_type[name_lower]

        if "." in field_name:
            nested_type = self._resolve_nested_field_path(field_name, table_name)
            if nested_type and nested_type != UNKNOWN:
                return nested_type

        return UNKNOWN

    def _resolve_nested_field_path(self, dotted_path, table_name=None):
        parts = dotted_path.split(".")
        if len(parts) < 2:
            return UNKNOWN

        top_level_name = parts[0].lower()
        nested_path = parts[1:]

        if table_name and table_name in self.table_schema:
            for col_info in self.table_schema[table_name]:
                if col_info["field_name"].lower() == top_level_name:
                    return TableSchemaReader.resolve_nested_field_type(col_info, nested_path)

        for tbl_name, columns in self.table_schema.items():
            for col_info in columns:
                if col_info["field_name"].lower() == top_level_name:
                    return TableSchemaReader.resolve_nested_field_type(col_info, nested_path)

        return UNKNOWN

    def _resolve_field_path_from_schema(self, col_info, field_path):
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
        if not input_schema or index < 0 or index >= len(input_schema):
            return UNKNOWN
        return input_schema[index].get("field_type", UNKNOWN)

    def resolve_literal_type(self, value):
        if value is None:
            return "NULL"

        if isinstance(value, bool):
            return "BOOLEAN"

        if isinstance(value, int):
            return "INT"

        if isinstance(value, float):
            return "DOUBLE"

        if isinstance(value, str):
            value_str = value.strip()
            if not value_str:
                return UNKNOWN

            if (value_str.startswith("'") and value_str.endswith("'")) or \
               (value_str.startswith('"') and value_str.endswith('"')):
                return "VARCHAR"

            for pattern, match_type in TYPE_PATTERNS:
                if pattern.match(value_str):
                    return match_type

            return UNKNOWN

        return UNKNOWN

    def resolve_expression_type(self, expr, input_schema=None, depth=0):
        if depth > MAX_DEPTH:
            return UNKNOWN

        if not expr:
            return UNKNOWN

        if isinstance(expr, dict):
            return self._resolve_json_expr_type(expr, input_schema, depth)

        if isinstance(expr, str):
            return self._resolve_text_expr_type(expr, input_schema, depth)

        return UNKNOWN

    def _resolve_json_expr_type(self, expr, input_schema, depth):
        expr_type = expr.get("exprType", "")

        if expr_type == "FIELD_REFERENCE":
            col_val = expr.get("colVal", -1)
            data_type = expr.get("dataType")
            if data_type:
                return self._normalize_return_type(data_type)
            field_path = expr.get("fieldPath")
            if field_path and input_schema and 0 <= col_val < len(input_schema):
                col_info = input_schema[col_val]
                resolved = self._resolve_field_path_from_schema(col_info, field_path)
                if resolved and resolved != UNKNOWN:
                    return resolved
            if input_schema and 0 <= col_val < len(input_schema):
                base_type = self.resolve_indexed_field_type(col_val, input_schema)
                if base_type != "ROW":
                    return base_type
                col_info = input_schema[col_val]
                col_name = col_info.get("field_name", "")
                if col_name:
                    nested_path = field_path if field_path else []
                    if nested_path:
                        resolved = self._resolve_nested_field_path(
                            ".".join([col_name] + nested_path)
                        )
                        if resolved and resolved != UNKNOWN:
                            return resolved
            return UNKNOWN

        if expr_type == "LITERAL":
            data_type = expr.get("dataType")
            if data_type:
                return self._normalize_return_type(data_type)
            if expr.get("isNull", False):
                return "NULL"
            return self.resolve_literal_type(expr.get("value"))

        if expr_type == "BINARY":
            return_type = expr.get("returnType")
            if return_type is not None:
                return self._normalize_return_type(return_type)
            return UNKNOWN

        if expr_type == "UNARY":
            return_type = expr.get("returnType")
            if return_type is not None:
                return self._normalize_return_type(return_type)
            inner = expr.get("expr")
            if inner:
                return self.resolve_expression_type(inner, input_schema, depth + 1)
            return UNKNOWN

        if expr_type == "FUNCTION":
            return self._resolve_function_expr_type(expr, input_schema, depth)

        if expr_type in ("SWITCH", "CASE"):
            return self._resolve_case_expr_type(expr, input_schema, depth)

        if expr_type == "COALESCE":
            return self._resolve_coalesce_expr_type(expr, input_schema, depth)

        if expr_type == "IS_NOT_NULL":
            return "BOOLEAN"

        if expr_type == "MULTIPLE_AND_OR":
            return "BOOLEAN"

        if expr_type in ("IN", "BETWEEN"):
            return "BOOLEAN"

        if expr_type in ("REGEX_EXTRACT", "SPLIT_INDEX"):
            return_type = expr.get("returnType")
            if return_type is not None:
                return self._normalize_return_type(return_type)
            return "VARCHAR"

        if expr_type == "PROCTIME":
            return "TIMESTAMP"

        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)

        return UNKNOWN

    def _resolve_function_expr_type(self, expr, input_schema, depth):
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

        if rule == "ARGUMENT_TYPE":
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

        if rule == "RESULT_TYPE":
            return self._resolve_result_type(func_name, arguments, input_schema, depth)

        return UNKNOWN

    def _resolve_result_type(self, func_name, arguments, input_schema, depth):
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

    def _resolve_text_expr_type(self, expr_str, input_schema, depth):
        if not expr_str or not isinstance(expr_str, str):
            return UNKNOWN

        expr_str = expr_str.strip()
        if not expr_str:
            return UNKNOWN

        if (expr_str.startswith("'") and expr_str.endswith("'")) or \
           (expr_str.startswith('"') and expr_str.endswith('"')):
            return "VARCHAR"

        for pattern, match_type in TYPE_PATTERNS:
            if pattern.match(expr_str):
                return match_type

        if expr_str.upper() == "NULL":
            return "NULL"

        if expr_str.upper() in ("TRUE", "FALSE"):
            return "BOOLEAN"

        name_lower = expr_str.lower()
        if name_lower in self.column_type:
            return self.column_type[name_lower]

        if "." in expr_str:
            nested_type = self._resolve_nested_field_path(expr_str)
            if nested_type and nested_type != UNKNOWN:
                return nested_type

        if input_schema:
            for field in input_schema:
                if field.get("field_name", "").lower() == name_lower:
                    return field.get("field_type", UNKNOWN)

        alias_resolved = self._resolve_alias(expr_str)
        if alias_resolved and alias_resolved != UNKNOWN:
            return alias_resolved

        comparison_type = self._resolve_comparison_type(expr_str, input_schema, depth)
        if comparison_type:
            return comparison_type

        func_type = self._resolve_text_function_type(expr_str, input_schema, depth)
        if func_type and func_type != UNKNOWN:
            return func_type

        return UNKNOWN

    def _resolve_comparison_type(self, expr_str, input_schema, depth):
        op_match = re.match(r'^(.+?)\s*(=|<>|!=|>=|<=|>|<)\s*(.+)$', expr_str.strip())
        if not op_match:
            return None
        left = op_match.group(1).strip()
        op = op_match.group(2)
        right = op_match.group(3).strip()
        if not left or not right:
            return None
        left_type = self._resolve_text_expr_type(left, input_schema, depth + 1)
        right_type = self._resolve_text_expr_type(right, input_schema, depth + 1)
        if left_type != UNKNOWN or right_type != UNKNOWN:
            return "BOOLEAN"
        return None

    def _resolve_alias(self, param):
        alias_param = re.sub(r"\[\d+\]$", "", param)
        if alias_param in self.alias_map:
            real_param = self.alias_map[alias_param]
            return self._resolve_text_expr_type(real_param, None, 0)
        return None

    def _resolve_text_function_type(self, expr_str, input_schema, depth):
        func_match = re.match(r'^([a-zA-Z_]\w*)\s*\(', expr_str)
        if not func_match:
            return None

        func_name = func_match.group(1).lower()
        dict_entry = self.return_type_dict.get(func_name)
        if not dict_entry:
            return None

        if func_name == "case":
            return self._resolve_case_return_type_from_text(expr_str, input_schema, depth)

        if not dict_entry.get("need_param_type", False):
            ret = dict_entry.get("return_type", UNKNOWN)
            return ret if ret != UNKNOWN else None

        rule = dict_entry.get("return_type", "")
        if rule == "ARGUMENT_TYPE":
            return self._extract_first_arg_type_from_text(expr_str, input_schema, depth)

        if rule == "RESULT_TYPE":
            if func_name in ("cast", "try_cast"):
                cast_match = re.search(r'\bAS\s+(\w+)', expr_str, re.I)
                if cast_match:
                    return TypeNormalizer.normalize_type(cast_match.group(1))
            return None

        return None

    def _resolve_case_return_type_from_text(self, expr_str, input_schema, depth):
        args_str = self._extract_function_args_text(expr_str)
        if not args_str:
            return UNKNOWN

        args = self._split_function_args(args_str)
        value_types = []
        has_else = len(args) % 2 == 1
        for i, arg in enumerate(args):
            arg = arg.strip()
            if i % 2 == 0 and not (has_else and i == len(args) - 1):
                continue
            t = self._resolve_text_expr_type(arg, input_schema, depth + 1)
            if t and t != UNKNOWN:
                value_types.append(t)

        if not value_types:
            return UNKNOWN

        common = TypeNormalizer.find_common_type_multi(value_types)
        if common and common != UNKNOWN:
            return common

        return UNKNOWN

    def _extract_first_arg_type_from_text(self, expr_str, input_schema, depth):
        inner = self._extract_function_args_text(expr_str)
        if not inner:
            return None

        first_arg = inner.split(",")[0].strip()
        if not first_arg:
            return None

        arg_type = self._resolve_text_expr_type(first_arg, input_schema, depth + 1)
        return arg_type if arg_type != UNKNOWN else None

    @staticmethod
    def _extract_function_args_text(expr_str):
        start = expr_str.find("(")
        if start == -1:
            return None

        depth = 0
        for i in range(start, len(expr_str)):
            if expr_str[i] == "(":
                depth += 1
            elif expr_str[i] == ")":
                depth -= 1
                if depth == 0:
                    return expr_str[start + 1 : i]

        return expr_str[start + 1 :]

    @staticmethod
    def _split_function_args(args_str):
        if not args_str:
            return []
        parts = []
        depth = 0
        current = []
        for ch in args_str:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current))
        return parts

    def find_json_descriptions(self, description_data):
        results = []
        if not description_data:
            return results

        for item in description_data:
            if isinstance(item, dict):
                if "inputTypes" in item or "outputTypes" in item or "originDescription" in item:
                    results.append(item)

        return results

    def find_json_desc_for_op(self, op_type, description_data):
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

    def _parse_text_output_types(self, matched_text, input_schema):
        items = [item.strip() for item in matched_text.split(",") if item.strip()]
        if not items:
            return []

        all_int = True
        indices = []
        for item in items:
            try:
                indices.append(int(item))
            except ValueError:
                all_int = False
                break

        if all_int and indices and input_schema:
            types = []
            for idx in indices:
                if 0 <= idx < len(input_schema):
                    types.append(input_schema[idx].get("field_type", UNKNOWN))
                else:
                    types.append(UNKNOWN)
            return types

        return self._parse_select_types(matched_text, input_schema)

    def _parse_select_types(self, select_str, input_schema):
        types = []
        items = self._split_select_items(select_str)
        for item in items:
            item = item.strip()
            if not item:
                continue

            original_expr, _ = self._split_alias_from_expr(item)
            t = self._resolve_text_expr_type(original_expr, input_schema, 0)
            types.append(t)

        return types

    @staticmethod
    def _split_select_items(text):
        items = []
        current = []
        depth = 0

        for char in text:
            if char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                items.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            items.append("".join(current))

        return items

    def extract_alias_map_from_description(self, description_data):
        alias_map = {}
        for desc in description_data:
            if isinstance(desc, str):
                select_match = re.search(r'select=\[(.*?)\]', desc, re.I)
                if select_match:
                    items = self._split_select_items(select_match.group(1))
                    for item in items:
                        item = item.strip()
                        if not item:
                            continue
                        original, alias = self._split_alias_from_expr(item)
                        if original != alias and alias:
                            alias_map[alias] = original
                else:
                    as_matches = re.finditer(r'(\S+)\s+AS\s+(\w+)', desc, re.I)
                    for m in as_matches:
                        original = m.group(1)
                        alias = m.group(2)
                        if alias.upper() in ("INT", "BIGINT", "VARCHAR", "DOUBLE", "FLOAT",
                                             "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL",
                                             "STRING", "LONG", "SHORT", "BYTE", "CHAR"):
                            continue
                        if original != alias:
                            alias_map[alias] = original

        self.update_alias_map(alias_map)
        return alias_map

    def extract_table_source_info(self, description_data):
        tables = []
        output_schema = []

        for desc in description_data:
            if isinstance(desc, dict):
                origin = desc.get("originDescription", "")
                output_names = desc.get("outputNames", [])
                output_types = desc.get("outputTypes", [])

                if output_names and output_types:
                    for name, type_str in zip(output_names, output_types):
                        output_schema.append({
                            "field_name": name,
                            "field_type": TypeNormalizer.normalize_type(type_str),
                        })

                table_name = self._extract_table_name_from_origin(origin)
                if table_name:
                    tables.append(table_name)

                if not output_schema:
                    input_types = desc.get("inputTypes", [])
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
        if not desc:
            return []

        fields_match = re.search(r'fields=\[([^\]]+)\]', desc)
        if not fields_match:
            return []

        fields_str = fields_match.group(1)
        field_names = [f.strip() for f in fields_str.split(',') if f.strip()]
        if not field_names:
            return []

        result = []
        for name in field_names:
            field_type = self._resolve_field_type_by_name(name)
            result.append({
                "field_name": name,
                "field_type": field_type,
            })
        return result

    def _resolve_field_type_by_name(self, field_name):
        if self.column_type and field_name in self.column_type:
            return TypeNormalizer.normalize_type(self.column_type[field_name])
        if self.table_column_type:
            for key, type_val in self.table_column_type.items():
                if key.endswith(f".{field_name}"):
                    return TypeNormalizer.normalize_type(type_val)
        return "unknown"

    def _resolve_field_name_from_expr(self, expr, input_schema, default_index):
        expr_type = expr.get("exprType", "")
        if expr_type == "FIELD_REFERENCE":
            col_val = expr.get("colVal", -1)
            if input_schema and 0 <= col_val < len(input_schema):
                return input_schema[col_val].get("field_name", f"field_{col_val}")
        if expr_type == "FUNCTION":
            return expr.get("function_name", f"expr_{default_index}")
        return f"expr_{default_index}"

    def build_output_schema(self, op_type, description_data, input_schema=None):
        if not description_data:
            return input_schema or []

        json_desc = self.find_json_desc_for_op(op_type, description_data)
        if json_desc:
            return self._build_output_schema_from_json(op_type, json_desc, input_schema)

        return self._build_output_schema_from_text(op_type, description_data, input_schema)

    def _build_output_schema_from_json(self, op_type, json_desc, input_schema):
        output_schema = []
        output_names = json_desc.get("outputNames", [])
        output_types = json_desc.get("outputTypes", [])

        if output_names and output_types:
            for name, type_str in zip(output_names, output_types):
                output_schema.append({
                    "field_name": name,
                    "field_type": TypeNormalizer.normalize_type(type_str),
                })
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
                    field_type = self.resolve_expression_type(idx_expr, input_schema)
                    output_schema.append({
                        "field_name": field_name,
                        "field_type": field_type,
                    })

        elif op_type == "GroupAggregate":
            grouping = json_desc.get("grouping", [])
            if grouping and input_schema:
                for idx in grouping:
                    if 0 <= idx < len(input_schema):
                        output_schema.append({
                            "field_name": input_schema[idx].get("field_name", f"group_{idx}"),
                            "field_type": input_schema[idx].get("field_type", UNKNOWN),
                        })

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
                output_schema.append({
                    "field_name": agg_name,
                    "field_type": field_type,
                })

        elif op_type in ("Join", "WindowJoin"):
            left_types = json_desc.get("leftInputTypes", [])
            right_types = json_desc.get("rightInputTypes", [])
            all_types = left_types + right_types
            for i, t in enumerate(all_types):
                output_schema.append({
                    "field_name": f"field_{i}",
                    "field_type": TypeNormalizer.normalize_type(t),
                })

        elif op_type == "LookupJoin":
            input_types = json_desc.get("inputTypes", [])
            lookup_types = json_desc.get("lookupInputTypes", [])
            all_types = input_types + lookup_types
            for i, t in enumerate(all_types):
                output_schema.append({
                    "field_name": f"field_{i}",
                    "field_type": TypeNormalizer.normalize_type(t),
                })

        else:
            if input_schema:
                output_schema = list(input_schema)

        return output_schema

    def _build_output_schema_from_text(self, op_type, description_data, input_schema):
        if op_type in ("Deduplicate", "Expand", "WatermarkAssigner",
                       "StreamRecordTimestampInserter", "ConstraintEnforcer", "Sink"):
            return list(input_schema) if input_schema else []

        if op_type == "Calc":
            for desc in description_data:
                if isinstance(desc, str):
                    select_match = re.search(r'select=\[(.*?)\]', desc, re.I)
                    if select_match:
                        return self._build_calc_output_from_text(select_match.group(1), input_schema)

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
        output_schema = []
        items = self._split_select_items(select_str)
        for i, item in enumerate(items):
            item = item.strip()
            if not item:
                continue

            original_expr, alias_name = self._split_alias_from_expr(item)
            field_type = self._resolve_text_expr_type(original_expr, input_schema, 0)
            output_schema.append({
                "field_name": alias_name,
                "field_type": field_type,
            })

        return output_schema

    @staticmethod
    def _split_alias_from_expr(expr_str):
        depth = 0
        last_as_pos = -1
        upper = expr_str.upper()
        i = 0
        while i < len(expr_str):
            c = expr_str[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif depth == 0 and upper[i:i+4] == " AS " and (i + 4 <= len(expr_str)):
                last_as_pos = i
            i += 1

        if last_as_pos >= 0:
            original = expr_str[:last_as_pos].strip()
            alias = expr_str[last_as_pos + 4:].strip()
            if alias and not alias.upper() in (
                "INT", "BIGINT", "VARCHAR", "DOUBLE", "FLOAT",
                "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL",
                "STRING", "LONG", "SHORT", "BYTE", "CHAR",
                "TINYINT", "SMALLINT", "BINARY", "VARBINARY",
                "ARRAY", "MAP", "ROW", "MULTISET",
            ):
                return original, alias

        return expr_str, expr_str
