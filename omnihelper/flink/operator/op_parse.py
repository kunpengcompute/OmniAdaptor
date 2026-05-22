"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
              http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.

函数解析模块 - 负责从 Flink 物理计划中解析函数调用
"""

import json
import re
import os
import html
from collections import defaultdict
from json import JSONDecodeError
from omnihelper.flink.function.function_parse import FlinkFunctionParser
from omnihelper.flink.schema.type_resolver import FlinkTypeResolver
from omnihelper.flink.schema.table_schema_reader import TableSchemaReader
from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger
from omnihelper.constants.flink_constants import ExcelColumns, TaskStatus


class FlinkParser:
    EXCEL_COLUMNS = [
        ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
        ExcelColumns.OPERATOR_NAME, ExcelColumns.IS_SUPPORTED, ExcelColumns.INPUT,
        ExcelColumns.OUTPUT, ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME,
        ExcelColumns.INPUT_DATA_SIZE, ExcelColumns.OUTPUT_DATA_SIZE,
        ExcelColumns.FUNC_NAME, ExcelColumns.FUNC_INPUT,
        ExcelColumns.NESTED_CONTENT, ExcelColumns.FUNC_FREQUENCY
    ]

    SOURCE_OP_TYPES = {"Csv Source", "KafKa Source", "TableSourceScan", "Source"}
    PASS_THROUGH_OP_TYPES = {"Deduplicate", "Expand", "WatermarkAssigner",
                             "StreamRecordTimestampInserter", "ConstraintEnforcer"}

    def __init__(self, table_schema=None, column_type=None, table_column_type=None):
        self.function_parser = FlinkFunctionParser()
        self.op_dictionary = {}
        self.dictionary_path = os.path.join(self.get_resource_path(), "flink_op_dictionary.json")
        self._load_op_dictionary()
        self.supported_operators = set(self.op_dictionary.keys())
        self.type_resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)
        self.schema_chain = {}

    @staticmethod
    def get_resource_path():
        return os.path.join(CommonUtil.get_execute_path(), "resources")

    @staticmethod
    def parse_single_description_line(line):
        if not line:
            return None
        clean_part = line.strip(" :+- \t")
        if not clean_part:
            return None

        if clean_part.startswith(("{", "[")):
            try:
                return json.loads(clean_part)
            except (JSONDecodeError, TypeError) as e:
                logger.debug(f"Failed to parse JSON: {e}")
        return clean_part

    @staticmethod
    def get_description(job_detail, job_id):
        plan = job_detail.get("plan", {})
        if not isinstance(plan, dict) or "nodes" not in plan:
            return {job_id: {}}

        vertex_map = {}
        for node in plan.get("nodes", []):
            vertex_id = node.get("id")
            if not vertex_id:
                continue
            description = node.get("description", "")
            if description:
                description = html.unescape(description)
            if not description:
                continue
            raw_parts = re.split(r"<br/>[\s:*\+\-]*|\n", description)
            description_data = [
                parsed_line for line in raw_parts
                if (parsed_line := FlinkParser.parse_single_description_line(line)) is not None
            ]
            vertex_map[vertex_id] = {"plan_desc": description_data}

        return {job_id: vertex_map}

    @staticmethod
    def filter_num_data(available, target_metrics):
        if not available:
            return []
        return [m['id'] for m in available if any(m['id'].endswith(s) for s in target_metrics)]

    @staticmethod
    def operator_analysis(jobs, metrics):
        op_pattern = r"\[(\d+)\]:([A-Za-z]+)"
        ops = {
            m.group(1): {"type": m.group(2), "vertex": vertex_id, "job": job_id}
            for job_id, vertices in jobs.items()
            for vertex_id, vertex in vertices.items()
            for desc in vertex["plan_desc"]
            if (m := re.match(op_pattern, desc))
        }

        metric_pattern = r"(\d+)\.([A-Za-z_]+)\[(\d+)\]\.(\w+)"
        agg = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for vertex_id, vertex_metrics in metrics.items():
            for key, value in vertex_metrics.items():
                if m := re.match(metric_pattern, key):
                    _, _, op_id, metric = m.groups()
                    agg[vertex_id][op_id][metric].append(float(value))

        out_put = {}
        for vertex_id, ops_metrics in agg.items():
            job_id = None
            for op_id, info in ops.items():
                if info["vertex"] == vertex_id:
                    job_id = info["job"]
                    break
            if job_id is None:
                continue
            out_put.setdefault(job_id, {})
            out_put[job_id].setdefault(vertex_id, [])

            for op_id, metrics_dict in ops_metrics.items():
                op_type = ops[op_id]["type"]
                out_put[job_id][vertex_id].append({
                    "op_id": int(op_id),
                    "op_type": op_type,
                    "metrics": {metric: sum(vals) for metric, vals in metrics_dict.items()}
                })
        return out_put

    @staticmethod
    def safe_float(val):
        if not val:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def group_metrics_by_operator(raw_map):
        operator_stats = {}
        for full_id, val in raw_map.items():
            parts = full_id.split(".")
            if len(parts) < 2:
                continue
            operator = ".".join(parts[:-1])
            metric = parts[-1]
            if operator not in operator_stats:
                operator_stats[operator] = {
                    "numRecordsIn": 0,
                    "numRecordsInPerSecond": 0.0,
                    "numRecordsOut": 0,
                    "numRecordsOutPerSecond": 0.0,
                    "numBytesIn": 0,
                    "numBytesInPerSecond": 0.0,
                    "numBytesOut": 0,
                    "numBytesOutPerSecond": 0.0,
                }
            if metric in operator_stats[operator]:
                val_f = FlinkParser.safe_float(val)
                if "PerSecond" in metric:
                    operator_stats[operator][metric] = round(val_f, 2)
                else:
                    operator_stats[operator][metric] = int(val_f)
        return operator_stats

    @staticmethod
    def calc_active_duration(operator_stats):
        for op, stats in operator_stats.items():
            rps_in = stats["numRecordsInPerSecond"]
            cnt_in = stats["numRecordsIn"]
            stats["active_duration_in"] = round(cnt_in / rps_in, 2) if rps_in > 0 else 0.0

            cnt_out = stats["numRecordsOut"]
            rps_out = stats["numRecordsOutPerSecond"]
            stats["active_duration_out"] = round(cnt_out / rps_out, 2) if rps_out > 0 else 0.0
        return operator_stats

    @staticmethod
    def calc_summary(operator_stats):
        return {
            "totalRecordsIn": sum(stats["numRecordsIn"] for stats in operator_stats.values()),
            "totalRecordsOut": sum(stats["numRecordsOut"] for stats in operator_stats.values()),
            "totalBytesIn": sum(stats["numBytesIn"] for stats in operator_stats.values()),
            "totalBytesOut": sum(stats["numBytesOut"] for stats in operator_stats.values()),
            "avgRecordsInPerSecond": round(sum(stats["numRecordsInPerSecond"] for stats in operator_stats.values())),
            "avgRecordsOutPerSecond": round(sum(stats["numRecordsOutPerSecond"] for stats in operator_stats.values())),
        }

    @staticmethod
    def restructure_by_op_type(analysis):
        operators_by_type = {}
        for job_id, vertices in analysis.items():
            for vertex_id, ops_list in vertices.items():
                for op in ops_list:
                    op_type = op["op_type"]
                    operators_by_type.setdefault(op_type, [])
                    operators_by_type[op_type].append({
                        "op_id": op["op_id"],
                        "metrics": op["metrics"]
                    })
        return operators_by_type

    @staticmethod
    def aggregate_metrics(op_list):
        num_in = sum(op["metrics"].get("numRecordsIn", 0) for op in op_list)
        num_in_sec = sum(op["metrics"].get("numRecordsInPerSecond", 0.0) for op in op_list)
        num_out = sum(op["metrics"].get("numRecordsOut", 0) for op in op_list)
        num_out_sec = sum(op["metrics"].get("numRecordsOutPerSecond", 0.0) for op in op_list)
        return num_in, num_in_sec, num_out, num_out_sec

    @staticmethod
    def compute_runtime(num_in, num_in_sec, num_out, num_out_sec):
        run_time = 0.0
        if num_in_sec > 0:
            run_time += num_in / num_in_sec
        if num_out_sec > 0:
            run_time += num_out / num_out_sec
        return round(run_time, 2)

    @staticmethod
    def bytes_to_mb(value):
        if not value:
            return 0.0
        return round(value / (1024 * 1024), 2)

    @staticmethod
    def parse_performance_stats(vid, metrics_raw, jobs=None):
        if not metrics_raw:
            return {"operators": {}, "summary": {}, "analysis": {}}

        raw_map = {item['id']: item['value'] for item in metrics_raw}
        operator_stats = FlinkParser.group_metrics_by_operator(raw_map)
        operator_stats = FlinkParser.calc_active_duration(operator_stats)
        summary = FlinkParser.calc_summary(operator_stats)
        analysis = {}
        operators_by_type = {}
        if jobs is not None:
            analysis = FlinkParser.operator_analysis(jobs, {vid: raw_map})
            operators_by_type = FlinkParser.restructure_by_op_type(analysis)

        return {
            "operators": operators_by_type,
            "summary": summary,
            "analysis": analysis
        }

    @staticmethod
    def _aggregate_functions_by_name(func_analysis):
        aggregated = {}
        for func_info in func_analysis:
            name = func_info.get("func_name")
            if not name:
                continue
            input_types = func_info.get("input", [])
            key = (name, tuple(input_types)) if input_types else (name, ())
            if key not in aggregated:
                aggregated[key] = {"count": 0, "inputs": set(), "name": name}
            aggregated[key]["count"] += func_info.get("times", 0)
            aggregated[key]["inputs"].update(input_types)
        return [(info["name"], {"count": info["count"], "inputs": info["inputs"]}) for info in aggregated.values()]

    @staticmethod
    def _create_empty_row(job_id, task_id, status):
        return {
            ExcelColumns.JOB_ID: job_id,
            ExcelColumns.TASK_ID: task_id,
            ExcelColumns.STATUS: status,
            ExcelColumns.OPERATOR_NAME: "",
            ExcelColumns.IS_SUPPORTED: "",
            ExcelColumns.INPUT: "",
            ExcelColumns.OUTPUT: "",
            ExcelColumns.FREQUENCY: "",
            ExcelColumns.RUNTIME: "",
            ExcelColumns.INPUT_DATA_SIZE: "",
            ExcelColumns.OUTPUT_DATA_SIZE: "",
            ExcelColumns.FUNC_NAME: "",
            ExcelColumns.FUNC_INPUT: "",
            ExcelColumns.NESTED_CONTENT: "",
            ExcelColumns.FUNC_FREQUENCY: ""
        }

    @staticmethod
    def _create_row(job_id, task_id, status, op=None, func_name="", func_inputs_str="",
                    func_nested="", func_count=""):
        is_supported_str = "是" if op.get("is_supported", False) else "否"
        return {
            ExcelColumns.JOB_ID: job_id,
            ExcelColumns.TASK_ID: task_id,
            ExcelColumns.STATUS: status,
            ExcelColumns.OPERATOR_NAME: op["op_type"],
            ExcelColumns.IS_SUPPORTED: is_supported_str,
            ExcelColumns.INPUT: op.get("input_types_str", ""),
            ExcelColumns.OUTPUT: op.get("output_types_str", ""),
            ExcelColumns.FREQUENCY: op["count"],
            ExcelColumns.RUNTIME: op["run_time"],
            ExcelColumns.INPUT_DATA_SIZE: f"{FlinkParser.bytes_to_mb(op['num_in'])}MB",
            ExcelColumns.OUTPUT_DATA_SIZE: f"{FlinkParser.bytes_to_mb(op['num_out'])}MB",
            ExcelColumns.FUNC_NAME: func_name,
            ExcelColumns.FUNC_INPUT: func_inputs_str,
            ExcelColumns.NESTED_CONTENT: func_nested,
            ExcelColumns.FUNC_FREQUENCY: func_count
        }

    @staticmethod
    def _strip_outer_parens(expr_str):
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
    def _is_operator_func(func_name):
        if not func_name:
            return False
        return len(func_name) <= 2 and not any(c.isalpha() for c in func_name)

    @staticmethod
    def _extract_operator_operands_from_desc(func_name, description):
        if not description or not func_name:
            return None, None
        clean_desc = re.sub(r'<[^>]+>[\s:*\+\-]*', ' ', description)
        op_pattern = re.compile(re.escape(func_name))
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
            if func_name == '*' and pos > 0 and clean_desc[pos - 1] == '(' and op_end < len(clean_desc) and clean_desc[
                op_end] == ')':
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
                elif depth == 0 and ch == '=':
                    if left_start + 1 < len(clean_desc) and clean_desc[left_start + 1] == '[':
                        depth += 1
                        left_start -= 1
                        continue
                    left_start += 1
                    break
                elif depth == 0 and ch == ',':
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
                right_end += 1
            left_expr = clean_desc[left_start:pos].strip()
            right_expr = clean_desc[op_end:right_end].strip()
            if left_expr and right_expr:
                return left_expr, right_expr
        return None, None

    @staticmethod
    def _extract_function_args_text_from_desc(func_name, description):
        if not description or not func_name:
            return None
        if FlinkParser._is_operator_func(func_name):
            left, right = FlinkParser._extract_operator_operands_from_desc(func_name, description)
            if left is not None and right is not None:
                return f"{left},{right}"
            return None
        func_pattern = re.compile(
            re.escape(func_name) + r'\s*\(', re.I
        )
        match = func_pattern.search(description)
        if not match:
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
        args = []
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
                args.append("".join(current))
                current = []
            else:
                current.append(char)
        if current:
            args.append("".join(current))
        return args

    @staticmethod
    def _extract_func_expression(func_name, description):
        if not description:
            return ""
        if FlinkParser._is_operator_func(func_name):
            left, right = FlinkParser._extract_operator_operands_from_desc(func_name, description)
            if left is not None and right is not None:
                return f"{left} {func_name} {right}"
            return func_name
        func_pattern = re.compile(
            re.escape(func_name) + r'\s*\(', re.I
        )
        match = func_pattern.search(description)
        if not match:
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

    @staticmethod
    def _expr_to_string(expr):
        if isinstance(expr, str):
            return expr
        if isinstance(expr, dict):
            expr_type = expr.get("exprType", "")
            if expr_type == "FIELD_REFERENCE":
                return f"field[{expr.get('colVal', '?')}]"
            if expr_type == "LITERAL":
                return str(expr.get("value", "?"))
            if expr_type == "FUNCTION":
                name = expr.get("function_name", "?")
                args = expr.get("arguments", [])
                args_str = ", ".join(FlinkParser._expr_to_string(a) for a in args)
                return f"{name}({args_str})"
            if expr_type == "BINARY":
                op = expr.get("operator", "?")
                left = FlinkParser._expr_to_string(expr.get("left", {}))
                right = FlinkParser._expr_to_string(expr.get("right", {}))
                return f"{left} {op} {right}"
            if expr_type == "UNARY":
                op = expr.get("operator", "?")
                inner = FlinkParser._expr_to_string(expr.get("expr", {}))
                return f"{op}({inner})"
        return str(expr)

    @staticmethod
    def _topological_sort(vertices):
        in_degree = {vid: 0 for vid in vertices}
        for vid, info in vertices.items():
            for uid in info.get("upstream_ids", []):
                if uid in in_degree:
                    in_degree[vid] += 1

        queue = [vid for vid, deg in in_degree.items() if deg == 0]
        result = []
        while queue:
            vid = queue.pop(0)
            result.append(vid)
            for other_id, info in vertices.items():
                if vid in info.get("upstream_ids", []):
                    in_degree[other_id] -= 1
                    if in_degree[other_id] == 0:
                        queue.append(other_id)

        for vid in vertices:
            if vid not in result:
                result.append(vid)

        return result

    @staticmethod
    def _merge_upstream_context(upstream_ids, vertex_context):
        merged_alias_map = {}
        merged_output_schema = []
        for uid in upstream_ids:
            ctx = vertex_context.get(uid)
            if not ctx:
                continue
            merged_alias_map.update(ctx.get("alias_map", {}))
            if ctx.get("output_schema"):
                if not merged_output_schema:
                    merged_output_schema = list(ctx["output_schema"])
                else:
                    merged_output_schema.extend(ctx["output_schema"])
        return {
            "alias_map": merged_alias_map,
            "output_schema": merged_output_schema,
        }

    @staticmethod
    def _aggregate_ops_by_name_and_types(ops):
        aggregated = {}
        for op in ops:
            key = (op["op_type"], op["input_types_str"], op["output_types_str"])
            if key in aggregated:
                aggregated[key]["count"] += op["count"]
                aggregated[key]["num_in"] += op["num_in"]
                aggregated[key]["num_out"] += op["num_out"]
            else:
                aggregated[key] = dict(op)
        return list(aggregated.values())

    def set_table_schema(self, table_schema, column_type, table_column_type):
        self.type_resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)

    def _analyze_operators_functions(self, description, task_id, input_schema=None):
        if not description:
            return []

        param_types_map = self._build_func_param_types_map(description, input_schema)

        unsupported_funcs = self.function_parser.analyze_unsupported_functions(
            description, param_types_map
        )
        result = []
        for func in unsupported_funcs:
            func_name = func["func_name"]
            func_input_types, nested_content = self._resolve_func_param_types(
                func_name, description, input_schema
            )
            if func_input_types and all(t != "unknown" for t in func_input_types):
                nested_content = ""
            entry = {
                "func_name": func_name,
                "task_id": task_id,
                "input": func_input_types,
                "nested_content": nested_content,
                "times": func["times"]
            }
            unsupported_types = func.get("unsupported_types", [])
            if unsupported_types:
                entry["unsupported_types"] = unsupported_types
            result.append(entry)
        return result

    def _build_func_param_types_map(self, description, input_schema=None):
        param_types_map = {}
        all_funcs = self.function_parser.parse_plan_description(description)
        for func_info in all_funcs:
            func_name = func_info.get("func", "")
            if not func_name:
                continue
            types, _ = self._resolve_func_param_types(func_name, description, input_schema)
            if types:
                param_types_map[func_name] = types
        return param_types_map

    def _resolve_func_param_types(self, func_name, description, input_schema=None):
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
        input_types = []
        nested_content = existing_nested

        indices = json_desc.get("indices", [])
        condition = json_desc.get("condition")

        all_exprs = list(indices)
        if condition:
            all_exprs.append(condition)

        for expr in all_exprs:
            if isinstance(expr, dict):
                expr_type = expr.get("exprType", "")
                if expr_type == "FUNCTION" and expr.get("function_name", "").lower() == func_name.lower():
                    arguments = expr.get("arguments", [])
                    for arg in arguments:
                        t = self.type_resolver.resolve_expression_type(arg, input_schema)
                        if t == "unknown":
                            nested_content = nested_content or self._expr_to_string(arg)
                        input_types.append(t)
                elif expr_type == "BINARY" and expr.get("operator", "").lower() == func_name.lower():
                    left_expr = expr.get("left", {})
                    right_expr = expr.get("right", {})
                    left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
                    right_type = self.type_resolver.resolve_expression_type(right_expr, input_schema)
                    if left_type == "unknown":
                        nested_content = nested_content or self._expr_to_string(left_expr)
                    if right_type == "unknown":
                        nested_content = nested_content or self._expr_to_string(right_expr)
                    input_types.append(left_type)
                    input_types.append(right_type)

        return input_types, nested_content

    def _resolve_func_types_from_text(self, func_name, description, input_schema, existing_nested):
        input_types = []
        nested_content = existing_nested

        if self._is_operator_func(func_name):
            input_types, nested_content = self._resolve_operator_param_types_from_text(
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
                    args = self._split_function_args(args_str)
                    for arg in args:
                        arg = arg.strip()
                        if not arg:
                            continue
                        t = self.type_resolver.resolve_expression_type(arg, input_schema)
                        if t == "unknown":
                            nested_content = nested_content or arg
                        input_types.append(t)

        if not nested_content:
            full_expr = self._extract_func_expression(func_name, description)
            if full_expr and full_expr != func_name:
                nested_content = full_expr

        return input_types, nested_content

    def _resolve_operator_param_types_from_text(self, func_name, description, input_schema, existing_nested):
        input_types = []
        nested_content = existing_nested

        left_expr, right_expr = self._extract_operator_operands_from_desc(func_name, description)
        if left_expr is not None and right_expr is not None:
            left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
            right_type = self.type_resolver.resolve_expression_type(right_expr, input_schema)
            if left_type == "unknown":
                nested_content = nested_content or left_expr
            if right_type == "unknown":
                nested_content = nested_content or right_expr
            input_types.append(left_type)
            input_types.append(right_type)

        return input_types, nested_content

    def _resolve_case_param_types_from_text(self, args_str, input_schema, existing_nested):
        input_types = []
        nested_content = existing_nested

        args = FlinkTypeResolver._split_function_args(args_str)
        i = 0
        while i < len(args):
            if i + 1 < len(args):
                condition_arg = args[i].strip()
                condition_arg = self._strip_outer_parens(condition_arg)
                t = self.type_resolver.resolve_expression_type(condition_arg, input_schema)
                if t == "unknown":
                    nested_content = nested_content or condition_arg
                input_types.append(t)
                i += 2
            else:
                break

        return input_types, nested_content

    def _resolve_cast_param_types_from_text(self, args_str, input_schema, existing_nested):
        input_types = []
        nested_content = existing_nested

        original_expr, target_type_str = FlinkTypeResolver._split_alias_from_expr(args_str)
        if original_expr == args_str and " AS " in args_str.upper():
            parts = re.split(r'\s+AS\s+', args_str, maxsplit=1, flags=re.I)
            if len(parts) == 2:
                original_expr = parts[0].strip()
                target_type_str = parts[1].strip()

        t = self.type_resolver.resolve_expression_type(original_expr, input_schema)
        if t == "unknown":
            nested_content = nested_content or original_expr
        input_types.append(t)

        return input_types, nested_content

    def _load_op_dictionary(self):
        try:
            with open(self.dictionary_path, "r", encoding="utf-8") as f:
                self.op_dictionary = json.load(f)
            logger.info(f"Loaded op dictionary from {self.dictionary_path}, total {len(self.op_dictionary)} entries")
        except FileNotFoundError:
            raise FileNotFoundError(f"Op dictionary file not found: {self.dictionary_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in dictionary file: {self.dictionary_path}")
        except Exception as e:
            raise Exception(f"Unexpected error while loading dictionary file: {self.dictionary_path}, error: {e}")

    def is_operator_supported(self, operator_type):
        op_info = self.op_dictionary.get(operator_type, {})
        return op_info.get("is_supported", False)

    def filter_operators_by_whitelist(self, operators_metrics):
        filtered = {}
        for op_type, op_list in operators_metrics.items():
            if self.is_operator_supported(op_type):
                logger.debug(f"Operator {op_type} is supported, keeping")
            else:
                filtered[op_type] = op_list
                logger.debug(f"Operator {op_type} is not supported, filtering out")
        return filtered

    def parse_job_data(self, json_data):
        result = []
        logger.info(f"Starting to parse job data, total {len(json_data)} jobs")
        for job_id, job_info in json_data.items():
            self._process_job(job_id, job_info, result)
        logger.info(f"Finished parsing job data, total {len(result)} records")
        return result

    def _collect_valid_operators(self, operators_metrics):
        ops = []
        for op_type, op_list in operators_metrics.items():
            num_in, num_in_sec, num_out, num_out_sec = FlinkParser.aggregate_metrics(op_list)
            run_time = FlinkParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec)
            ops.append({
                "op_type": op_type,
                "is_supported": self.is_operator_supported(op_type),
                "num_in": num_in,
                "num_out": num_out,
                "run_time": run_time,
                "count": len(op_list),
                "input_types_str": "",
                "output_types_str": "",
            })
        return ops

    def _build_schema_chain_for_vertex(self, description_data, upstream_context=None):
        schema_chain = []
        upstream_context = upstream_context or {}
        upstream_alias_map = upstream_context.get("alias_map", {})
        upstream_output_schema = upstream_context.get("output_schema")

        current_input = upstream_output_schema
        accumulated_alias_map = dict(upstream_alias_map)
        if accumulated_alias_map:
            self.type_resolver.update_alias_map(accumulated_alias_map)

        for desc_item in description_data:
            op_type = None
            if isinstance(desc_item, str):
                op_match = re.match(r'\[(\d+)\]:([A-Za-z]+)', desc_item)
                if op_match:
                    op_type = op_match.group(2)

            if op_type in self.SOURCE_OP_TYPES or (not schema_chain and op_type is None and not current_input):
                tables, output_schema = self.type_resolver.extract_table_source_info(description_data)
                if output_schema:
                    schema_chain.append({
                        "op_type": op_type or "Source",
                        "input_schema": [],
                        "output_schema": output_schema,
                        "tables": tables,
                        "alias_map": dict(accumulated_alias_map),
                    })
                    for table_name in tables:
                        table_cols = self.type_resolver.table_schema.get(table_name, [])
                        if table_cols:
                            col_type, table_col_type = TableSchemaReader.build_column_type_mapping(
                                self.type_resolver.table_schema, {table_name}
                            )
                            self.type_resolver.update_column_type(col_type, table_col_type)
                    current_input = output_schema
                continue

            if op_type and current_input is not None:
                output_schema = self.type_resolver.build_output_schema(
                    op_type, description_data, current_input
                )
                op_alias_map = self.type_resolver.extract_alias_map_from_description(description_data)
                accumulated_alias_map.update(op_alias_map)
                self.type_resolver.update_alias_map(accumulated_alias_map)
                schema_chain.append({
                    "op_type": op_type,
                    "input_schema": current_input,
                    "output_schema": output_schema,
                    "alias_map": dict(accumulated_alias_map),
                })
                current_input = output_schema

        if not schema_chain and current_input:
            schema_chain.append({
                "op_type": "Upstream",
                "input_schema": [],
                "output_schema": current_input,
                "alias_map": dict(accumulated_alias_map),
            })

        if not schema_chain and description_data:
            tables, output_schema = self.type_resolver.extract_table_source_info(description_data)
            if output_schema:
                schema_chain.append({
                    "op_type": "Source",
                    "input_schema": [],
                    "output_schema": output_schema,
                    "tables": tables,
                    "alias_map": dict(accumulated_alias_map),
                })

        return schema_chain

    def _get_input_schema_for_op(self, op_type, schema_chain):
        if not schema_chain:
            return None

        for i, entry in enumerate(schema_chain):
            if entry.get("op_type") == op_type:
                return entry.get("input_schema")

        if schema_chain:
            last = schema_chain[-1]
            if op_type in self.PASS_THROUGH_OP_TYPES:
                return last.get("output_schema")
            return last.get("output_schema")

        return None

    def _process_job(self, job_id, job_info, result):
        vertices = job_info.get("vertices")
        if not vertices:
            logger.warning(f"No vertices found for job {job_id}")
            return
        logger.debug(f"Processing job {job_id}, total {len(vertices)} vertices")

        sorted_ids = self._topological_sort(vertices)

        vertex_context = {}
        task_data = {}
        for task_id in sorted_ids:
            task_info = vertices.get(task_id)
            if not task_info:
                continue

            upstream_ids = task_info.get("upstream_ids", [])
            upstream_context = self._merge_upstream_context(upstream_ids, vertex_context)

            data = self._collect_task_data(task_id, task_info, upstream_context)
            if not data:
                continue

            vertex_context[task_id] = {
                "output_schema": data.get("output_schema"),
                "alias_map": data.get("alias_map", {}),
            }

            if task_id not in task_data:
                task_data[task_id] = {"jobid": job_id, "taskid": task_id, "status": data["status"],
                                      "ops": [], "func_list": []}

            task_data[task_id]["ops"].extend(data["ops"])
            task_data[task_id]["func_list"].extend(data["func_list"])
        for data in task_data.values():
            result.extend(self._build_rows_for_task(data))

    def _collect_task_data(self, task_id, task_info, upstream_context=None):
        status = task_info.get("status", "UNKNOWN")
        operators_metrics = task_info.get("summary_metrics", {}).get("operators", {})
        logic_metadata = task_info.get("logic_metadata", {})
        full_description = logic_metadata.get("full_description", "")
        description_data = task_info.get("description_data", [])

        upstream_context = upstream_context or {}
        schema_chain = self._build_schema_chain_for_vertex(description_data,
                                                           upstream_context) if description_data else []

        ops = self._build_ops_from_schema_chain(schema_chain, operators_metrics)

        func_input_schema = None
        final_alias_map = {}
        if schema_chain:
            last = schema_chain[-1]
            func_input_schema = last.get("output_schema")
            final_alias_map = last.get("alias_map", {})

        func_list = [
            {
                "func_name": f.get("func_name"),
                "times": f.get("times", 0),
                "input": f.get("input", []),
                "nested_content": f.get("nested_content", ""),
            }
            for f in self._analyze_operators_functions(full_description, task_id, func_input_schema)
            if f.get("func_name")
        ]

        result = {"status": status, "ops": ops, "func_list": func_list}
        if schema_chain:
            result["output_schema"] = schema_chain[-1].get("output_schema")
            result["alias_map"] = final_alias_map
        return result if ops or func_list else None

    def _build_ops_from_schema_chain(self, schema_chain, operators_metrics):
        if not schema_chain:
            ops = self._collect_valid_operators(operators_metrics)
            return self._aggregate_ops_by_name_and_types(ops)

        metrics_by_type = {}
        for op_type, op_list in operators_metrics.items():
            num_in, num_in_sec, num_out, num_out_sec = FlinkParser.aggregate_metrics(op_list)
            metrics_by_type[op_type] = {
                "num_in": num_in,
                "num_out": num_out,
                "run_time": FlinkParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec),
                "count": len(op_list),
            }

        type_occurrence = {}
        ops = []
        for chain_entry in schema_chain:
            op_type = chain_entry.get("op_type", "")
            if op_type == "Upstream":
                continue

            input_schema = chain_entry.get("input_schema", [])
            output_schema = chain_entry.get("output_schema", [])

            input_types = [f.get("field_type", "unknown") for f in output_schema] if output_schema else []
            input_str = ",".join(t for t in input_types if t)
            output_str = ""

            metrics = metrics_by_type.get(op_type, {})
            type_occurrence[op_type] = type_occurrence.get(op_type, 0) + 1
            total_of_type = sum(1 for e in schema_chain if e.get("op_type") == op_type)
            count = 1 if total_of_type > 1 else metrics.get("count", 1)

            ops.append({
                "op_type": op_type,
                "is_supported": self.is_operator_supported(op_type),
                "num_in": metrics.get("num_in", 0),
                "num_out": metrics.get("num_out", 0),
                "run_time": metrics.get("run_time", 0.0),
                "count": count,
                "input_types_str": input_str,
                "output_types_str": output_str,
            })

        for op_type, metrics in metrics_by_type.items():
            if not any(e.get("op_type") == op_type for e in schema_chain):
                ops.append({
                    "op_type": op_type,
                    "is_supported": self.is_operator_supported(op_type),
                    "num_in": metrics["num_in"],
                    "num_out": metrics["num_out"],
                    "run_time": metrics["run_time"],
                    "count": metrics["count"],
                    "input_types_str": "",
                    "output_types_str": "",
                })

        return self._aggregate_ops_by_name_and_types(ops)

    def _build_rows_for_task(self, data):
        job_id, task_id, status = data["jobid"], data["taskid"], data["status"]
        ops, func_list = data["ops"], data["func_list"]
        if not ops and not func_list:
            return []
        rows, max_len = [], max(len(ops), len(func_list))
        for i in range(max_len):
            row = self._create_empty_row(job_id, task_id, status)
            if i < len(ops):
                op = ops[i]
                row.update(self._create_row(job_id, task_id, status, op=op))
            if i < len(func_list):
                func = func_list[i]
                row.update({
                    ExcelColumns.FUNC_NAME: func["func_name"],
                    ExcelColumns.FUNC_INPUT: ",".join(func["input"]) if func["input"] else "",
                    ExcelColumns.NESTED_CONTENT: func.get("nested_content", ""),
                    ExcelColumns.FUNC_FREQUENCY: str(func["times"]),
                })
            rows.append(row)
        return rows
