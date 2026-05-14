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
from collections import defaultdict
from json import JSONDecodeError
from omnihelper.flink.function.function_parse import FlinkFunctionParser
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger
from omnihelper.constants.flink_constants import ExcelColumns, TaskStatus


class FlinkParser:
    EXCEL_COLUMNS = [
        ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
        ExcelColumns.OPERATOR_NAME, ExcelColumns.INPUT, ExcelColumns.OUTPUT,
        ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME, ExcelColumns.INPUT_DATA_SIZE,
        ExcelColumns.OUTPUT_DATA_SIZE, ExcelColumns.FUNC_NAME, ExcelColumns.FUNC_INPUT,
        ExcelColumns.NESTED_CONTENT, ExcelColumns.FUNC_FREQUENCY
    ]

    def __init__(self):
        self.function_parser = FlinkFunctionParser()
        self.op_dictionary = {}
        self.dictionary_path = os.path.join(self.get_resource_path(), "flink_op_dictionary.json")
        self._load_op_dictionary()
        self.supported_operators = set(self.op_dictionary.keys())  # 支持的算子白名单

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
            if not description:
                continue
            raw_parts = re.split(r"<br/>|\n", description)
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
            if name not in aggregated:
                aggregated[name] = {"count": 0, "inputs": set()}
            aggregated[name]["count"] += func_info.get("times", 0)
            aggregated[name]["inputs"].update(func_info.get("input", []))
        return list(aggregated.items())

    @staticmethod
    def _create_empty_row(job_id, task_id, status):
        return {
            ExcelColumns.JOB_ID: job_id,
            ExcelColumns.TASK_ID: task_id,
            ExcelColumns.STATUS: status,
            ExcelColumns.OPERATOR_NAME: "",
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
    def _create_row(job_id, task_id, status, op=None, func_name="", func_inputs_str="", func_count=""):
        return {
            ExcelColumns.JOB_ID: job_id,
            ExcelColumns.TASK_ID: task_id,
            ExcelColumns.STATUS: status,
            ExcelColumns.OPERATOR_NAME: op["op_type"],
            ExcelColumns.INPUT: "",
            ExcelColumns.OUTPUT: "",
            ExcelColumns.FREQUENCY: op["count"],
            ExcelColumns.RUNTIME: op["run_time"],
            ExcelColumns.INPUT_DATA_SIZE: f"{FlinkParser.bytes_to_mb(op['num_in'])}MB",
            ExcelColumns.OUTPUT_DATA_SIZE: f"{FlinkParser.bytes_to_mb(op['num_out'])}MB",
            ExcelColumns.FUNC_NAME: func_name,
            ExcelColumns.FUNC_INPUT: func_inputs_str,
            ExcelColumns.NESTED_CONTENT: "",
            ExcelColumns.FUNC_FREQUENCY: func_count
        }

    def _analyze_operators_functions(self, description, task_id):
        if not description:
            return []

        unsupported_funcs = self.function_parser.analyze_unsupported_functions(description)
        return [
            {
                "func_name": func["func_name"],
                "task_id": task_id,
                "input": [""],
                "times": func["times"]
            }
            for func in unsupported_funcs
        ]

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
        """
        检查算子是否在白名单中（支持）
        
        :param operator_type: 算子类型
        :return: True 表示支持，False 表示不支持
        """
        return operator_type in self.supported_operators

    def filter_operators_by_whitelist(self, operators_metrics):
        """
        过滤出支持的算子（在白名单中）
        
        :param operators_metrics: 算子指标字典 {op_type: [op_list]}
        :return: 过滤后的算子指标字典
        """
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
            # 只输出白名单中支持的算子
            if self.is_operator_supported(op_type):
                logger.debug(f"Operator type {op_type} is not in whitelist, filtering out")
                continue

            num_in, num_in_sec, num_out, num_out_sec = FlinkParser.aggregate_metrics(op_list)
            run_time = FlinkParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec)
            ops.append({
                "op_type": op_type,
                "num_in": num_in,
                "num_out": num_out,
                "run_time": run_time,
                "count": len(op_list)
            })
        return ops

    def _process_job(self, job_id, job_info, result):
        vertices = job_info.get("vertices")
        if not vertices:
            logger.warning(f"No vertices found for job {job_id}")
            return
        logger.debug(f"Processing job {job_id}, total {len(vertices)} vertices")
        task_data = {}
        for task_id, task_info in vertices.items():
            data = self._collect_task_data(task_id, task_info)
            if not data:
                continue
            if task_id not in task_data:
                task_data[task_id] = {"jobid": job_id, "taskid": task_id, "status": data["status"], "ops": [],
                                      "func_list": []}

            task_data[task_id]["ops"].extend(data["ops"])
            task_data[task_id]["func_list"].extend(data["func_list"])
        for data in task_data.values():
            result.extend(self._build_rows_for_task(data))

    def _collect_task_data(self, task_id, task_info):
        status = task_info.get("status", "UNKNOWN")
        operators_metrics = task_info.get("summary_metrics", {}).get("operators", {})
        logic_metadata = task_info.get("logic_metadata", {})
        full_description = logic_metadata.get("full_description", "")
        ops = self._collect_valid_operators(operators_metrics)
        func_list = [{"func_name": f.get("func_name"), "times": f.get("times", 0), "input": f.get("input", [])}
                     for f in self._analyze_operators_functions(full_description, task_id) if f.get("func_name")]
        return {"status": status, "ops": ops, "func_list": func_list} if ops or func_list else None

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
                row.update({ExcelColumns.FUNC_NAME: func["func_name"],
                            ExcelColumns.FUNC_INPUT: ",".join(func["input"]) if func["input"] else "",
                            ExcelColumns.FUNC_FREQUENCY: str(func["times"])})
            rows.append(row)
        return rows
