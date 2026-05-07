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
import time
import requests
import json
import re
import os
from collections import defaultdict
from json import JSONDecodeError

from urllib.parse import urljoin
import logging
from datetime import datetime

import pandas as pd
from omnihelper.parser.function.function_builder import FunctionBuilder
from omnihelper.util.flink_excel_util import FlinkExcelWriterWithStyle

LOG_FILE = "parse_flink.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FlinkFunctionParser:
    """
    Flink 物理计划函数解析器
    负责从 Flink JobManager REST API 获取的物理计划中解析函数调用和表达式

    输出格式与 flink_log_parser.py 完全兼容：
    - 表达式/内置函数名称
    - 表达式Input
    - 嵌套内容
    - 表达式出现频次
    """

    def __init__(self):
        self.function_list = []
        self.omni_functions = []
        self.udf_list = []
        self.user_defined_functions = []
        self.all_funcs = []
        self.func_pattern = None
        self.function_builder = None
        self._load_func_list()

    @staticmethod
    def _get_resource_path():
        """获取资源文件路径"""
        current_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_path, "resources")

    @staticmethod
    def _load_json_file(file_path):
        """加载JSON文件的通用方法"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load {file_path}: {e}")
            return None

    @staticmethod
    def _find_config_file(base_paths, filename):
        """查找文件配置"""
        for base_path in base_paths:
            file_path = os.path.join(base_path, filename)
            if os.path.exists(file_path):
                return file_path
        return None

    def _load_func_list(self):
        """加载函数字典配置"""
        resource_path = self._get_resource_path()
        # 尝试多个可能的路径
        base_paths = [
            resource_path,
            os.path.join(os.path.dirname(resource_path), "resources"),
            os.path.join(os.path.dirname(os.path.dirname(resource_path)), "resources"),
        ]

        dictionary_path = self._find_config_file(base_paths, "flink_function_dictionary.json")
        if dictionary_path:
            self.function_list = self._load_json_file(dictionary_path) or []
            logger.info(f"Loaded {len(self.function_list)} functions from {dictionary_path}")
        else:
            logger.warning(f"Flink function dictionary not found in any of: {base_paths}")
            self.function_list = []
        # 尝试加载 UDF 字典
        udf_possible_paths = self._find_config_file(base_paths, "udf_dictionary.json")
        if udf_possible_paths:
            self.udf_list = self._load_json_file(udf_possible_paths) or []
            logger.info(f"Loaded {len(self.udf_list)} UDFs from {udf_possible_paths}")
        else:
            logger.warning(f"UDF dictionary not found in any of: {udf_possible_paths}")
            self.udf_list = []

        self.omni_functions = [func.get("func_name", "").lower() for func in self.function_list if
                               func.get("func_name")]
        self.user_defined_functions = [func.get("func_name", "").lower() for func in self.udf_list if
                                       func.get("func_name")]
        self.all_funcs = self.omni_functions + self.user_defined_functions

        if self.all_funcs:
            self.func_pattern = re.compile(r"({})\s*\(".format("|".join(map(re.escape, self.all_funcs))), re.I)
            logger.info(f"Created function pattern for {len(self.all_funcs)} functions")
        else:
            logger.warning("No functions loaded, func_pattern is None")

        self.function_builder = FunctionBuilder(self.func_pattern, self.all_funcs)

    def parse_plan_description(self, description):
        """
        解析 Flink 节点描述中的函数调用
        :param description: 节点描述字符串
        :return: 函数调用列表
        """
        if not description:
            return []
        return self.function_builder.search_func_expr_pairs(description)

    def extract_expressions_from_plan(self, plan):
        """
        从完整的物理计划中提取所有表达式
        :param plan: Flink 作业的 plan 节点（字典格式）
        :return: 函数调用列表
        """
        expressions = []

        if not isinstance(plan, dict):
            return expressions

        nodes = plan.get("nodes", [])
        for node in nodes:
            description = node.get("description", "")
            if description:
                funcs = self.parse_plan_description(description)
                for func in funcs:
                    expressions.append({
                        "node_id": node.get("id"),
                        "node_name": node.get("name"),
                        "description": description,
                        **func
                    })

        return expressions

    def analyze_physical_plan(self, plan):
        """
        分析物理计划（与 flink_log_parser.py 完全兼容）
        :param plan: 物理计划（字符串或字典格式）
        :return: 函数分析结果列表
        """
        if isinstance(plan, str):
            funcs = self.parse_plan_description(plan)
        elif isinstance(plan, dict):
            expressions = self.extract_expressions_from_plan(plan)
            funcs = []
            for expr in expressions:
                func_info = {k: v for k, v in expr.items() if k in ['func', 'params', 'input', 'expr']}
                funcs.append(func_info)
        else:
            return []

        func_counter = defaultdict(int)
        input_dict = defaultdict(set)
        for func in funcs:
            func_name = func.get("func", "").lower()
            if func_name:
                func_counter[func_name] += 1
                inputs = func.get("params", []) or func.get("input", [])
                if inputs:
                    input_dict[func_name].update(inputs)

        results = []
        for func_name, count in func_counter.items():
            results.append({
                "表达式/内置函数名称": func_name,
                "表达式Input": list(input_dict[func_name]),
                "嵌套内容": [],
                "表达式出现频次": count
            })

        return results

    def _check_function_support(self, func_name):
        """
        检查函数是否支持 Omni
        :param func_name: 函数名
        :return: True 表示支持，False 表示不支持
        """
        if func_name.lower() in self.user_defined_functions:
            return False

        for func in self.function_list:
            if func.get("func_name", "").lower() == func_name.lower():
                return func.get("is_support_func", False)

        return False

    def analyze_job_functions(self, job_detail):
        """
        分析整个作业的函数使用情况（用于 FlinkMonitor）
        :param job_detail: Flink 作业详情（从 REST API 获取的字典）
        :return: 函数分析结果，包含 job_id, job_name, func_name, count, is_udf, is_supported
        """
        if not job_detail or not isinstance(job_detail, dict):
            return []

        result = []
        plan = job_detail.get("plan", {})
        job_name = job_detail.get("name", "Unknown")
        job_id = job_detail.get("jid", "")

        expressions = self.extract_expressions_from_plan(plan)

        func_counter = defaultdict(int)
        for expr in expressions:
            func_name = expr.get("func", "").lower()
            if func_name:
                func_counter[func_name] += 1

        for func_name, count in func_counter.items():
            is_udf = func_name in self.user_defined_functions
            result.append({
                "job_id": job_id,
                "job_name": job_name,
                "func_name": func_name,
                "count": count,
                "is_udf": is_udf,
                "is_supported": self._check_function_support(func_name)
            })

        return result

    def parse_operator_chain(self, operator_chain_str):
        """
        解析运算符链字符串
        :param operator_chain_str: 运算符链字符串，如 "Map -> Calc -> Sink"
        :return: 运算符列表
        """
        if not operator_chain_str:
            return []

        operators = []
        parts = operator_chain_str.split(' -> ')
        for part in parts:
            part = part.strip()
            if not part:
                continue
            match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\((.*)\))?', part)
            if not match:
                continue
            op_name = match.group(1)
            op_params = match.group(2) if match.group(2) else ""

            funcs = []
            if op_params:
                func_pairs = self.function_builder.search_func_expr_pairs(op_params)
                funcs = [f.get("func") for f in func_pairs if f.get("func")]

            operators.append({
                "operator_name": op_name,
                "parameters": op_params,
                "functions": funcs
            })

        return operators

    def generate_function_report(self, job_details):
        """
        生成函数使用报告
        :param job_details: 作业详情列表
        :return: 汇总报告
        """
        report = {
            "total_jobs": 0,
            "functions_used": [],
            "udf_count": 0,
            "supported_func_count": 0,
            "unsupported_func_count": 0
        }

        all_functions = []
        seen_funcs = set()

        for job_detail in job_details:
            if not job_detail:
                continue

            report["total_jobs"] += 1
            job_funcs = self.analyze_job_functions(job_detail)

            for func_info in job_funcs:
                func_key = (func_info["job_id"], func_info["func_name"])
                if func_key not in seen_funcs:
                    seen_funcs.add(func_key)
                    all_functions.append(func_info)

                    if func_info["is_udf"]:
                        report["udf_count"] += 1
                    if func_info["is_supported"]:
                        report["supported_func_count"] += 1
                    else:
                        report["unsupported_func_count"] += 1

        report["functions_used"] = all_functions
        return report

    def extract_column_usage(self, description):
        """
        从描述中提取列使用信息
        :param description: 节点描述
        :return: 列名列表
        """
        if not description:
            return []

        columns = []

        patterns = [
            re.compile(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'),
            re.compile(r'`([^`]+)`'),
            re.compile(r'\[([^\]]+)\]')
        ]

        for pattern in patterns:
            for match in pattern.finditer(description):
                col = match.group(1)
                if col.lower() not in ['select', 'from', 'where', 'and', 'or', 'as', 'in', 'is', 'not', 'null']:
                    if col.lower() not in self.all_funcs:
                        columns.append(col)

        return list(set(columns))


class FlinkRequester:
    """
    Requester Layer: Network Communication
    """

    def __init__(self, url, timeout=5, ssl_verify=True, interval=100, max_retries=3):
        self.base_url = url
        self.session = requests.Session()
        self.timeout = int(timeout) if timeout else 5
        self.max_retries = max_retries
        self.ssl_verify = ssl_verify
        self.interval = interval

    def _get_json(self, endpoint, params=None):
        url = urljoin(self.base_url, endpoint.lstrip('/'))
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.interval / 1000)
                resp = self.session.get(url, params=params, timeout=self.timeout, verify=self.ssl_verify)
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"[API Error] {endpoint} Status: {resp.status_code}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying {endpoint} (attempt {attempt + 1})")
            except requests.exceptions.RequestException as e:
                logger.error(f"[Network Error] {endpoint} Failed: {e}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying {endpoint} (attempt {attempt + 1})")
                continue
            except JSONDecodeError as e:
                logger.error(f"[JSON Decode Error] {endpoint} Failed: {e}")
                break
            except Exception as e:
                logger.error(f"[UnException Error] {endpoint} Failed: {e}")
                break
        return None

    def get_jobs_overview(self):
        """获取作业概览"""
        return self._get_json("jobs/overview")

    def get_job_detail(self, jid):
        """获取作业详情"""
        return self._get_json(f"jobs/{jid}")

    def get_vertex_metrics(self, jid, vid, metric_ids=None):
        """
        :param jid: 作业ID
        :param vid: 任务ID
        :param metric_ids: 算子指标ID列表
        :return: 算子指标列表
        """
        if isinstance(metric_ids, list):
            params = {"get": ",".join(metric_ids)} if metric_ids else {}
        else:
            params = {"get": metric_ids} if metric_ids else {}
        return self._get_json(f"jobs/{jid}/vertices/{vid}/metrics", params=params)


class FlinkParser:
    """
    Parser Layer: Logic & Data Transformation
    """
    FUNC_PATTERN = re.compile(r'([A-Z_]{2,}|>=|<=|<>|!=|>|<|=)')

    def __init__(self):
        self.function_parser = FlinkFunctionParser()
        self.op_dictionary = {}
        self.resource_path = self._get_resource_path()
        self.dictionary_path = os.path.join(self.resource_path, "flink_op_dictionary.json")
        self._load_op_dictionary()

    @staticmethod
    def _get_resource_path():
        """获取资源文件路径"""
        current_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_path, "resources")

    @staticmethod
    def _parse_single_description_line(line):
        """处理单行描述的清理与解析"""
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
                pass
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
                if (parsed_line := FlinkParser._parse_single_description_line(line)) is not None
            ]

            vertex_map[vertex_id] = {"plan_desc": description_data}

        return {job_id: vertex_map}

    @staticmethod
    def filter_num_data(available, target_metrics):
        if not available:
            return []
        needed_ids = [m['id'] for m in available if
                      any(m['id'].endswith(s) for s in target_metrics)] if available else []
        return needed_ids

    @staticmethod
    def extract_expressions(description):
        """Extracts SQL functions and operators from operator description."""
        if not description:
            return []
        matches = FlinkParser.FUNC_PATTERN.findall(description)
        exclude_set = {'AS', 'AND', 'OR', 'IS', 'NOT', 'NULL', 'TRUE', 'FALSE'}
        return sorted(list(set(m for m in matches if m not in exclude_set)))

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
        """把原始 metrics 按算子分组"""
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
        """计算每个算子的活跃时长"""
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
        """汇总整体"""
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
        """把operator_analysis 的结果op_type 重组"""
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
        """聚合同类算子的指标"""
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
    def collect_expressions(op_list, operator_chain):
        """收集表达式信息"""
        all_expr, total_count = [], 0
        for op_info in op_list:
            op_id = str(op_info.get("op_id", ""))
            for operator in operator_chain:
                if op_id in operator.get("operator_name", ""):
                    all_expr.extend(operator.get("expressions", []))
                    total_count += operator.get("expression_count", 0)
                    break
        return all_expr, total_count

    @staticmethod
    def bytes_to_mb(value):
        """字节计算"""
        if not value:
            return 0.0
        return round(value / (1024 * 1024), 2)

    @staticmethod
    def parse_performance_stats(vid, metrics_raw, jobs=None):
        """Processes raw metrics into structured inbound/outbound stats."""
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
    def _merge_expression_inputs(func_analysis):
        """合并函数分析中的输入参数"""
        all_inputs = set()
        for func in func_analysis:
            inputs = func.get("表达式Input", [])
            all_inputs.update(inputs)
        return ",".join(sorted(all_inputs)) if all_inputs else "N/A"

    def _load_op_dictionary(self):
        try:
            with open(self.dictionary_path, "r", encoding="utf-8") as f:
                self.op_dictionary = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Op dictionary file not found: {self.dictionary_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in dictionary file: {self.dictionary_path}")
        except Exception as e:
            raise Exception(f"Unexpected error while loading dictionary file: {self.dictionary_path}, error: {e}")

    def parse_job_data(self, json_data):
        """
        解析作业数据，输出格式与 flink_log_parser.py 完全兼容
        """
        result = []
        for job_id, job_info in json_data.items():
            self._process_job(job_id, job_info, result)
        return result

    def _process_job(self, job_id, job_info, result):
        vertices = job_info.get("vertices")
        if not vertices:
            return
        for task_id, task_info in vertices.items():
            self._process_task(job_id, task_id, task_info, result)

    def _process_task(self, job_id, task_id, task_info, result):
        status = task_info.get("status", "UNKNOWN")
        operator_chain = task_info.get("operator_chain", [])
        operators_metrics = task_info.get("summary_metrics", {}).get("operators", {})
        if not operators_metrics:
            return
        for op_type, op_list in operators_metrics.items():

            if op_type in self.op_dictionary.keys():
                continue
            self._process_operator(job_id, task_id, status, op_type, op_list, operator_chain, result)

    def _process_operator(self, job_id, task_id, status, op_type, op_list, operator_chain, result):
        num_in, num_in_sec, num_out, num_out_sec = FlinkParser.aggregate_metrics(op_list)
        run_time = FlinkParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec)

        func_analysis = self._analyze_operators_functions(operator_chain)
        expressions = [f["表达式/内置函数名称"] for f in func_analysis]
        expr_inputs = self._merge_expression_inputs(func_analysis)
        expr_count = sum(f["表达式出现频次"] for f in func_analysis)

        result.append({
            'jobid': job_id,
            'taskid': task_id,
            '状态': status,
            '算子名称': op_type,
            'Input': num_in,
            'Output': num_out,
            '出现频次': len(op_list),
            '运行时间(s)': run_time,
            '输入数据量': f"{FlinkParser.bytes_to_mb(num_in)}MB",
            '输出数据量': f"{FlinkParser.bytes_to_mb(num_out)}MB",
            '表达式/内置函数名称': ",".join(expressions) if expressions else "N/A",
            '表达式Input': expr_inputs,
            '嵌套内容': "N/A",
            '表达式出现频次': expr_count
        })

    def _analyze_operators_functions(self, operator_chain):
        """分析算子链中的所有函数"""
        all_funcs = []
        for op in operator_chain:
            description = op.get("full_description", "")
            if description:
                funcs = self.function_parser.analyze_physical_plan(description)
                all_funcs.extend(funcs)
        return all_funcs

    def parse_operator_chain(self, description, stats):
        """Splits a vertex description into individual operators with metrics."""
        if not description:
            return []

        raw_ops = description.split(' -> ')
        chain = []

        operator_stats = stats.get("operators", {})
        for op_raw in raw_ops:
            base_name = op_raw.split('(')[0].strip()
            exprs = FlinkParser.extract_expressions(op_raw)

            func_analysis = self.function_parser.analyze_physical_plan(op_raw)
            detailed_exprs = [f["表达式/内置函数名称"] for f in func_analysis]
            if detailed_exprs:
                exprs = detailed_exprs

            op_data = operator_stats.get(base_name, {})
            chain.append({
                "operator_name": base_name,
                "records_in_per_sec": op_data.get("numRecordsInPerSecond", 0.0),
                "records_out_per_sec": op_data.get("numRecordsOutPerSecond", 0.0),
                "total_records_in": op_data.get("numRecordsIn", 0),
                "total_records_out": op_data.get("numRecordsOut", 0),
                "bytes_in": op_data.get("numBytesIn", 0),
                "bytes_out": op_data.get("numBytesOut", 0),
                "expression_count": len(exprs),
                "active_duration_in": op_data.get("active_duration_in", 0.0),
                "active_duration_out": op_data.get("active_duration_out", 0.0),
                "expressions": exprs,
                "full_description": op_raw
            })
        return chain


class FlinkMonitor:
    """
    Monitor Layer: Orchestration
    """

    def __init__(self, url, allow_jobs=None, output_dir=None):
        self.req = FlinkRequester(url)
        self.parser = FlinkParser()
        self.allow_jobs = allow_jobs
        self.target_metrics = [
            ".numRecordsIn", ".numRecordsInPerSecond",
            ".numRecordsOut", ".numRecordsOutPerSecond",
            ".numBytesIn", ".numBytesInPerSecond",
            ".numBytesOut", ".numBytesOutPerSecond",
        ]

        # Excel 报告相关配置
        self.output_dir = output_dir
        self.excel_writer = FlinkExcelWriterWithStyle()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 如果没有指定输出目录，默认使用当前目录下的 output 文件夹
        if self.output_dir is None:
            # 使用项目根目录
            current_path = os.path.dirname(os.path.abspath(__file__))
            # 从 flink/parse.py 到项目根目录: OmniAdaptor/omnihelper/flink -> OmniAdaptor
            self.output_dir = os.path.join(current_path, "..", "..", "output")

        # 转换为绝对路径并创建目录
        self.output_dir = os.path.abspath(self.output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

        print(f"[INFO] Excel report will be saved to: {self.output_dir}")

    def fetch_metrics(self, jid, vid, metrics, batch_size=10):
        results = []
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i + batch_size]
            metric_values = self.req.get_vertex_metrics(jid, vid, batch) if batch else []
            if metric_values:
                results.extend(metric_values)
        return results

    def run_full_scan(self, jids=None, generate_excel=True):
        """
        运行完整的扫描分析

        Parameters:
        -----------
        jids : list, optional
            指定要分析的作业ID列表，如果为None则获取所有作业
        generate_excel : bool, optional
            是否生成Excel报告，默认为True

        Returns:
        --------
        list: 解析后的报告数据
        """
        full_report = {}
        jobs_ids = self._get_job_ids(jids)
        for jid in jobs_ids:
            job_data = self._process_job(jid)
            if job_data:
                full_report[jid] = job_data
        final_output = self.parser.parse_job_data(full_report)
        logger.info(f"Generated report with {len(final_output)}")

        # 如果需要生成Excel报告
        if generate_excel and final_output:
            self.generate_excel_report(final_output)

        return final_output

    def generate_excel_report(self, report_data):
        """
        生成Excel报告

        Parameters:
        -----------
        report_data : list
            报告数据列表

        Returns:
        --------
        str: 生成的Excel文件路径，如果失败返回None
        """
        if not report_data:
            logger.warning("No data to generate Excel report")
            return None

        try:
            # 定义列顺序，确保与表头配置一致
            columns = [
                'jobid', 'taskid', '状态',
                '算子名称', 'Input', 'Output', '出现频次', '运行时间(s)', '输入数据量', '输出数据量',
                '表达式/内置函数名称', 'Input', '嵌套内容', '出现频次'
            ]

            # 处理重复列名的情况
            # 为第二个 Input 和 出现频次 列添加临时后缀
            temp_columns = [
                'jobid', 'taskid', '状态',
                '算子名称', 'Input', 'Output', '出现频次', '运行时间(s)', '输入数据量', '输出数据量',
                '表达式/内置函数名称', 'Input_2', '嵌套内容', '出现频次_2'
            ]

            # 创建一个新的列表，将数据中的 '表达式Input' 和 '表达式出现频次' 映射到临时列名
            processed_data = []
            for item in report_data:
                processed_item = {
                    'jobid': item.get('jobid'),
                    'taskid': item.get('taskid'),
                    '状态': item.get('状态'),
                    '算子名称': item.get('算子名称'),
                    'Input': item.get('Input'),
                    'Output': item.get('Output'),
                    '出现频次': item.get('出现频次'),
                    '运行时间(s)': item.get('运行时间(s)'),
                    '输入数据量': item.get('输入数据量'),
                    '输出数据量': item.get('输出数据量'),
                    '表达式/内置函数名称': item.get('表达式/内置函数名称'),
                    'Input_2': item.get('表达式Input'),
                    '嵌套内容': item.get('嵌套内容'),
                    '出现频次_2': item.get('表达式出现频次')
                }
                processed_data.append(processed_item)

            # 创建DataFrame
            df = pd.DataFrame(processed_data, columns=temp_columns)

            # 重命名列，将临时后缀去掉，实现重复列名
            df.columns = columns

            # 生成输出文件路径
            output_excel_path = os.path.join(self.output_dir, f"Omni_Analysis_All_Report_{self.timestamp}.xlsx")

            # 写入Excel
            success = self.excel_writer.write_to_excel(df, output_excel_path)

            if success:
                logger.info(f"Excel report generated: {output_excel_path}")
                return output_excel_path
            else:
                logger.error("Failed to generate Excel report")
                return None

        except Exception as e:
            logger.error(f"Error generating Excel report: {e}")
            return None

    def _get_job_ids(self, jids):
        if jids is not None:
            logger.info(f"Using provided job IDS: {jids}")
            return jids
        overview = self.req.get_jobs_overview()
        if not overview:
            logger.warning("No jobs overview data received")
            return []
        all_jobs = overview.get('jobs', [])
        return [j['jid'] for j in all_jobs if not self.allow_jobs or j.get('name') in self.allow_jobs]

    def _process_job(self, jid):
        detail = self.req.get_job_detail(jid)
        if not detail:
            logger.warning(f"Failed to get detail for job {jid}")
            return None
        plan = detail.get("plan", "")
        if not plan:
            logger.warning(f"Failed to get plan for job {jid}")
            return None
        job_name = detail.get('name', 'Unknown')
        plan_nodes = {node['id']: node for node in plan.get('nodes', [])}
        vertices = {
            vertex["id"]: self._process_vertex(vertex["id"], vertex, plan_nodes, jid, detail)
            for vertex in detail.get("vertices", [])
        }
        return {
            "job_name": job_name,
            "vertices": {k: v for k, v in vertices.items() if v is not None}
        }

    def _process_vertex(self, vid, vertex, plan_nodes, jid, detail):
        metrics = self._get_vertex_metrics(vid, jid)
        if not metrics:
            return None
        stats = self.parser.parse_performance_stats(vid, metrics["values"],
                                                    self.parser.get_description(detail, jid))
        description = plan_nodes.get(vid, {}).get('description', '')
        op_chain = self.parser.parse_operator_chain(description, stats)

        return {
            "status": vertex.get('status'),
            "vertex_name": vertex.get("name"),
            "operator_chain": op_chain,
            "logic_metadata": {
                "full_description": description,
                "detected_functions": self.parser.extract_expressions(description)
            },
            "summary_metrics": stats
        }

    def _get_vertex_metrics(self, vid, jid):
        available = self.req.get_vertex_metrics(jid, vid)
        if not available:
            logger.warning(f"No metrics available for vertex {vid} in job {jid}")
            return None
        needed_ids = self.parser.filter_num_data(available, self.target_metrics)
        return {
            "ids": needed_ids,
            "values": self.fetch_metrics(jid, vid, needed_ids) if needed_ids else []
        }


if __name__ == "__main__":
    CONFIG = {
        "url": "http://100.102.199.225:8081",
        "allow_jobs": None,
    }

    monitor = FlinkMonitor(**CONFIG)
    report = monitor.run_full_scan()
    print(f"Report generated with {len(report)} entries")
