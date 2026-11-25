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
Flink 算子解析模块 - 主入口

核心职责：
1. 加载和维护算子字典
2. 作为各子模块的协调中心
3. 提供作业解析的统一入口

依赖模块：
- OperatorMetricsParser: 算子指标解析
- FunctionAnalyzer: 函数分析
- SchemaChainBuilder: Schema链构建
- ExcelRowBuilder: Excel行生成
"""

import json
import os
import re
import html
from json import JSONDecodeError

from omnihelper.flink.schema.type_resolver import FlinkTypeResolver
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger
from omnihelper.constants.flink_constants import ExcelColumns

from .metrics_parser import OperatorMetricsParser
from .function_analyzer import FunctionAnalyzer
from .schema_chain_builder import SchemaChainBuilder
from .excel_row_builder import ExcelRowBuilder


class FlinkParser:
    """
    Flink 物理计划解析器 - 核心类

    协调各子模块完成作业解析：
    1. 加载算子字典
    2. 解析作业计划描述
    3. 构建 schema 链
    4. 分析函数
    5. 生成 Excel 行数据
    """

    EXCEL_COLUMNS = [
        ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
        ExcelColumns.OPERATOR_NAME, ExcelColumns.IS_SUPPORTED, ExcelColumns.INPUT,
        ExcelColumns.OUTPUT, ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME,
        ExcelColumns.INPUT_DATA_SIZE, ExcelColumns.OUTPUT_DATA_SIZE,
        ExcelColumns.FUNC_NAME, ExcelColumns.FUNC_INPUT,
        ExcelColumns.NESTED_CONTENT, ExcelColumns.FUNC_FREQUENCY
    ]

    def __init__(self, table_schema=None, column_type=None, table_column_type=None):
        """
        初始化解析器

        :param table_schema: dict，表结构字典
        :param column_type: dict，字段名→类型 的全局映射
        :param table_column_type: dict，表名.字段名→类型 的映射
        """
        # 创建类型解析器
        self.type_resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)

        # 加载算子字典
        self.op_dictionary = {}
        self.dictionary_path = os.path.join(self.get_resource_path(), "flink_op_dictionary.json")
        self._load_op_dictionary()
        self.supported_operators = set(self.op_dictionary.keys())

        # 初始化子模块
        self.metrics_parser = OperatorMetricsParser()
        self.function_analyzer = FunctionAnalyzer(self.type_resolver)
        self.schema_builder = SchemaChainBuilder(self.type_resolver)

        # 缓存
        self.schema_chain = {}

    @staticmethod
    def get_resource_path():
        """获取资源文件目录路径"""
        return os.path.join(CommonUtil.get_execute_path(), "resources")

    def _load_op_dictionary(self):
        """加载算子字典配置"""
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
        """判断算子是否支持"""
        op_info = self.op_dictionary.get(operator_type, {})
        return op_info.get("is_supported", False)

    def filter_operators_by_whitelist(self, operators_metrics):
        """按白名单过滤算子"""
        filtered = {}
        for op_type, op_list in operators_metrics.items():
            if self.is_operator_supported(op_type):
                logger.debug(f"Operator {op_type} is supported, keeping")
            else:
                filtered[op_type] = op_list
                logger.debug(f"Operator {op_type} is not supported, filtering out")
        return filtered

    def set_table_schema(self, table_schema, column_type, table_column_type):
        """更新表 schema 配置"""
        self.type_resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)
        self.function_analyzer = FunctionAnalyzer(self.type_resolver)
        self.schema_builder = SchemaChainBuilder(self.type_resolver)

    @staticmethod
    def parse_single_description_line(line):
        """解析单行描述内容"""
        if not line:
            return None
        clean_part = line.strip(" :+- \t")
        if not clean_part:
            return None

        if (clean_part.startswith('{') and clean_part.endswith('}')) or \
                (clean_part.startswith('[') and clean_part.endswith(']')):
            try:
                return json.loads(clean_part)
            except (JSONDecodeError, TypeError) as e:
                logger.debug(f"Failed to parse JSON: {e}")
        return clean_part

    @staticmethod
    def get_description(job_detail, job_id):
        """从作业详情中提取节点描述信息"""
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
        """过滤可用的指标 ID"""
        if not available:
            return []
        return [m['id'] for m in available if any(m['id'].endswith(s) for s in target_metrics)]

    def parse_performance_stats(self, vid, metrics_raw, jobs=None):
        """解析性能统计数据，委托给 metrics_parser"""
        return self.metrics_parser.parse_performance_stats(vid, metrics_raw, jobs)

    @staticmethod
    def _topological_sort(vertices):
        """对顶点进行拓扑排序（Kahn 算法）"""
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

    def _collect_valid_operators(self, operators_metrics):
        """收集有效算子信息"""
        ops = []
        for op_type, op_list in operators_metrics.items():
            num_in, num_in_sec, num_out, num_out_sec = self.metrics_parser.aggregate_metrics(op_list)
            run_time = self.metrics_parser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec)
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

    @staticmethod
    def _aggregate_ops_by_name_and_types(ops):
        """按算子类型聚合，合并相同算子的不同输入类型"""
        aggregated = {}
        for op in ops:
            key = op["op_type"]
            if key in aggregated:
                aggregated[key]["count"] += op["count"]
                aggregated[key]["num_in"] += op["num_in"]
                aggregated[key]["num_out"] += op["num_out"]
                aggregated[key]["run_time"] += op.get("run_time", 0)
                # 合并不同的输入类型（精确匹配，避免子串误过滤）
                existing_types = [t.strip() for t in aggregated[key]["input_types_str"].split(";") if t.strip()]
                if op["input_types_str"] and op["input_types_str"] not in existing_types:
                    if aggregated[key]["input_types_str"]:
                        aggregated[key]["input_types_str"] = f"{aggregated[key]['input_types_str']}; {op['input_types_str']}"
                    else:
                        aggregated[key]["input_types_str"] = op["input_types_str"]
            else:
                aggregated[key] = dict(op)
        return list(aggregated.values())

    def _build_ops_from_schema_chain(self, schema_chain, operators_metrics, description_data=None):
        """从 schema chain 构建 ops 列表"""
        if not schema_chain:
            ops = self._collect_valid_operators(operators_metrics)
            return self._aggregate_ops_by_name_and_types(ops)

        metrics_by_type = self.metrics_parser.aggregate_metrics_by_type(operators_metrics)
        ops = []

        for entry in schema_chain:
            op_type = entry.get("op_type", "")
            if op_type == "Upstream":
                continue

            input_types_str = self.schema_builder.extract_input_types_from_schema_entry(entry, description_data)
            metrics = metrics_by_type.get(op_type, {})
            total_of_type = sum(1 for e in schema_chain if e.get("op_type") == op_type)
            count = 1 if total_of_type > 1 else metrics.get("count", 1)

            ops.append(self._build_op_entry(op_type, input_types_str, metrics, count))

        self._append_missing_metrics_ops(ops, metrics_by_type, schema_chain, description_data)
        return self._aggregate_ops_by_name_and_types(ops)

    def _build_op_entry(self, op_type, input_types_str, metrics, count):
        """构建单条 op dict"""
        return {
            "op_type": op_type,
            "is_supported": self.is_operator_supported(op_type),
            "num_in": metrics.get("num_in", 0),
            "num_out": metrics.get("num_out", 0),
            "run_time": metrics.get("run_time", 0.0),
            "count": count,
            "input_types_str": input_types_str,
            "output_types_str": "",
        }

    def _append_missing_metrics_ops(self, ops, metrics_by_type, schema_chain, description_data):
        """补充 metrics 中存在但 schema_chain 没有的 op"""
        for op_type, metrics in metrics_by_type.items():
            if any(e.get("op_type") == op_type for e in schema_chain):
                continue

            input_str = ""
            if not self.is_operator_supported(op_type) and description_data:
                input_str = self.schema_builder._extract_types_from_description(op_type, description_data, None)

            ops.append(self._build_op_entry(op_type, input_str, metrics, metrics["count"]))

    def _collect_task_data(self, task_id, task_info, upstream_context=None):
        """收集任务数据"""
        status = task_info.get("status", "UNKNOWN")
        operators_metrics = task_info.get("summary_metrics", {}).get("operators", {})
        logic_metadata = task_info.get("logic_metadata", {})
        full_description = logic_metadata.get("full_description", "")
        description_data = task_info.get("description_data", [])

        upstream_context = upstream_context or {}
        schema_chain = self.schema_builder.build_schema_chain(description_data, upstream_context) if description_data else []
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
            for f in self.function_analyzer.analyze_operators_functions(full_description, task_id, func_input_schema)
            if f.get("func_name")
        ]

        result = {"status": status, "ops": ops, "func_list": func_list}
        if schema_chain:
            result["output_schema"] = schema_chain[-1].get("output_schema")
            result["alias_map"] = final_alias_map
        return result

    def _process_job(self, job_id, job_info, result):
        """处理单个作业"""
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
            upstream_context = SchemaChainBuilder.merge_upstream_context(upstream_ids, vertex_context)

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
            result.extend(ExcelRowBuilder.build_rows_for_task(data))

    def parse_job_data(self, json_data):
        """解析作业数据"""
        result = []
        logger.info(f"Starting to parse job data, total {len(json_data)} jobs")
        for job_id, job_info in json_data.items():
            self._process_job(job_id, job_info, result)
        logger.info(f"Finished parsing job data, total {len(result)} records")
        return result
