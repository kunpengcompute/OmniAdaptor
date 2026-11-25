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
Flink 算子指标解析模块

负责解析和聚合 Flink 作业的算子性能指标，包括：
- 从原始指标数据中提取算子相关指标
- 计算运行时间、数据量等统计信息
- 按算子类型聚合指标
"""

import re
from collections import defaultdict


class OperatorMetricsParser:
    """
    算子指标解析器

    核心职责：
    1. 解析原始指标数据，按算子分组
    2. 计算算子活跃持续时间
    3. 聚合同类型算子的指标
    4. 计算运行时间和数据量统计
    """

    @staticmethod
    def safe_float(val):
        """安全地将值转换为浮点数"""
        if not val:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def group_metrics_by_operator(raw_map):
        """
        将原始指标按算子分组

        :param raw_map: dict，{完整指标ID: 值}
        :return: dict，{算子ID: {metric_name: 值}}
        """
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
                val_f = OperatorMetricsParser.safe_float(val)
                if "PerSecond" in metric:
                    operator_stats[operator][metric] = round(val_f, 2)
                else:
                    operator_stats[operator][metric] = int(val_f)
        return operator_stats

    @staticmethod
    def calc_active_duration(operator_stats):
        """计算算子的活跃持续时间"""
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
        """计算算子统计的总和汇总"""
        return {
            "totalRecordsIn": sum(stats["numRecordsIn"] for stats in operator_stats.values()),
            "totalRecordsOut": sum(stats["numRecordsOut"] for stats in operator_stats.values()),
            "totalBytesIn": sum(stats["numBytesIn"] for stats in operator_stats.values()),
            "totalBytesOut": sum(stats["numBytesOut"] for stats in operator_stats.values()),
            "avgRecordsInPerSecond": round(sum(stats["numRecordsInPerSecond"] for stats in operator_stats.values())),
            "avgRecordsOutPerSecond": round(sum(stats["numRecordsOutPerSecond"] for stats in operator_stats.values())),
        }

    @staticmethod
    def aggregate_metrics(op_list):
        """聚合同类型算子的指标"""
        num_in = sum(op["metrics"].get("numBytesIn", 0) for op in op_list)
        num_in_sec = sum(op["metrics"].get("numBytesInPerSecond", 0.0) for op in op_list)
        num_out = sum(op["metrics"].get("numBytesOut", 0) for op in op_list)
        num_out_sec = sum(op["metrics"].get("numBytesOutPerSecond", 0.0) for op in op_list)
        return num_in, num_in_sec, num_out, num_out_sec

    @staticmethod
    def compute_runtime(num_in, num_in_sec, num_out, num_out_sec):
        """计算算子预估运行时间"""
        run_time = 0.0
        if num_in_sec > 0:
            run_time += num_in / num_in_sec
        if num_out_sec > 0:
            run_time += num_out / num_out_sec
        return round(run_time, 2)

    @staticmethod
    def bytes_to_mb(value):
        """将字节数转换为 MB"""
        if not value:
            return 0.0
        return round(value / (1024 * 1024), 2)

    @staticmethod
    def parse_performance_stats(vid, metrics_raw, jobs=None):
        """解析性能统计数据"""
        if not metrics_raw or not isinstance(metrics_raw, list):
            return {"operators": {}, "summary": {}, "analysis": {}}

        valid_metrics = [item for item in metrics_raw if isinstance(item, dict) and 'id' in item and 'value' in item]
        if not valid_metrics:
            return {"operators": {}, "summary": {}, "analysis": {}}

        raw_map = {item['id']: item['value'] for item in valid_metrics}
        operator_stats = OperatorMetricsParser.group_metrics_by_operator(raw_map)
        operator_stats = OperatorMetricsParser.calc_active_duration(operator_stats)
        summary = OperatorMetricsParser.calc_summary(operator_stats)

        analysis = {}
        operators_by_type = {}
        if jobs is not None:
            analysis = OperatorMetricsParser.operator_analysis(jobs, {vid: raw_map})
            operators_by_type = OperatorMetricsParser.restructure_by_op_type(analysis)

        return {
            "operators": operators_by_type,
            "summary": summary,
            "analysis": analysis
        }

    @staticmethod
    def operator_analysis(jobs, metrics):
        """分析作业中的算子信息及其关联指标"""
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
    def restructure_by_op_type(analysis):
        """按算子类型重新组织分析结果"""
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
    def aggregate_metrics_by_type(operators_metrics):
        """将 operators_metrics 聚合成 {op_type: {num_in, num_out, run_time, count}}"""
        metrics_by_type = {}
        for op_type, op_list in operators_metrics.items():
            num_in, num_in_sec, num_out, num_out_sec = OperatorMetricsParser.aggregate_metrics(op_list)
            metrics_by_type[op_type] = {
                "num_in": num_in,
                "num_out": num_out,
                "run_time": OperatorMetricsParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec),
                "count": len(op_list),
            }
        return metrics_by_type
