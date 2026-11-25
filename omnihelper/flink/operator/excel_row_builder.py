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
Flink Excel 行生成模块

负责生成符合 Excel 导出格式的数据行，包括：
- 创建空数据行
- 创建包含算子信息的数据行
- 构建任务的 Excel 行列表
"""

from omnihelper.constants.flink_constants import ExcelColumns, TaskStatus
from omnihelper.flink.operator.metrics_parser import OperatorMetricsParser


class ExcelRowBuilder:
    """
    Excel 行构建器

    核心职责：
    1. 创建空的数据行（占位用）
    2. 创建包含算子或函数信息的数据行
    3. 为任务构建完整的 Excel 行列表
    """

    @staticmethod
    def create_empty_row(job_id, task_id, status):
        """
        创建空的数据行

        :param job_id: str，作业 ID
        :param task_id: str，任务 ID
        :param status: str，任务状态
        :return: dict，空行字典
        """
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
    def create_row(job_id, task_id, status, op=None, func_name="", func_inputs_str="",
                   func_nested="", func_count=""):
        """
        创建包含算子或函数信息的数据行

        :param job_id: str，作业 ID
        :param task_id: str，任务 ID
        :param status: str，状态
        :param op: dict，算子信息字典（可选）
        :param func_name: str，函数名
        :param func_inputs_str: str，函数输入类型字符串
        :param func_nested: str，嵌套内容
        :param func_count: str，函数出现次数
        :return: dict，填充了数据的行字典
        """
        is_supported_str = "是" if op.get("is_supported", False) else "否"
        task_status = (status == TaskStatus.SUCCESS)

        return {
            ExcelColumns.JOB_ID: job_id,
            ExcelColumns.TASK_ID: task_id,
            ExcelColumns.STATUS: status,
            ExcelColumns.OPERATOR_NAME: op["op_type"],
            ExcelColumns.IS_SUPPORTED: is_supported_str,
            ExcelColumns.INPUT: op.get("input_types_str", ""),
            ExcelColumns.OUTPUT: op.get("output_types_str", ""),
            ExcelColumns.FREQUENCY: op["count"],
            ExcelColumns.RUNTIME: op["run_time"] if task_status else "",
            ExcelColumns.INPUT_DATA_SIZE: f"{OperatorMetricsParser.bytes_to_mb(op['num_in'])}" if task_status else "",
            ExcelColumns.OUTPUT_DATA_SIZE: f"{OperatorMetricsParser.bytes_to_mb(op['num_out'])}" if task_status else "",
            ExcelColumns.FUNC_NAME: func_name,
            ExcelColumns.FUNC_INPUT: func_inputs_str,
            ExcelColumns.NESTED_CONTENT: func_nested,
            ExcelColumns.FUNC_FREQUENCY: func_count
        }

    @staticmethod
    def build_rows_for_task(data):
        """
        构建任务的 Excel 行数据

        :param data: dict，任务数据
        :return: list，Excel 行列表
        """
        job_id, task_id, status = data["jobid"], data["taskid"], data["status"]
        ops, func_list = data["ops"], data["func_list"]

        if not ops and not func_list:
            if status != "SUCCESS":
                return [ExcelRowBuilder.create_empty_row(job_id, task_id, status)]
            return []

        rows, max_len = [], max(len(ops), len(func_list))

        for i in range(max_len):
            row = ExcelRowBuilder.create_empty_row(job_id, task_id, status)

            if i < len(ops):
                op = ops[i]
                row.update(ExcelRowBuilder.create_row(job_id, task_id, status, op=op))

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
