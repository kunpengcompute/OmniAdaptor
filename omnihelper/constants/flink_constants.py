"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
              http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.

Flink 常量定义模块
"""


class ExcelColumns:
    """Excel 输出列名常量"""
    JOB_ID = 'jobid'
    TASK_ID = 'taskid'
    STATUS = '状态'
    OPERATOR_NAME = '算子名称'
    INPUT = 'Input'
    OUTPUT = 'Output'
    FREQUENCY = '出现频次'
    RUNTIME = '运行时间(s)'
    INPUT_DATA_SIZE = '输入数据量'
    OUTPUT_DATA_SIZE = '输出数据量'
    FUNC_NAME = '表达式/内置函数名称'
    FUNC_INPUT = '表达式Input'
    NESTED_CONTENT = '嵌套内容'
    FUNC_FREQUENCY = '表达式出现频次'


class TaskStatus:
    """任务状态常量"""
    SUCCESS = "获取成功"
    JOB_DETAIL_FAILED = "作业详情获取失败"
    JOB_PLAN_FAILED = "作业执行计划获取失败"
    VERTEX_METRICS_FAILED = "任务指标获取失败"
    VERTEX_METRICS_EMPTY = "任务指标为空"
    OPERATOR_PARSE_FAILED = "算子解析失败"
    NETWORK_ERROR = "网络请求失败"
    REQUEST_TIMEOUT = "请求超时"
    UNKNOWN_ERROR = "未知异常"


class MetricType:
    """指标类型常量"""
    NUM_RECORDS_IN = "numRecordsIn"
    NUM_RECORDS_IN_PER_SECOND = "numRecordsInPerSecond"
    NUM_RECORDS_OUT = "numRecordsOut"
    NUM_RECORDS_OUT_PER_SECOND = "numRecordsOutPerSecond"
    NUM_BYTES_IN = "numBytesIn"
    NUM_BYTES_IN_PER_SECOND = "numBytesInPerSecond"
    NUM_BYTES_OUT = "numBytesOut"
    NUM_BYTES_OUT_PER_SECOND = "numBytesOutPerSecond"

    @classmethod
    def get_target_metrics(cls):
        """获取目标指标列表"""
        return [
            f".{cls.NUM_RECORDS_IN}",
            f".{cls.NUM_RECORDS_IN_PER_SECOND}",
            f".{cls.NUM_RECORDS_OUT}",
            f".{cls.NUM_RECORDS_OUT_PER_SECOND}",
            f".{cls.NUM_BYTES_IN}",
            f".{cls.NUM_BYTES_IN_PER_SECOND}",
            f".{cls.NUM_BYTES_OUT}",
            f".{cls.NUM_BYTES_OUT_PER_SECOND}",
        ]


class CsvColumns:
    """CSV 文件列名常量"""
    TABLE_NAME = 'table_name'
    FIELD_NAME = 'field_name'
    FIELD_TYPE = 'field_type'

    @classmethod
    def get_required_columns(cls):
        """获取 CSV 必需列"""
        return [cls.TABLE_NAME, cls.FIELD_NAME, cls.FIELD_TYPE]