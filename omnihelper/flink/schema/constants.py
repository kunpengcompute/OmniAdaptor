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
Flink 类型系统常量定义
"""

# 最大递归深度
MAX_DEPTH = 10

# 未知类型标识
UNKNOWN = "unknown"

# 嵌套函数标识
NESTED_FUNCTION = "NESTED_FUNCTION"

# OMNI 类型 ID 到类型名称的映射表
OMNI_TYPE_ID_MAP = {
    1: "INT",           # 整型
    2: "BIGINT",        # 长整型
    3: "DOUBLE",        # 双精度浮点型
    4: "FLOAT",         # 单精度浮点型
    5: "BOOLEAN",       # 布尔型
    6: "TINYINT",       # 微型整型
    7: "SMALLINT",      # 小型整型
    8: "DATE",          # 日期类型
    9: "TIME",          # 时间类型
    10: "TIMESTAMP",    # 时间戳类型
    11: "TIMESTAMP_LTZ",# 本地时区时间戳
    12: "DECIMAL",      # 高精度小数
    13: "BINARY",       # 二进制类型
    14: "CHAR",         # 定长字符
    15: "VARCHAR",      # 变长字符
    16: "ARRAY",        # 数组类型
    17: "MAP",          # 映射类型
    18: "ROW",          # 行类型（嵌套结构）
    19: "VARBINARY",    # 变长二进制
    20: "MULTISET",     # 多重集合类型
}

# SQL 类型关键字列表（用于过滤别名）
SQL_TYPE_KEYWORDS = {
    "INT", "BIGINT", "VARCHAR", "DOUBLE", "FLOAT",
    "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL",
    "STRING", "LONG", "SHORT", "BYTE", "CHAR",
    "TINYINT", "SMALLINT", "BINARY", "VARBINARY",
    "ARRAY", "MAP", "ROW", "MULTISET",
}

# 透传算子类型列表（不改变字段结构）
PASS_THROUGH_OPERATORS = {
    "Deduplicate", "Expand", "WatermarkAssigner",
    "StreamRecordTimestampInserter", "ConstraintEnforcer", "Sink",
}

# 表达式类型常量
EXPR_TYPE_FIELD_REFERENCE = "FIELD_REFERENCE"
EXPR_TYPE_LITERAL = "LITERAL"
EXPR_TYPE_FUNCTION = "FUNCTION"
EXPR_TYPE_BINARY = "BINARY"
EXPR_TYPE_UNARY = "UNARY"
EXPR_TYPE_CASE = "CASE"
EXPR_TYPE_SWITCH = "SWITCH"
EXPR_TYPE_COALESCE = "COALESCE"
EXPR_TYPE_IS_NOT_NULL = "IS_NOT_NULL"
EXPR_TYPE_MULTIPLE_AND_OR = "MULTIPLE_AND_OR"
EXPR_TYPE_IN = "IN"
EXPR_TYPE_BETWEEN = "BETWEEN"
EXPR_TYPE_REGEX_EXTRACT = "REGEX_EXTRACT"
EXPR_TYPE_SPLIT_INDEX = "SPLIT_INDEX"
EXPR_TYPE_PROCTIME = "PROCTIME"

# 函数返回类型规则
RETURN_TYPE_RULE_ARGUMENT = "ARGUMENT_TYPE"
RETURN_TYPE_RULE_RESULT = "RESULT_TYPE"
