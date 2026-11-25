# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# OmniAdaptor is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

from enum import Enum

class SparkTypeEnum(Enum):
    INT = "int"
    TINYINT = "tinyint"
    SMALLINT = "smallint"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    DOUBLE = "double"
    FLOAT = "float"
    BOOLEAN = "boolean"
    STRING = "string"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"
