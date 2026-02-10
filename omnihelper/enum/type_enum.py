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
from enum import Enum

class TypeEnum(Enum):
    ARRAY = "ARRAY"
    BYTE = "BYTE"
    STRING = "STRING"
    INT = "INT"
    SHORT = "SHORT"
    LONG = "LONG"
    DOUBLE = "DOUBLE"
    MAP = "MAP"
    NULL = "NULL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    NESTED_FUNCTIONS = "NESTED_FUNCTIONS"
    DECIMAL = "DECIMAL"
    DECIMAL64 = "DECIMAL64"
    DECIMAL128 = "DECIMAL128"
    PARTITION = "PARTITION"