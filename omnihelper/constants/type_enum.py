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

class TypeEnum(Enum):
    ARRAY = "ARRAY"
    BYTE = "BYTE"
    STRING = "STRING"
    INT = "INT"
    SHORT = "SHORT"
    LONG = "LONG"
    FLOAT = "FLOAT"
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
