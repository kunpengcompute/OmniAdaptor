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
from omnihelper.constants.type_enum import TypeEnum

NOT_SUPPORTED_TYPE = [TypeEnum.PARTITION.value, TypeEnum.NESTED_FUNCTIONS.value]

def extract_cast_param(call):
    call = call.strip()
    if not call.lower().startswith("cast(") or not call.lower().endswith(")"):
        return []
    inner = call[5:-1]

    level = 0
    for i in range(len(inner)):
        if inner[i] == "(":
            level += 1
        elif inner[i] == ")":
            level -= 1
        elif level == 0 and inner[i:i + 3].lower().strip() == "as":
            left = inner[:i].strip()
            right = inner[i + 3:].strip()
            return [left, right]
    return []

def replace_predicate_partition(args):
    # 找到第一个支持类型
    supported = None
    for arg in args:
        if arg not in NOT_SUPPORTED_TYPE and arg != TypeEnum.NULL.value:
            supported = arg
            break

    if supported is None:
        return args

    return [supported] * len(args)

def strip_outer_parens(expr):
    expr = expr.strip()
    if expr.startswith("(") and expr.endswith(")"):
        depth = 0
        for i, ch in enumerate(expr):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and i != len(expr) - 1:
                    return expr
        return expr[1:-1].strip()
    return expr
