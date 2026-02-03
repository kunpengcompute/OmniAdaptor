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