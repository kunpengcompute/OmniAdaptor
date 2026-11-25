# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# OmniAdaptor is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

import re


class ExprTree:
    def __init__(self, left, op, right, is_wrap=False):
        self.left = re.sub(r"#\d+(L)*", "", left).strip()
        self.op = op
        self.right = re.sub(r"#\d+(L)*", "", right).strip()
        self.is_wrap = is_wrap

    def __str__(self):
        expr = f"{self.left} {self.op} {self.right}"
        if self.is_wrap:
            return f"({expr})"
        return expr

    def wrap(self, x):
        return [x, f"({x})"]

    def build(self):
        results = []
        for left_param in self.wrap(self.left):
            for right_param in self.wrap(self.right):
                for whole in [False, True]:
                    expr = ExprTree(left_param, self.op, right_param, whole)
                    results.append(str(expr))
        return results
