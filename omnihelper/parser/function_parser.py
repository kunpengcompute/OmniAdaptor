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
import json
import os
import re
import hashlib
from collections import defaultdict

from omnihelper.parser.type_matcher import TypeMatcher, TypeEnum
from omnihelper.util.common_util import CommonUtil

NOT_SUPPORTED_TYPE = [TypeEnum.PARTITION.value, TypeEnum.NESTED_FUNCTIONS.value]

class FunctionParser:

    DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "omni_function_dictionary.json")

    def __init__(self):
        self.function_list = []
        self.omni_functions = []
        self.param_type_mapping = {}
        self.partial_func_mapping = {}
        self.load_func_list()

    def load_func_list(self):
        try:
            with open(self.DICTIONARY_PATH, "r", encoding="utf-8") as f:
                self.function_list = json.load(f)
        except Exception as e:
            raise Exception("Failed to load the functions list: " + str(e))
        self.omni_functions = [func.get("func_name").lower() for func in self.function_list]
        self.func_pattern = re.compile("({})\\((.*)".format("|".join(map(re.escape, self.omni_functions))))
        for func in self.function_list:
            if func.get("hash_agg_func"):
                self.partial_func_mapping[func["func_name"]] = func["hash_agg_func"]

    def parse_event(self, event):
        """
        单事件表达式、函数解析核心逻辑
        :return:
        """
        if not self.function_list:
            return []
        analysis_result = []
        physical_plan = event.get("physical plan")
        if not physical_plan:
            return []
        if event.get("node_metrics"):
            TypeMatcher.extract_param_type(event.get("node_metrics"), self.param_type_mapping)
        update_physical_plan = self.preprocess_physical_plan(physical_plan)

        for line in update_physical_plan:
            if "ReadSchema" in line:
                # 更新参数类型映射表
                TypeMatcher.extract_param_type(line, self.param_type_mapping)
                continue
            func_pairs = self.match_expr_pattern(line)
            if not func_pairs:
                continue

            for pair in func_pairs:
                func_name = pair.get("func")
                params = pair.get("params")

                input_type = TypeMatcher.get_input_type(params, self.param_type_mapping)
                is_not_supported_func = self.evaluate_support_status(func_name, params, input_type)
                if not is_not_supported_func:
                    continue
                not_supported_func = self.build_not_supported_func(func_name, event, input_type)
                analysis_result.append(not_supported_func)
        return self.count_func_times(analysis_result)

    def preprocess_physical_plan(self, physical_plan):
        preprocess_phy_plan = []
        split_phy_plan = physical_plan.split("\n")
        for line in split_phy_plan:
            if not line or line.strip().startswith("+- =="):
                continue
            if line.startswith("Condition"):
                # 行内容为condition时，后面会跟着一个括号
                condition_match = re.match(r"Condition\s*:\s*\((.*)\)\s*", line)
                if condition_match:
                    line = condition_match.group(1)
            preprocess_phy_plan.append(line)
        return preprocess_phy_plan

    def match_expr_pattern(self, line):
        func_pairs = []
        if self.func_pattern.search(line):
            calls = self.extract_function_calls(line)
            for call in calls:
                func = self.extract_func_name(call)
                params = self.extract_func_args(call)
                if not func.lower() in self.omni_functions:
                    continue
                func_pairs.append({"func": func.lower(), "params": params})

        exprs = self.split_by_ops(line)
        for expr in exprs:
            left, op, right = expr
            left_param = self.extract_left_param(left)
            right_param = self.extract_right_param(right)
            params = [self.strip_outer_parens(left_param), self.strip_outer_parens(right_param)]

            if not left_param or not right_param:
                continue
            if re.fullmatch(r"^\s*(?::\s*)*(?:\+\-|\:\-)\s*$", left_param):
                # 排除类似【+- * Project (32)】的情况
                continue
            if not op.lower() in self.omni_functions:
                continue
            func_pairs.append({"func": op.lower(), "params": params})

        return func_pairs

    def extract_function_calls(self, s):
        """
        提取行内的所有函数调用
        :return:
        """
        results = []
        stack = []
        depth = 0
        i = 0
        n = len(s)

        while i < n:
            if s[i].isalpha() or s[i] == "_":
                j = i + 1
                while j < n and (s[j].isalnum() or s[j] == '_'):
                    j += 1
                if j < n and s[j] == "(":
                    stack.append((i, depth))
                    i = j

            if s[i] == "(":
                depth += 1
            elif s[i] == ")":
                depth -= 1
                if stack and stack[-1][1] == depth:
                    start, _ = stack.pop()
                    results.append(s[start:i + 1])
            i += 1
        return results

    def extract_func_name(self, call):
        """
        提取函数调用的函数名
        :return: 函数名
        """
        m = re.match(r'\s*([a-zA-Z_]\w*)\s*\(', call)
        return m.group(1) if m else ""

    def extract_func_args(self, call):
        """
        提取函数调用的参数值
        :return: 参数列表
        """
        l = call.find("(")
        r = call.rfind(")")
        if l == -1 or r == -1 or r<= l:
            return []

        args_str = call[l + 1:r]
        args = []
        buf = []
        paren = 0
        bracket = 0
        for ch in args_str:
            if ch == "(":
                paren += 1
                buf.append(ch)
            elif ch == ")":
                paren -= 1
                buf.append(ch)
            elif ch == "[":
                # 排除【input[0, int, true]】这种情况
                bracket += 1
                buf.append(ch)
            elif ch == "]":
                bracket -= 1
                buf.append(ch)
            elif ch == "," and paren == 0 and bracket == 0:
                args.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            args.append("".join(buf).strip())
        return args

    def split_by_ops(self, expr):
        results = []
        pattern = r"\s+(%s)\s+" % "|".join(map(re.escape, self.omni_functions))
        for m in re.finditer(pattern, expr):
            results.append((
                expr[:m.start(1)].strip(),
                m.group(1),
                expr[m.end(1):].strip()
            ))
        return results

    def extract_left_param(self, left_part):
        """
        返回表达式左边最靠近的参数或函数
        :return: 左边参数
        """
        lt_index = len(left_part)
        depth = 0
        end = lt_index
        for i in range(lt_index - 1, -1, -1):
            ch = left_part[i]
            if ch == ")":
                depth += 1
            elif ch == "(":
                depth -= 1
            if depth < 0:
                return left_part[i + 1:end].strip()
        left = left_part[:lt_index].rstrip()
        return left

    def extract_right_param(self, right_part):
        """
        返回表达式右边最靠近的参数或函数
        :return: 右边参数
        """
        depth = 0
        for i, ch in enumerate(right_part):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if depth < 0:
                return right_part[:i].strip()
        right = right_part.rstrip()
        return right

    def strip_outer_parens(self, expr):
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

    def evaluate_support_status(self, func_name, params, input_type):
        for rule in self.function_list:
            if not rule.get("func_name").lower() == func_name.lower():
                continue
            if not rule.get("is_support_func"):
                # 表示是不支持的函数，需要记录到结果中
                return True
            if rule.get("param_count") and len(params) != rule.get("param_count"):
                # 表示是不支持的参数个数，需要记录到结果中
                return True
            for type in input_type:
                if type in rule.get("no_support_type") or type in NOT_SUPPORTED_TYPE:
                    # 只要有一个不支持，就判定为不支持
                    return True
        return False

    def build_not_supported_func(self, func_name, event, input_type):
        func_name = self.partial_func_mapping[func_name] if func_name in self.partial_func_mapping else func_name
        sql_hash = hashlib.sha256(event.get("original query").encode("utf-8")).hexdigest()[-6:]
        return {
            "func_name": func_name,
            "sql_hash": sql_hash,
            "input": input_type
        }

    def count_func_times(self, event_result):
        counter = defaultdict(int)

        for item in event_result:
            key = (item["func_name"], item["sql_hash"], tuple(item["input"]))
            counter[key] += 1

        update_event_result = []
        for (func_name, sql_hash, input_type), times in counter.items():
            update_event_result.append({
                "func_name": func_name,
                "sql_hash": sql_hash,
                "input": input_type,
                "times": times
            })
        return update_event_result
