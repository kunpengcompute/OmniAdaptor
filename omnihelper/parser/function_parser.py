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

from omnihelper.enum.function_enum import FunctionEnum
from omnihelper.parser.function_checker import FunctionChecker
from omnihelper.parser.type_matcher import TypeMatcher
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.func_util import extract_cast_param

# 在函数提取中需要排除的表达式
EXCLUDED_EXPRS = [FunctionEnum.IF.value, FunctionEnum.CASE.value, FunctionEnum.IN.value]
# 在表达式提取中需要排除的函数
EXCLUDED_FUNCTIONS = [FunctionEnum.IF.value, FunctionEnum.CASE.value, FunctionEnum.FILTER.value]
# trim相关函数
TRIM_FUNCTIONS = [FunctionEnum.TRIM.value, FunctionEnum.LTRIM.value, FunctionEnum.RTRIM.value, FunctionEnum.BTRIM.value]

class FunctionParser:

    DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "omni_function_dictionary.json")
    UDF_DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "udf_dictionary.json")

    def __init__(self):
        self.function_list = []
        self.omni_functions = []
        self.udf_list = []
        self.user_defined_functions = []
        self.partial_func_mapping = {}
        self.all_funcs = []
        self.load_func_list()

    def load_func_list(self):
        try:
            with open(self.DICTIONARY_PATH, "r", encoding="utf-8") as f:
                self.function_list = json.load(f)
        except Exception as e:
            raise Exception("Failed to load the functions list: " + str(e))

        if os.path.exists(self.UDF_DICTIONARY_PATH):
            try:
                with open(self.UDF_DICTIONARY_PATH, "r", encoding="utf-8") as f:
                    self.udf_list = json.load(f)
            except Exception as e:
                raise Exception("Failed to load the user defined function: " + str(e))
        self.omni_functions = [func.get("func_name").lower() for func in self.function_list]
        self.user_defined_functions = [func.get("func_name").lower() for func in self.udf_list]
        self.all_funcs = self.omni_functions + self.user_defined_functions
        self.func_pattern = re.compile("({})\\((.*)".format("|".join(map(re.escape, self.all_funcs))))
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
        param_type_mapping = {}
        physical_plan = event.get("physical plan")
        if not physical_plan:
            return []
        if event.get("node metrics"):
            TypeMatcher.extract_param_type(event.get("node metrics"), param_type_mapping)
        update_physical_plan = self.preprocess_physical_plan(physical_plan)

        for line in update_physical_plan:
            if "ReadSchema" in line:
                # 更新参数类型映射表
                TypeMatcher.extract_param_type(line, param_type_mapping)
            func_pairs = self.search_func_expr_pairs(line)
            if not func_pairs:
                continue

            for pair in func_pairs:
                func_name = pair.get("func")
                params = pair.get("params")

                input_type = TypeMatcher.get_input_type(params, param_type_mapping, event.get("original query"), pair)
                function_checker = FunctionChecker(self.function_list, self.udf_list)
                is_not_supported_func = function_checker.check_support_status(func_name, params, input_type, event.get("original query"))
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
            processed_line = self.extract_bracket_content(line)
            preprocess_phy_plan.append(processed_line)
        return preprocess_phy_plan

    def extract_bracket_content(self, line):
        """
        去除算子参数的名称字段，例如Input、Keys、Output等字段，只取值
        例如：Input [12]: [sum#167]，仅保留sum#167
        """
        line = line.strip()

        # pattern: 任意前缀 + 数字 + : + [内容]，非贪婪匹配最外层方括号内容
        m = re.search(r"\[[^\[\]]*\]\s*:\s*\[(.*)\]$", line)
        if m:
            return m.group(1).strip()

        return line

    def search_func_expr_pairs(self, line):
        func_expr_pairs = []
        self.search_func_calls(line, func_expr_pairs)
        self.search_exprs(line, func_expr_pairs)
        self.extract_special_func(line, func_expr_pairs)
        return func_expr_pairs

    def search_func_calls(self, line, func_expr_pairs):
        if not self.func_pattern.search(line):
            return

        calls = self.extract_function_calls(line)
        for call in calls:
            func = self.extract_func_name(call)
            params = self.extract_func_args(call)
            if not func.lower() in self.all_funcs:
                continue
            if func.lower() in EXCLUDED_EXPRS:
                continue
            if func.lower() in TRIM_FUNCTIONS and len(params) > 1 and params[1] == "None":
                # trim函数的第二个参数如果为None则删除第二个参数
                del params[1]
            if func.lower() == FunctionEnum.CAST.value:
                # cast函数的参数XXX as type的形式，需要特殊处理
                params = extract_cast_param(call)
                if not params:
                    continue
            func_expr_pairs.append({"func": func.lower(), "params": params, "extract_type": "func"})

    def search_exprs(self, line, func_expr_pairs):
        exprs = self.split_by_ops(line)
        for expr in exprs:
            params = []
            left, op, right = expr
            if not op.lower() in self.all_funcs:
                continue
            if op.lower() in EXCLUDED_FUNCTIONS:
                continue
            left_param = self.strip_outer_parens(self.extract_left_param(left))
            params.append(left_param)

            right_param = self.strip_outer_parens(self.extract_right_param(right))
            if not op.lower() == FunctionEnum.IN.value:
                # in表达式只需要提取一边的类型
                params.append(right_param)

            if not left_param or not right_param:
                continue
            if re.fullmatch(r"^\s*(?::\s*)*(?:\+\-|\:\-|\-)\s*$", left_param):
                # 排除类似【+- * Project (32)】左边参数是+-，: +-，: :-等情况
                continue

            func_expr_pairs.append({"func": op.lower(), "params": params, "extract_type": "expr"})

    def extract_special_func(self, line, func_expr_pairs):
        line_low = line.lower()
        if "if (" in line_low:
            self.extract_if_expressions(line, func_expr_pairs)
        if "case when" in line_low:
            self.extract_case_when_exprs(line, func_expr_pairs)

    def extract_if_expressions(self, line, func_expr_pairs):
        results = []
        for m in re.finditer(r"\bif\s*\(", line):
            start = m.start()
            parsed = self.parse_if(line[start:])
            if parsed:
                results.append(parsed)
        for res in results:
            params = self.collect_if_values(res)
            func_expr_pairs.append({
                "func": "if",
                "params": params,
                "extract_type": "func"
            })

    def parse_if(self, expr):
        """
        提取if函数的cond, true_value, false_value
        :return: dict<cond:条件，true_value:为真的值，false_value:为假的值，nested:嵌套的if内容>
        """
        expr = expr.strip()
        if not expr.startswith("if"):
            return None

        # 解析cond
        i = expr.find("(")
        if i == -1:
            return None

        stack = 0
        cond_start = i +1
        j = cond_start
        while j < len(expr):
            if expr[j] == "(":
                stack += 1
            elif expr[j] == ")":
                if stack == 0:
                    cond_end = j
                    break
                stack -= 1
            j += 1
        else:
            return None

        cond = expr[cond_start:cond_end].strip()

        # 解析true_value
        k = cond_end + 1
        while k < len(expr) and expr[k].isspace():
            k += 1

        true_start = k
        stack = 0

        while k < len(expr):
            if expr[k] == "(":
                stack += 1
            elif expr[k] == ")":
                stack -= 1
            elif expr[k:k + 4] == "else" and stack == 0:
                true_end = k
                break
            k += 1
        else:
            return None

        true_value = expr[true_start:true_end].strip()
        true_value = self.clean_spark_suffix(true_value)

        # 解析false_value
        false_start = true_end + 4
        false_raw = expr[false_start:].lstrip()

        # 扫描到顶层分隔符（逗号、右括号、右中括号）
        stack = 0
        end = len(false_raw)
        for idx, ch in enumerate(false_raw):
            if ch == "(":
                stack += 1
            elif ch == ")":
                if stack == 0:
                    end = idx
                    break
                stack -= 1
            elif ch in "]," and stack == 0:
                end = idx
                break

        false_value = false_raw[:end].strip()
        false_value = self.clean_spark_suffix(false_value)

        nested = None
        if false_value.startswith(FunctionEnum.IF.value):
            nested = self.parse_if(false_value)
            false_value = ""

        return {
            "cond": cond,
            "true_value": true_value,
            "false_value": false_value,
            "nested": nested
        }

    def clean_spark_suffix(self, expr):
        """去除spark的参数后缀"""
        expr = expr.strip()

        # 去除AS别名
        m = re.search(r"\s+AS\s+[A-Za-z0-9_#]+$", expr, re.I)
        if m:
            expr = expr[:m.start()].strip()

        # 去除排序修饰：ASC/DESC [NULLS FIRST|LAST]
        expr = re.sub(r"\s+(ASC|DESC)\s+(NULLS\s+(FIRST|LAST))$", "", expr, re.I)
        expr = re.sub(r"\s+(ASC|DESC)$", "", expr, re.I)

        # 去掉末尾逗号、空白
        expr = re.sub(r"[,\s]+$", "", expr)

        # 去掉多余右括号、中括号
        while expr and expr[-1] in ")]":
            if expr[-1] == ")" and self.paren_balance(expr) < 0:
                expr = expr[:-1].rstrip()
            elif expr[-1] == "]" and self.bracket_balance(expr) < 0:
                expr = expr[:-1].rstrip()
            else:
                break
        return expr

    def paren_balance(self, expr):
        bal = 0
        for ch in expr:
            if ch == "(":
                bal += 1
            elif ch == ")":
                bal -= 1
        return bal

    def bracket_balance(self, expr):
        bal = 0
        for ch in expr:
            if ch == "[":
                bal += 1
            elif ch == "]":
                bal -= 1
        return bal

    def collect_if_values(self, res):
        """递归收集true_value/false_value"""
        values = []
        if res.get("true_value"):
            values.append(res["true_value"])
        if res.get("false_value"):
            values.append(res["false_value"])
        # 递归处理 nested
        nested = res.get("nested")
        if nested:
            values.extend(self.collect_if_values(nested))
        return values

    def extract_case_when_exprs(self, line, func_expr_pairs):
        line_low = line.lower()
        res = []
        i = 0

        while i < len(line):
            if line_low.startswith(" then ", i):
                st = i + 6
                ed = self.skip_expr(line_low, st, line)
                res.append(self.strip_outer_parens(line[st:ed].strip()))
                i = ed
            elif line_low.startswith(" else ", i):
                st = i + 6
                ed = line_low.find(" end", st)
                res.append(self.strip_outer_parens(line[st:ed].strip()))
                break
            else:
                i += 1

        func_expr_pairs.append({"func": "case", "params": res, "extract_type": "func"})

    def skip_expr(self, line_low, pos, line):
        depth = 0
        while pos < len(line):
            if line[pos] == "(":
                depth += 1
            elif line[pos] == ")":
                depth -= 1
            elif depth == 0 and line_low.startswith((" when ", " then ", " else ", " end "), pos):
                break
            pos += 1
        return pos

    def extract_function_calls(self, line):
        """
        提取行内的所有函数调用
        :return:
        """
        results = []
        stack = []
        depth = 0
        i = 0
        n = len(line)

        while i < n:
            if line[i].isalpha() or line[i] == "_":
                j = i + 1
                while j < n and (line[j].isalnum() or line[j] == '_'):
                    j += 1
                if j < n and line[j] == "(":
                    stack.append((i, depth))
                    i = j

            if line[i] == "(":
                depth += 1
            elif line[i] == ")":
                depth -= 1
                if stack and stack[-1][1] == depth:
                    start, _ = stack.pop()
                    results.append(line[start:i + 1])
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
        paren = 0  # 统计小括号出现次数
        bracket = 0  # 统计中括号出现次数
        brace = 0  # 统计大括号出现次数
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
            elif ch == "{":
                brace += 1
                buf.append(ch)
            elif ch == "}":
                brace -= 1
                buf.append(ch)
            elif ch == "," and paren == 0 and bracket == 0 and brace == 0:
                args.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            args.append("".join(buf).strip())
        return args

    def split_by_ops(self, expr):
        results = []
        pattern = r"\s+(%s)\s+" % "|".join(map(re.escape, self.all_funcs))
        for m in re.finditer(pattern, expr, re.I):
            results.append((
                expr[:m.start(1)].strip(),
                m.group(1),
                expr[m.end(1):].strip()
            ))
        return results

    def extract_left_param(self, left_part):
        """
        返回表达式左边最靠近的参数或函数：
        - 支持函数调用：rand(..)等
        - 支持括号表达式：(255.0)
        - 支持为闭合括号：((255
        - 支持普通token：c_string#11, 255.0
        - 支持表达式partition列：avg(c_int)#20
        :return: 左边参数
        """
        s = left_part.rstrip()
        n = len(s)
        if not n:
            return s

        # 找到最右边的非空字符
        i = n - 1
        while i >= 0 and s[i].isspace():
            i -= 1
        if i < 0:
            return ""

        # 如果是token（字母/数字/#/_/.）
        if s[i].isalnum() or s[i] in ["#", "_", "."]:
            j = i
            while j >= 0 and (s[j].isalnum() or s[j] in ["#", "_", "."]):
                j -= 1
            token_start = j + 1
            token = s[token_start:i + 1]

            # 检查token前是否紧跟未闭合括号表达式
            k = token_start - 1
            if k >= 0 and s[k] == ")":
                # 解析括号表达式
                depth = 0
                paren_end = k
                paren_start = -1
                for t in range(k, -1, -1):
                    if s[t] == ")":
                        depth += 1
                    elif s[t] == "(":
                        depth -= 1
                        if depth == 0:
                            paren_start = t
                            break

                if paren_start != -1:
                    # 扩展函数名
                    f = paren_start - 1
                    while f >= 0 and (s[f].isalnum() or s[f] in ["_", "#"]):
                        f -= 1
                    return s[f + 1:i + 1]
            return token

        # 如果是右括号 -> 完整函数或括号表达式
        if s[i] == ')':
            depth = 0
            paren_end = i
            paren_start = -1

            for k in range(i, -1, -1):
                if s[k] == ")":
                    depth += 1
                elif s[k] == "(":
                    depth -= 1
                    if depth == 0:
                        paren_start = k
                        break

            if paren_start != -1:
                # 扩展函数名
                j = paren_start - 1
                while j >= 0 and (s[j].isalnum() or s[j] in ["_", "#"]):
                    j -= 1
                return s[j + 1:paren_end + 1].strip()

            return s[i:]

        # 如果是左括号 -> 未闭合括号
        if s[i] == "(":
            j = i - 1
            while j >= 0 and (s[j].isalnum() or s[j] in ["#", "_", "."]):
                j -= 1
            return s[j + 1:i].strip()

        return s[i]

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

    def extract_right_param(self, right_part):
        """
        返回表达式右边最靠近的参数或函数
        - 支持函数调用：lower(...), rand(...)
        - 括号表达式: (255.0)
        - 普通token: c_string#11, 255.0
        :return: 右边参数
        """
        s = right_part.lstrip()
        n = len(s)
        if not n:
            return s

        i = 0
        # 找第一个非空字符
        while i < n and s[i].isspace():
            i += 1
        if i >= n:
            return ""

        ch = s[i]

        # 如果是字母/下划线/# 开头 -> 可能是函数名或普通标识符
        if ch.isalpha() or ch in ["_", "#"]:
            start = i
            # 先读完整标识符
            while i < n and (s[i].isalnum() or s[i] in ["_", "#"]):
                i += 1
            name_end = i

            # 跳过空格
            while i < n and s[i].isspace():
                i += 1

            # 如果后面紧跟'(' -> 函数调用，向后找匹配的右括号
            if i < n and s[i] == "(":
                depth = 0
                lparen = i
                end = None
                for k in range(lparen, n):
                    if s[k] == "(":
                        depth += 1
                    elif s[k] == ")":
                        depth -= 1
                        if depth == 0:
                            end = k
                            break
                if end is not None:
                    return s[start:end + 1].strip()
                else:
                    # 括号没闭合，就取到结尾
                    return s[start:].strip()
            else:
                # 普通标识符
                return s[start:name_end].strip()

        # 如果是'(' -> 括号表达式
        if ch == "(":
            depth = 0
            lparen = i
            end = None
            for k in range(lparen, n):
                if s[k] == "(":
                    depth += 1
                elif s[k] == ")":
                    depth -= 1
                    if depth == 0:
                        end = k
                        break
            if end is not None:
                return s[lparen:end + 1].strip()
            else:
                # 括号没闭合 就取到结尾
                return s[lparen:].strip()

        # 如果是数字或'.' -> 数字token（支持小数）
        if ch.isdigit() or ch == ".":
            start = i
            while i < n and (s[i].isdigit() or s[i] == "."):
                i += 1
            return s[start:i]

        # 兜底：返回这个字符后面的连续非空白
        start = i
        while i < n and not s[i].isspace():
            i += 1
        return s[start:i]

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
                "times": times,
                "is_udf": True if func_name.lower() in self.user_defined_functions else False
            })
        return update_event_result
