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
import re


from omnihelper.constants.function_enum import FunctionEnum
from omnihelper.util.func_util import extract_cast_param, strip_outer_parens

# 在函数提取中需要排除的表达式
EXCLUDED_EXPRS = [FunctionEnum.IF.value, FunctionEnum.CASE.value, FunctionEnum.IN.value]
# 在表达式提取中需要排除的函数
EXCLUDED_FUNCTIONS = [FunctionEnum.IF.value, FunctionEnum.CASE.value, FunctionEnum.FILTER.value]
# trim相关函数
TRIM_FUNCTIONS = [FunctionEnum.TRIM.value, FunctionEnum.LTRIM.value, FunctionEnum.RTRIM.value, FunctionEnum.BTRIM.value]

class FunctionBuilder:

    def __init__(self, func_pattern, all_funcs):
        self.func_pattern = func_pattern
        self.all_funcs = all_funcs

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
            func_expr_pairs.append({"func": func.lower(), "params": params, "type": "func"})

    def search_exprs(self, line, func_expr_pairs):
        exprs = self.split_by_ops(line)
        for expr in exprs:
            params = []
            left, op, right = expr
            if not op.lower() in self.all_funcs:
                continue
            if op.lower() in EXCLUDED_FUNCTIONS:
                continue
            left_param = strip_outer_parens(self.extract_left_param(left))
            params.append(left_param)

            right_param = strip_outer_parens(self.extract_right_param(right))
            if not op.lower() == FunctionEnum.IN.value:
                # in表达式只需要提取一边的类型
                params.append(right_param)

            if not left_param or not right_param:
                continue
            if re.fullmatch(r"^\s*(?::\s*)*(?:\+\-|\:\-|\-)\s*$", left_param):
                # 排除类似【+- * Project (32)】左边参数是+-，: +-，: :-等情况
                continue
            if left_param == "Expression" and op == "=":
                # 排除类似【Subquery:Hosting Expression = ...】的情况
                continue

            func_expr_pairs.append({"func": op.lower(), "params": params, "type": "expr"})

    def extract_special_func(self, line, func_expr_pairs):
        line_low = line.lower()
        if "if (" in line_low:
            self.extract_if_func(line, func_expr_pairs)
        if "case when" in line_low:
            self.extract_case_when_func(line, func_expr_pairs)

    def extract_if_func(self, line, func_expr_pairs):
        results = []
        pos = 0
        while pos < len(line):
            node, new_pos = self.parse_if(line, pos)
            if not node:
                pos += 1
            else:
                results.append(node)
                pos = new_pos
        for res in results:
            params = self.collect_if_values(res)
            func_expr_pairs.append({
                "func": FunctionEnum.IF.value,
                "params": params,
                "type": "func"
            })

    def parse_if(self, expr, pos=0):
        """
        提取if函数的cond, true_value, false_value
        :return: dict<cond:条件，true_value:为真的值，false_value:为假的值，nested:嵌套的if内容>
        """
        n = len(expr)
        # 匹配if
        m = re.match(r"\s*if\s*\(", expr[pos:], re.I)
        if not m:
            return None, pos

        i = pos + m.end() - 1

        # 解析cond
        depth = 0
        cond_start = i + 1
        j = cond_start
        while j < n:
            if expr[j] == "(":
                depth += 1
            elif expr[j] == ")":
                if depth == 0:
                    break
                depth -= 1
            j += 1

        cond = expr[cond_start:j].strip()
        cur = j + 1

        while cur < n and expr[cur].isspace():
            # 跳过空格
            cur += 1

        # 解析true_expr
        true_node, cur = self.parse_expr(expr, cur, True)

        while cur < n and expr[cur].isspace():
            # 跳过空格
            cur += 1

        if not expr[cur:cur + 4].lower() == "else":
            return None, pos

        cur += 4
        while cur < n and expr[cur].isspace():
            # 跳过空格
            cur += 1

        # 解析false_expr
        false_node, cur = self.parse_expr(expr, cur, False)

        node = {
            "cond": cond,
            "true_value": true_node,
            "false_value": false_node
        }

        return node, cur

    def parse_expr(self, expr, pos, is_true_node):
        n = len(expr)

        # 如果是if，递归
        if re.match(r"\s*if\s*\(", expr[pos:], re.I):
            return self.parse_if(expr, pos)

        # 解析普通表达式
        depth = 0
        start = pos
        cur = pos

        while cur < n:
            if expr[cur] == "(":
                depth += 1
            elif expr[cur] == ")":
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and expr[cur] in "],":
                break
            elif depth == 0 and is_true_node and expr[cur:cur + 4].lower() == "else":
                break
            cur += 1

        return self.clean_spark_suffix(expr[start:cur].strip()), cur

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
        if isinstance(res["true_value"], str):
            values.append(res["true_value"])
        if isinstance(res["true_value"], dict):
            values.extend(self.collect_if_values(res["true_value"]))

        if isinstance(res["false_value"], str):
            values.append(res["false_value"])
        if isinstance(res["false_value"], dict):
            values.extend(self.collect_if_values(res["false_value"]))

        return values

    def extract_case_when_func(self, line, func_expr_pairs):
        line_low = line.lower()
        n = len(line)
        start = 0

        while True:
            i = line_low.find("case when", start)
            if i == -1:
                break

            res = []
            pos = i + len("case")

            while pos < n:
                if line_low.startswith(" then ", pos):
                    st = pos + 6
                    ed = self.skip_expr(line_low, st, line)
                    res.append(strip_outer_parens(line[st:ed].strip()))
                    pos = ed
                elif line_low.startswith(" else ", pos):
                    st = pos + 6
                    ed = self.skip_expr(line_low, st, line)
                    res.append(strip_outer_parens(line[st:ed].strip()))
                    pos = ed
                elif line_low.startswith(" end", pos):
                    start = i + 9  # 从当前case when后开始找下一个
                    break
                else:
                    pos += 1

            func_expr_pairs.append({"func": "case", "params": res, "type": "func"})

    def skip_expr(self, line_low, pos, line):
        depth = 0
        while pos < len(line):
            if line[pos] == "(":
                depth += 1
            elif line[pos] == ")":
                depth -= 1
            elif depth == 0 and line_low.startswith((" when ", " then ", " else ", " end"), pos):
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
                while j < n and (line[j].isalnum() or line[j] == "_"):
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
        m = re.match(r"\s*([a-zA-Z_]\w*)\s*\(", call)
        return m.group(1) if m else ""

    def extract_func_args(self, call):
        """
        提取函数调用的参数值
        :return: 参数列表
        """
        l = call.find("(")
        r = call.rfind(")")
        if l == -1 or r == -1 or r <= l:
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
        if left_part.rstrip().endswith("END"):
            return self.extract_case_when_param(left_part)

        return self.extract_base_left_param(left_part)

    def extract_case_when_param(self, expr):
        """
        从右向左提取完整 CASE WHEN ... END表达式
        """
        expr = expr.rstrip()
        n = len(expr)

        # 1. 先找到最右边的END
        end_pos = None
        for k in range(n - 1, -1, -1):
            if k >= 2 and expr[k - 2:k + 1].lower() == "end":
                end_pos = k + 1
                break
        if end_pos is None:
            return "END"

        i = end_pos - 4
        depth = 0

        while i >= 0:
            if i >= 2 and expr[i -2:i + 1].lower() == "end":
                depth += 1
                i -= 3
                continue

            if i >= 3 and expr[i - 3:i + 1].lower() == "case":
                if depth == 0:
                    case_start = i - 3
                    return expr[case_start:end_pos].strip()
                else:
                    depth -= 1
                    i -= 4
                    continue

            i -= 1

        return "END"

    def extract_base_left_param(self, left_part):
        """
        返回表达式左边最靠近的参数或函数：
        - 支持函数调用：rand(..)等
        - 支持括号表达式：(255.0)
        - 支持未闭合括号：((255
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
            while j >= 0 and (s[j].isalnum() or s[j] in ["#", "_", ".", "-"]):
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
        if s[i] == ")":
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

    def extract_right_param(self, right_part):
        """
        返回表达式右边最靠近的参数或函数
        :return: 右边参数
        """
        if right_part.lower().lstrip().startswith(FunctionEnum.IF.value):
            return self.extract_if_expr(right_part.lstrip())

        return self.extract_base_param(right_part)

    def extract_if_expr(self, expr):
        """
        提取表达式右侧为if函数的完整参数
        :return: 完整的if函数
        """
        i = 2
        n = len(expr)

        while i < n and expr[i].isspace():
            i += 1

        if expr[i] != "(":
            return None

        depth = 0
        cond_end = None
        for k in range(i, n):
            if expr[k] == "(":
                depth += 1
            elif expr[k] == ")":
                depth -= 1
                if depth == 0:
                    cond_end = k
                    break

        pos = cond_end + 1

        while pos < n and expr[pos].isspace():
            pos += 1

        true_expr, next_pos = self.extract_single_if_expr(expr, pos)

        pos = next_pos
        while pos < n and expr[pos].isspace():
            pos += 1

        if not expr.startswith("else", pos):
            return expr[:next_pos]

        pos += 4
        while pos < n and expr[pos].isspace():
            pos += 1

        false_expr, next_pos = self.extract_single_if_expr(expr, pos)

        return expr[:next_pos]

    def extract_single_if_expr(self, expr, pos):
        sub = expr[pos:].lstrip()
        offset = len(expr[pos:]) - len(sub)
        pos += offset

        if sub.lower().startswith(FunctionEnum.IF.value):
            expr = self.extract_if_expr(sub)
            return expr, pos + len(expr)

        expr = self.extract_base_param(sub)
        return expr, pos + len(expr)

    def extract_base_param(self, right_part):
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
        if ch.isalpha() or ch in ["_", "#", "'"]:
            start = i
            # 先读完整标识符
            while i < n and not s[i].isspace() and s[i] not in ["(", "[", ")", "]", ","]:
                i += 1
            name_end = i

            # 跳过空格
            while i < n and s[i].isspace():
                i += 1

            # 如果后面紧跟"(" -> 函数调用，向后找匹配的右括号
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

        # 如果是"(" -> 括号表达式
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

        # 如果是数字或"." -> 数字token（支持小数）
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