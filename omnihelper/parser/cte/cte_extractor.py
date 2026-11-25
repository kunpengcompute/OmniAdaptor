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


class CTEExtractor:
    """从SQL中提取CTE定义的工具类"""

    def __init__(self):
        self.cte_definitions = {}

    def extract(self, sql):
        """
        提取所有CTE定义
        :param sql: 完整的SQL语句
        :return: {cte_name: cte_sql} 映射
        """
        sql = self._remove_comments(sql)

        with_match = re.search(r"\bWITH\b", sql, re.IGNORECASE)
        if not with_match:
            return {}

        # 从WITH之后开始解析
        sql_after_with = sql[with_match.end():]

        # 分割各个CTE
        ctes = self._split_ctes(sql_after_with)

        for cte_name, cte_sql in ctes:
            self.cte_definitions[cte_name] = cte_sql

        return self.cte_definitions

    def _remove_comments(self, sql):
        """移除SQL注释"""
        sql = re.sub(r"--[^\n]*", "", sql)

        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        return sql

    def _split_ctes(self, sql):
        """
        分割多个CTE定义
        """
        ctes = []

        # 使用栈来匹配括号，找到每个CTE的结束位置
        # CTE之间用逗号分隔

        # 找到所有CTE名称（AS关键字之前的部分）
        # 匹配模式: cte_name AS (
        pattern = re.compile(
            r"([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(",
            re.IGNORECASE
        )

        matches = list(pattern.finditer(sql))

        if not matches:
            return []

        for i, match in enumerate(matches):
            cte_name = match.group(1)
            as_pos = match.end() - 1

            # 找到这个CTE的SQL结束位置
            # 结束位置是下一个CTE的AS之前，或者是SQL的末尾
            if i + 1 < len(matches):
                next_as_pos = matches[i + 1].start()
                # 在next_as_pos之前找对应的结束括号
                end_pos = self._find_matching_paren(sql, as_pos)
                if end_pos and end_pos < next_as_pos:
                    cte_sql = sql[as_pos+1:end_pos]
                else:
                    cte_sql = sql[as_pos+1:next_as_pos]
            else:
                # 最后一个CTE
                end_pos = self._find_matching_paren(sql, as_pos)
                if end_pos:
                    cte_sql = sql[as_pos+1:end_pos]
                else:
                    cte_sql = sql[as_pos+1:]

            # 清理CTE SQL
            cte_sql = cte_sql.strip()
            if cte_sql.endswith(","):
                cte_sql = cte_sql[:-1].strip()

            ctes.append((cte_name, cte_sql))

        return ctes

    def _find_matching_paren(self, sql, open_pos):
        """
        找到匹配的结束括号位置
        :param sql: SQL字符串
        :param open_pos: '('的位置
        :return: int:匹配')'的位置，如果没找到返回None
        """
        if open_pos >= len(sql) or sql[open_pos] != '(':
            return None

        depth = 1
        i = open_pos + 1

        # 处理字符串
        in_string = False
        string_char = None

        while i < len(sql):
            char = sql[i]

            if char in ["'", "`"] and (i == 0 or sql[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if in_string:
                i += 1
                continue

            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return i

            i += 1

        return None
