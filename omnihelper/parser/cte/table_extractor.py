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


class TableExtractor:
    """从SQL中提取表引用的工具类"""

    # 匹配反引号包裹的表名：`table_name`
    BACKTICK_PATTERN = re.compile(r'`([^`]+)`')

    FROM_JOIN_PATTERN = re.compile(
        r'(?:FROM|JOIN|INTO)\s+(?:([a-zA-Z0-9_]+)\.)?([a-zA-Z0-9_]+)',
        re.IGNORECASE
    )

    def __init__(self):
        self.cte_names = set()

    def set_cte_names(self, cte_names):
        self.cte_names = set(cte_names)

    def extract_tables(self, sql):
        """
        提取SQL中所有表引用
        :param sql: SQL片段
        :return: list: 表名列表
        """
        if not sql:
            return []

        tables = set()

        # 1. 提取反引号包裹的表名（最优先）
        backtick_tables = self.BACKTICK_PATTERN.findall(sql)
        tables.update(backtick_tables)

        # 2. 提取FROM/JOIN后的表名
        from_join_matches = self.FROM_JOIN_PATTERN.findall(sql)
        for match in from_join_matches:
            # match是元组(schema, table_name)
            table_name = match[1] if match[1] else match[0]
            if table_name:
                tables.add(table_name)

        # 3. 处理子查询中的表引用
        subquery_tables = self._extract_subquery_tables(sql)
        tables.update(subquery_tables)

        tables = self._filter_sql_keywords(tables)

        return list(tables)

    def _extract_subquery_tables(self, sql):
        """
        解析子查询中的表引用
        """
        tables = set()

        stack = []
        start = -1

        for i, char in enumerate(sql):
            if char == "(":
                if start == -1:
                    start = i
                stack.append(i)
            elif char == ")":
                if stack:
                    stack.pop()
                    if not stack:
                        # 找到完整的子查询
                        subquery = sql[start+1:i]
                        # 递归提取子查询中的表
                        sub_tables = self.extract_tables(subquery)
                        tables.update(sub_tables)
                        start = -1

        return tables

    def _filter_sql_keywords(self, tables):
        """
        过滤掉SQL关键字和保留字
        """
        sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
            'FULL', 'CROSS', 'ON', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'AS',
            'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL',
            'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'OVER', 'PARTITION',
            'ROWS', 'RANGE', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW', 'UNBOUNDED',
            'INSERT', 'OVERWRITE', 'TABLE', 'VALUES', 'SET', 'WITH',
            'RECURSIVE', 'LATERAL', 'VIEW', 'TEMPORARY', 'TEMP', 'FUNCTION',
            'CAST', 'COALESCE', 'IF', 'NULLIF', 'GREATEST', 'LEAST',
            # 窗口函数相关
            'RANK', 'DENSE_RANK', 'ROW_NUMBER', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE',
            'NTH_VALUE', 'NTILE', 'CUME_DIST', 'PERCENT_RANK',
            # 条件表达式
            'IFNULL', 'NVL', 'NVL2', 'DECODE', 'IIF',
            # 其他SQL关键字
            'ASC', 'DESC', 'NULLS', 'FIRST', 'LAST', 'USING', 'NATURAL', 'BETWEEN',
            'EXISTS', 'LIKE', 'REGEXP', 'RLIKE', 'CONCAT', 'SUBSTRING', 'TRIM',
            'LENGTH', 'UPPER', 'LOWER', 'ROUND', 'FLOOR', 'CEIL', 'ABS', 'MOD',
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'STDDEV', 'VARIANCE'
        }

        filtered = []
        for table in tables:
            upper_table = table.upper()
            if upper_table not in sql_keywords:
                filtered.append(table)

        return filtered

    def is_cte(self, name):
        return name in self.cte_names

    def is_physical_table(self, name):
        return not self.is_cte(name)

    def extract_subquery_aliases(self, sql):
        """
        提取子查询别名及其对应SQL片段
        :param sql: SQL片段
        :return: dict: {alias: subquery_sql} 映射
        """
        if not sql:
            return {}

        result = {}

        # 匹配模式: FROM(SELECT ...) AS alias
        # 需要处理嵌套括号，支持换行
        pattern = re.compile(
            r'(?:FROM|JOIN|INTO)\s*\(\s*SELECT\s+',
            re.IGNORECASE | re.DOTALL
        )

        matches = list(pattern.finditer(sql))
        for match in matches:
            # 找到 (SELECT 的位置
            start = match.start()
            paren_start = sql.find('(', start)

            if paren_start == -1:
                continue

            # 找到匹配的结束括号
            end_paren = self._find_matching_paren(sql, paren_start)

            if end_paren is None:
                continue

            # 提取括号内的SQL
            subquery_sql = sql[paren_start + 1:end_paren]

            # 继续往后找 AS alias 或直接是alias
            after_paren = sql[end_paren + 1:].lstrip()

            # 匹配 AS alias 或直接是alias
            alias_match = re.match(r'(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', after_paren, re.IGNORECASE)

            if alias_match:
                alias = alias_match.group(1)
                result[alias] = subquery_sql

        return result

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

    def extract_cte_aliases(self, sql):
        """
        提取CTE别名及其对应CTE名称
        识别 FROM cte alias 模式
        :param sql: SQL片段
        :return: dict: {alias: cte_name} 映射
        """
        if not sql:
            return {}

        result = {}

        # 匹配模式： FROM cte_name AS alias 或 FROM cte_name alias
        # 需要排除已经匹配过的子查询

        # 首先找到所有子查询的结束位置
        subquery_ends = set()
        pattern = re.compile(r'\)\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*', re.IGNORECASE)
        for match in pattern.finditer(sql):
            end = match.end()
            subquery_ends.add(end)

        # 匹配 FROM/JOIN 后的表名（CTE或物理表）
        from_join_pattern = re.compile(
            r'(?:FROM|JOIN|INTO)\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            re.IGNORECASE
        )

        for match in from_join_pattern.finditer(sql):
            # 检查是否再子查询块之后（已处理过）
            if match.end() in subquery_ends:
                continue
            table_name = match.group(1)

            # 检查是否是CTE
            if self.is_cte(table_name):
                # 继续找别名
                after_table = sql[match.end():].lstrip()

                # 匹配 AS alias 或者 alias
                # 需要排除SQL关键字
                sql_keywords = {
                    'WHERE', 'ON', 'AND', 'OR', 'GROUP', 'ORDER', 'HAVING', 'LIMIT',
                    'UNION', 'SET', 'AS', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS',
                    'NULL', 'NOT', 'INTO', 'FROM', 'JOIN', 'LEFT', 'RIGHT', 'INNER',
                    'OUTER', 'FULL', 'CROSS', 'NATURAL', 'USING', 'PARTITION',
                    'DISTINCT', 'ALL', 'ANY', 'SOME', 'CASE', 'WHEN', 'THEN', 'ELSE',
                    'END', 'OVER', 'WINDOW', 'ROWS', 'RANGE', 'PRECEDING',
                    'FOLLOWING', 'CURRENT', 'UNBOUNDED', 'FIRST', 'LAST', 'NULLS',
                    'LATERAL', 'PIVOT', 'UNPIVOT', 'EXCEPT', 'INTERSECT', 'MINUS'
                }

                alias_match = re.match(r'(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', after_table, re.IGNORECASE)

                if alias_match:
                    alias = alias_match.group(1)
                    # 别名和CTE名称不同，且不是SQL关键字的，进行记录
                    if alias.upper() != table_name.upper() and alias.upper() not in sql_keywords:
                        result[alias] = table_name

        return result
