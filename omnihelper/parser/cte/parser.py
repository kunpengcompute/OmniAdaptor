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

from omnihelper.parser.cte.cte_extractor import CTEExtractor
from omnihelper.parser.cte.table_extractor import TableExtractor
from omnihelper.parser.cte.tracer import RecursiveTracer


class CTEParser:
    """
    Spark SQL CTE解析器
    用于解析WITH语句中的CTE定义，并递归追踪每个CTE到底层物理表的映射关系
    """

    def __init__(self):
        self.cte_extractor = CTEExtractor()
        self.table_extractor = TableExtractor()
        self.tracer = None

        self._cte_definitions = {}
        self._parse_result = {}

    def parse(self, sql):
        """
        解析SQL并返回CTE到物理表的映射
        :param sql: SPARK SQL
        :return: dict: {cte_name: [phy_tables]} 映射
        """
        if not sql:
            return {}

        # 1. 提取CTE定义
        self._cte_definitions = self.cte_extractor.extract(sql)

        # 2. 设置表提取器的CTE名称
        self.table_extractor.set_cte_names(self._cte_definitions.keys())

        # 3. 创建递归追踪器（必须在解析子查询别名和CTE别名之前创建）
        self.tracer = RecursiveTracer(self._cte_definitions)

        # 4. 解析主查询中的子查询别名和CTE别名
        main_query = self._extract_main_query(sql)

        # 解析子查询别名
        subquery_aliases = self.parse_subquery_aliases(main_query)

        # 解析CTE别名
        cte_aliases = self.parse_cte_aliases(main_query)

        # 如果没有CTE定义，直接返回子查询别名和CTE别名结果
        if not self._cte_definitions:
            result = {}
            result.update(subquery_aliases)
            result.update(cte_aliases)
            return result

        # 5. 追踪所有CTE到底层物理表
        self._parse_result = self.tracer.trace_all()

        # 6. 合并结果
        result = dict(self._parse_result)
        result.update(subquery_aliases)
        result.update(cte_aliases)

        return result

    def _extract_main_query(self, sql):
        """提取主查询"""
        # 查找WITH关键字
        with_match = re.search(r'\bWITH\b', sql, re.IGNORECASE)
        if not with_match:
            return sql

        # 找到所有CTE定义结束的位置
        # 从WITH之后开始解析，找到最后一个CTE的结束位置
        cte_definitions = self.cte_extractor.extract(sql)

        if not cte_definitions:
            return sql[with_match.end():]

        # 找到最后一个CTE的结束位置
        main_query_match = re.search(
            r'\b(SELECT|INSERT|UPDATE|DELETE)\b',
            sql[with_match.end():],
            re.IGNORECASE
        )

        if main_query_match:
            return sql[with_match.end() + main_query_match.start():]

        return sql[with_match.end():]

    def parse_subquery_aliases(self, sql):
        """
        解析子查询别名到底层物理表
        :param sql: SQL片段
        :return: dict: {alias: [phy_tables]}映射
        """
        if not sql:
            return {}

        # 设置CTE名称
        self.table_extractor.set_cte_names(self._cte_definitions.keys())

        # 提取子查询别名
        subquery_aliases = self.table_extractor.extract_subquery_aliases(sql)

        if not subquery_aliases:
            return {}

        # 追踪每个子查询别名到底层物理表
        result = {}

        for alias, subquery_sql in subquery_aliases.items():
            # 提取子查询中的表
            tables = self.table_extractor.extract_tables(subquery_sql)

            # 追踪每个表到底层物理表
            physical_tables = set()

            for table in tables:
                if table in self._cte_definitions:
                    # 是CTE 递归追踪
                    if self.tracer:
                        traced = self.tracer.trace(table)
                        physical_tables.update(traced)
                    else:
                        physical_tables.add(table)
                else:
                    # 是物理表
                    physical_tables.add(table)

            if physical_tables:
                result[alias] = list(physical_tables)

        return result

    def parse_cte_aliases(self, sql):
        """
        解析CTE别名到底层物理表
        :param sql: SQL片段
        :return: dict: {alias: [phy_tables]}映射
        """
        if not sql:
            return {}

        # 设置CTE名称
        self.table_extractor.set_cte_names(self._cte_definitions.keys())

        # 提取CTE名称
        cte_aliases = self.table_extractor.extract_cte_aliases(sql)

        if not cte_aliases:
            return {}

        # 追踪每个CTE别名到底层物理表
        result = {}

        for alias, cte_name in cte_aliases.items():
            # 追踪CTE到底层物理表
            if self.tracer:
                physical_tables = self.tracer.trace(cte_name)
            else:
                # 没有tracer，使用CTE定义追踪
                tracer = RecursiveTracer(self._cte_definitions)
                physical_tables = tracer.trace(cte_name)

            if physical_tables:
                result[alias] = physical_tables

        return result
