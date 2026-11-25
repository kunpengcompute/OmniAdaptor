# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# OmniAdaptor is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

from omnihelper.parser.cte.table_extractor import TableExtractor


class RecursiveTracer:
    """递归追踪CTE到底层物理表"""

    def __init__(self, cte_definitions):
        """
        初始化追踪器
        :param cte_definitions: dict, {cte_name: cte_sql} 映射
        """
        self.cte_definitions = cte_definitions
        self.table_extractor = TableExtractor()
        self.table_extractor.set_cte_names(cte_definitions.keys())

        # 缓存追踪结果
        self.trace_cache = {}

    def trace(self, cte_name, visited=None):
        """
        递归追踪到底层物理表
        :param cte_name: CTE名称
        :param visited: 已访问的CTE集合
        :return: list: 物理表名称列表
        """
        if visited is None:
            visited = set()

        # 检查缓存
        if cte_name in self.trace_cache:
            return self.trace_cache[cte_name]

        # 检查是否是CTE
        if cte_name not in self.cte_definitions:
            return [cte_name]

        # 检查循环引用
        if cte_name in visited:
            return []

        visited.add(cte_name)

        # 提取当前CTE中引用的表
        cte_sql = self.cte_definitions[cte_name]
        referenced_tables = self.table_extractor.extract_tables(cte_sql)

        # 递归追踪每个引用的表
        result = set()
        for table in referenced_tables:
            if table in self.cte_definitions:
                sub_tables = self.trace(table, visited.copy())
                result.update(sub_tables)
            else:
                result.add(table)

        # 缓存结果
        result_list = list(result)
        self.trace_cache[cte_name] = result_list

        return result_list

    def trace_all(self):
        """
        追踪所有CTE到底层物理表
        :return: dict: {cte: [phy_tables]} 映射
        """
        result = {}
        for cte_name in self.cte_definitions:
            result[cte_name] = self.trace(cte_name)
        return result

    def _has_cycle(self, cte_name, visited):
        """检测是否存在循环引用"""
        if cte_name in visited:
            return True

        if cte_name not in self.cte_definitions:
            return False

        visited.add(cte_name)

        cte_sql = self.cte_definitions[cte_name]
        referenced_tables = self.table_extractor.extract_tables(cte_sql)

        for table in referenced_tables:
            if table in self.cte_definitions:
                if self._has_cycle(table, visited.copy()):
                    return True
        return False
