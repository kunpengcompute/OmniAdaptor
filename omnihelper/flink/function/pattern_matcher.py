# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# OmniAdaptor is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

"""
Flink 函数模式匹配模块

负责从描述文本中匹配函数、关键字和操作符
"""

import re

from omnihelper.util.log import logger


class FunctionPatternMatcher:
    """
    函数模式匹配器

    核心职责：
    1. 从描述文本中匹配函数调用、关键字和操作符
    2. 过滤误报场景
    3. 分析不支持的函数
    """

    def __init__(self, func_pattern, keywords_pattern, operator_pattern, support_checker):
        """
        初始化匹配器

        :param func_pattern: re.Pattern，函数调用匹配模式
        :param keywords_pattern: re.Pattern，关键字匹配模式
        :param operator_pattern: re.Pattern，操作符匹配模式
        :param support_checker: FunctionSupportChecker，支持性检查器
        """
        self.func_pattern = func_pattern
        self.keywords_pattern = keywords_pattern
        self.operator_pattern = operator_pattern
        self.support_checker = support_checker

    @staticmethod
    def _strip_html_tags(description):
        """去除 HTML 标签及其周围的空白和分隔符"""
        if not description:
            return description
        cleaned = re.sub(r'<[^>]+>[\s:*\+\-]*', ' ', description)
        return cleaned

    @staticmethod
    def _is_operator_false_positive(op, match, description):
        """判断操作符匹配是否为误报"""
        pos = match.start()
        op_end = match.end()

        if op == '=' and op_end < len(description) and description[op_end] == '[':
            return True

        if op == '=' and pos > 0 and description[pos - 1].isalpha():
            return True

        if op in ('=', '<', '>') and pos > 0 and description[pos - 1] in ('<', '>', '!'):
            return True
        if op in ('=', '<', '>') and op_end < len(description) and description[op_end] == '=':
            return True

        if op == '*' and pos > 0 and description[pos - 1] == '(' and op_end < len(description) and description[op_end] == ')':
            return True

        quote_count = 0
        for i in range(pos):
            if description[i] in ("'", '"'):
                quote_count += 1
        if quote_count % 2 == 1:
            return True

        return False

    @staticmethod
    def _is_operator_form(description, match_start):
        """判断函数名位置是否在操作符描述行中"""
        if match_start <= 3:
            return False

        prev_part = description[max(0, match_start - 10):match_start]
        op_pattern = re.compile(r'\[\d+\]:[A-Za-z]*$')
        return bool(op_pattern.search(prev_part))

    @staticmethod
    def _is_inside_sarg(description, pos):
        """判断指定位置是否在 Sarg[...] 内部"""
        sarg_count = 0
        i = 0
        while i < pos:
            if i + 4 <= len(description) and description[i:i+4].upper() == 'SARG':
                if i + 5 <= len(description) and description[i+4] == '[':
                    sarg_count += 1
                    i += 5
                    continue
            if description[i] == ']':
                sarg_count -= 1
            i += 1
        return sarg_count > 0

    def parse_plan_description(self, description):
        """解析计划描述中的所有函数"""
        if not description:
            return []

        funcs = []

        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                func_name = match.group(1).lower()
                match_start = match.start()
                if self._is_operator_form(description, match_start):
                    continue
                if self._is_inside_sarg(description, match_start):
                    continue
                funcs.append({"func": func_name, "params": [], "type": "func"})

        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                match_start = match.start()
                if self._is_inside_sarg(description, match_start):
                    continue
                keyword = match.group(1).lower()
                funcs.append({"func": keyword, "params": [], "type": "keyword"})

        if self.operator_pattern:
            clean_desc = self._strip_html_tags(description)
            for match in self.operator_pattern.finditer(clean_desc):
                op = match.group(1).lower()
                match_start = match.start()
                if self._is_operator_false_positive(op, match, clean_desc):
                    continue
                if self._is_inside_sarg(clean_desc, match_start):
                    continue
                if match_start > 0 and (clean_desc[match_start - 1].isalnum() or clean_desc[match_start - 1] == '_'):
                    continue
                funcs.append({"func": op, "params": [], "type": "operator"})

        return funcs

    def analyze_unsupported_functions(self, description, param_types_map=None):
        """分析描述中的不支持函数"""
        if not description:
            return []

        func_counter = {}
        func_unsupported_types = {}

        all_func_matches = []
        all_keyword_matches = []
        all_operator_matches = []

        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                func_name = match.group(1).lower()
                match_start = match.start()
                if self._is_operator_form(description, match_start):
                    continue
                if self._is_inside_sarg(description, match_start):
                    continue
                all_func_matches.append(match)
                self._check_func_support(
                    func_name, func_counter, func_unsupported_types, param_types_map
                )

        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                match_start = match.start()
                if self._is_inside_sarg(description, match_start):
                    continue
                all_keyword_matches.append(match)
                keyword = match.group(1).lower()
                self._check_func_support(
                    keyword, func_counter, func_unsupported_types, param_types_map
                )

        clean_desc = self._strip_html_tags(description) if self.operator_pattern else description
        if self.operator_pattern:
            for match in self.operator_pattern.finditer(clean_desc):
                op = match.group(1).lower()
                match_start = match.start()
                if self._is_operator_false_positive(op, match, clean_desc):
                    continue
                if self._is_inside_sarg(clean_desc, match_start):
                    continue
                if match_start > 0 and (clean_desc[match_start - 1].isalnum() or clean_desc[match_start - 1] == '_'):
                    continue
                self._check_func_support(
                    op, func_counter, func_unsupported_types, param_types_map
                )
                all_operator_matches.append(match)

        all_matches = all_func_matches + all_keyword_matches + all_operator_matches
        if all_matches:
            matched_funcs = [m.group(1).lower() for m in all_matches]
            logger.info(f"从description解析到的所有函数: {matched_funcs}")

        results = []
        for key, times in func_counter.items():
            func_name = key[0] if isinstance(key, tuple) else key
            result = {
                "func_name": func_name,
                "times": times
            }
            unsupported = func_unsupported_types.get(key)
            if unsupported:
                result["unsupported_types"] = unsupported
            results.append(result)

        return results

    def _check_func_support(self, func_name, func_counter, func_unsupported_types, param_types_map):
        """检查单个函数是否支持，并更新计数器"""
        if func_name not in self.support_checker.func_support_map:
            return

        param_types = None
        if param_types_map:
            for key in param_types_map:
                if isinstance(key, tuple) and len(key) >= 2 and key[0] == func_name:
                    param_types = param_types_map[key]
                    break
            if param_types is None:
                param_types = param_types_map.get(func_name)

        if func_name == "cast" and self.support_checker.cast_is_support_type:
            if param_types:
                is_supported, unsupported = self.support_checker.check_cast_function(param_types)
                if not is_supported:
                    types_key = tuple(param_types)
                    counter_key = (func_name, types_key)
                    func_counter[counter_key] = func_counter.get(counter_key, 0) + 1
                    func_unsupported_types[counter_key] = unsupported
            return

        types_key = tuple(param_types) if param_types else ()
        counter_key = (func_name, types_key)

        if not self.support_checker.func_support_map[func_name]:
            func_counter[counter_key] = func_counter.get(counter_key, 0) + 1
            return

        is_supported_list = self.support_checker.func_is_supported_types.get(func_name, [])

        if not is_supported_list:
            logger.debug(f"函数 '{func_name}' 被过滤（is_support_func=True，无类型限制）")
            return

        if param_types:
            is_supported, unsupported = self.support_checker.is_func_type_supported(func_name, param_types)
            if is_supported:
                logger.debug(f"函数 '{func_name}' 被过滤（is_support_func=True，参数类型均支持）")
            else:
                func_counter[counter_key] = func_counter.get(counter_key, 0) + 1
                func_unsupported_types[counter_key] = unsupported
        else:
            logger.debug(f"函数 '{func_name}' is_support_func=True，有类型限制但未提供参数类型，视为支持")
