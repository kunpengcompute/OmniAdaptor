"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
              http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.

函数解析模块 - 负责从 Flink 物理计划中解析函数调用
"""
import re
import os
import json

from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger


class FlinkFunctionParser:
    """
    主要功能：
    1. 加载 flink_function_dictionary.json 函数字典
    2. 构建正则表达式用于匹配函数
    3. 解析 description 中的函数
    4. 判断函数的数据类型是否支持
    """

    def __init__(self):
        self.function_list = []
        self.omni_functions = []
        self.func_pattern = None
        self.keywords_pattern = None
        self.operator_pattern = None
        self.func_support_map = {}
        self.func_is_supported_types = {}
        self.load_func_list()

    def load_func_list(self):
        """加载函数字典配置并构建匹配模式"""
        base_path = os.path.join(CommonUtil.get_execute_path(), "resources")
        dictionary_path = os.path.join(base_path, "flink_function_dictionary.json")

        # 检查文件是否存在
        if not os.path.exists(dictionary_path):
            logger.warning(f"Flink function dictionary not found: {dictionary_path}")
            self.function_list = []
            self.omni_functions = []
            self.func_support_map = {}
            self.func_is_supported_types = {}
            self.func_pattern = None
            self.keywords_pattern = None
            self.operator_pattern = None
            return

        # 加载JSON文件
        try:
            with open(dictionary_path, "r", encoding="utf-8") as f:
                self.function_list = json.load(f)
            logger.info(f"Loaded {len(self.function_list)} functions from {dictionary_path}")
        except Exception as e:
            logger.warning(f"Failed to load {dictionary_path}: {e}")
            self.function_list = []
            self.omni_functions = []
            self.func_support_map = {}
            self.func_is_supported_types = {}
            self.func_pattern = None
            self.keywords_pattern = None
            self.operator_pattern = None
            return

        self.omni_functions = []
        self.func_support_map = {}
        self.func_is_supported_types = {}

        for func in self.function_list:
            func_name = func.get("func_name")
            if not func_name:
                continue
            func_name_lower = func_name.lower()
            self.omni_functions.append(func_name_lower)
            self.func_support_map[func_name_lower] = func.get("is_support_func", False)
            self.func_is_supported_types[func_name_lower] = func.get("is_supported_type", [])

        # 构建正则表达式模式
        self.build_patterns()

    def build_patterns(self):
        """
        构建三种正则表达式模式：
        1. func_pattern: 匹配函数调用形式 func(...)
        2. keyword_pattern: 匹配文字关键字（如 IS NULL, AND 等），使用 \\b 单词边界
        3. operator_pattern: 匹配符号运算符（如 *, <=, >= 等），不使用 \\b
        """
        if not self.omni_functions:
            logger.warning("No functions loaded, patterns are None")
            self.func_pattern = None
            self.keywords_pattern = None
            self.operator_pattern = None
            return

        keyword_keywords = {
            'or', 'and', 'like', 'between', 'case', 'array', 'map'
        }

        func_call_patterns = []
        keyword_patterns = []
        operator_patterns = []

        for func in self.omni_functions:
            is_keyword = False

            if ' ' in func:
                is_keyword = True
            elif len(func) <= 2 and not any(c.isalpha() for c in func):
                operator_patterns.append(func)
                continue
            elif func.lower() in keyword_keywords:
                is_keyword = True

            if is_keyword:
                keyword_patterns.append(func)
            else:
                func_call_patterns.append(func)

        if func_call_patterns:
            escaped_funcs = [re.escape(f) for f in func_call_patterns]
            self.func_pattern = re.compile(
                r"\b({})\s*\(".format("|".join(escaped_funcs)),
                re.I
            )
            logger.info(f"Created function call pattern for {len(func_call_patterns)} functions")
        else:
            self.func_pattern = None

        if keyword_patterns:
            escaped_keywords = [re.escape(f) for f in keyword_patterns]
            self.keywords_pattern = re.compile(
                r"\b({})\b".format("|".join(escaped_keywords)),
                re.I
            )
            logger.info(f"Created keywords pattern for {len(keyword_patterns)} keywords")
        else:
            self.keywords_pattern = None

        if operator_patterns:
            sorted_ops = sorted(operator_patterns, key=len, reverse=True)
            escaped_ops = [re.escape(f) for f in sorted_ops]
            self.operator_pattern = re.compile(
                r"({})".format("|".join(escaped_ops))
            )
            logger.info(f"Created operator pattern for {len(operator_patterns)} operators: {operator_patterns}")
        else:
            self.operator_pattern = None

    def is_func_type_supported(self, func_name, param_types):
        """
        判断函数在给定参数类型下是否支持

        :param func_name: 函数名（不区分大小写）
        :param param_types: 参数类型列表，如 ["INT", "VARCHAR"]
        :return: (is_supported, unsupported_types) - 是否支持，不支持的类型列表
        """
        func_name_lower = func_name.lower() if func_name else ""

        if func_name_lower not in self.func_support_map:
            return False, []

        if not self.func_support_map[func_name_lower]:
            return False, []

        is_supported_list = self.func_is_supported_types.get(func_name_lower, [])
        if not is_supported_list:
            return True, []

        unsupported_found = []
        for param_type in (param_types or []):
            normalized = TypeNormalizer.normalize_type(param_type)
            if normalized not in is_supported_list:
                unsupported_found.append(normalized)

        if unsupported_found:
            return False, unsupported_found

        return True, []

    @staticmethod
    def _strip_html_tags(description):
        if not description:
            return description
        cleaned = re.sub(r'<[^>]+>[\s:*\+\-]*', ' ', description)
        return cleaned

    @staticmethod
    def _is_operator_false_positive(op, match, description):
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
        return False

    def parse_plan_description(self, description):
        if not description:
            return []

        funcs = []

        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                func_name = match.group(1).lower()
                funcs.append({"func": func_name, "params": [], "type": "func"})

        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                keyword = match.group(1).lower()
                funcs.append({"func": keyword, "params": [], "type": "keyword"})

        if self.operator_pattern:
            clean_desc = self._strip_html_tags(description)
            for match in self.operator_pattern.finditer(clean_desc):
                op = match.group(1).lower()
                if self._is_operator_false_positive(op, match, clean_desc):
                    continue
                funcs.append({"func": op, "params": [], "type": "operator"})

        return funcs

    def analyze_unsupported_functions(self, description, param_types_map=None):
        """
        分析描述中的不支持函数
        返回 is_support_func=False 的函数，以及 is_support_func=True 但参数类型不支持的函数

        :param description: 算子描述字符串
        :param param_types_map: 可选，函数名→参数类型列表的映射，如 {"upper": ["VARCHAR"]}
        :return: 不支持的函数列表 [{"func_name": "xxx", "times": 1, "unsupported_types": [...]}, ...]
        """
        if not description:
            return []

        func_counter = {}
        func_unsupported_types = {}

        # 初始化列表，用于收集匹配结果（供调试日志使用）
        all_func_matches = []
        all_keyword_matches = []
        all_operator_matches = []

        # 匹配函数调用形式 func(...)
        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                all_func_matches.append(match)
                func_name = match.group(1).lower()
                self._check_func_support(
                    func_name, func_counter, func_unsupported_types, param_types_map
                )

        # 匹配关键字/操作符形式（如 IS NULL, AND, = 等）
        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                all_keyword_matches.append(match)
                keyword = match.group(1).lower()
                self._check_func_support(
                    keyword, func_counter, func_unsupported_types, param_types_map
                )

        clean_desc = self._strip_html_tags(description) if self.operator_pattern else description

        if self.operator_pattern:
            for match in self.operator_pattern.finditer(clean_desc):
                op = match.group(1).lower()
                if self._is_operator_false_positive(op, match, clean_desc):
                    continue
                all_operator_matches.append(match)
                self._check_func_support(
                    op, func_counter, func_unsupported_types, param_types_map
                )

        # 调试：打印解析到的所有函数（直接使用已收集的结果，无需重复遍历）
        all_matches = all_func_matches + all_keyword_matches + all_operator_matches
        if all_matches:
            matched_funcs = [m.group(1).lower() for m in all_matches]
            logger.info(f"从description解析到的所有函数: {matched_funcs}")

        # 转换为标准格式
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
        """
        检查单个函数是否支持，并更新计数器

        :param func_name: 函数名
        :param func_counter: 函数出现次数计数器
        :param func_unsupported_types: 函数不支持类型记录
        :param param_types_map: 函数名→参数类型列表的映射
        """
        if func_name not in self.func_support_map:
            return

        param_types = None
        if param_types_map:
            param_types = param_types_map.get(func_name)
        types_key = tuple(param_types) if param_types else ()
        counter_key = (func_name, types_key)

        if not self.func_support_map[func_name]:
            func_counter[counter_key] = func_counter.get(counter_key, 0) + 1
            return

        is_supported_list = self.func_is_supported_types.get(func_name, [])
        if not is_supported_list:
            logger.debug(f"函数 '{func_name}' 被过滤（is_support_func=True，无类型限制）")
            return

        if param_types:
            is_supported, unsupported = self.is_func_type_supported(func_name, param_types)
            if is_supported:
                logger.debug(f"函数 '{func_name}' 被过滤（is_support_func=True，参数类型均支持）")
            else:
                func_counter[counter_key] = func_counter.get(counter_key, 0) + 1
                func_unsupported_types[counter_key] = unsupported
        else:
            logger.debug(f"函数 '{func_name}' is_support_func=True，有类型限制但未提供参数类型，视为支持")