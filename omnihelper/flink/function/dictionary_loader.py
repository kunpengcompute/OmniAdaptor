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
Flink 函数字典加载模块

负责加载函数字典配置并构建正则表达式匹配模式
"""

import re
import os
import json

from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger


class FunctionDictionaryLoader:
    """
    函数字典加载器

    核心职责：
    1. 加载函数字典配置文件
    2. 构建函数支持性映射
    3. 构建三类正则表达式匹配模式（函数调用、关键字、操作符）
    """

    def __init__(self):
        """初始化加载器"""
        self.function_list = []
        self.omni_functions = []
        self.func_support_map = {}
        self.func_is_supported_types = {}
        self.cast_is_support_type = {}
        self.func_pattern = None
        self.keywords_pattern = None
        self.operator_pattern = None

    def load(self):
        """
        加载函数字典配置并构建匹配模式

        :return: dict，包含所有加载的数据和模式
        """
        base_path = os.path.join(CommonUtil.get_execute_path(), "resources")
        dictionary_path = os.path.join(base_path, "flink_function_dictionary.json")

        if not os.path.exists(dictionary_path):
            logger.warning(f"Flink function dictionary not found: {dictionary_path}")
            return self._get_empty_result()

        try:
            with open(dictionary_path, "r", encoding="utf-8") as f:
                self.function_list = json.load(f)
            logger.info(f"Loaded {len(self.function_list)} functions from {dictionary_path}")
        except Exception as e:
            logger.warning(f"Failed to load {dictionary_path}: {e}")
            return self._get_empty_result()

        self._build_mappings()
        self._build_patterns()

        return self._get_result()

    def _get_empty_result(self):
        """返回空结果"""
        return {
            "function_list": [],
            "omni_functions": [],
            "func_support_map": {},
            "func_is_supported_types": {},
            "cast_is_support_type": {},
            "func_pattern": None,
            "keywords_pattern": None,
            "operator_pattern": None,
        }

    def _get_result(self):
        """返回加载结果"""
        return {
            "function_list": self.function_list,
            "omni_functions": self.omni_functions,
            "func_support_map": self.func_support_map,
            "func_is_supported_types": self.func_is_supported_types,
            "cast_is_support_type": self.cast_is_support_type,
            "func_pattern": self.func_pattern,
            "keywords_pattern": self.keywords_pattern,
            "operator_pattern": self.operator_pattern,
        }

    def _build_mappings(self):
        """构建函数映射关系"""
        self.omni_functions = []
        self.func_support_map = {}
        self.func_is_supported_types = {}
        self.cast_is_support_type = {}

        for func in self.function_list:
            func_name = func.get("func_name")
            if not func_name:
                continue

            func_name_lower = func_name.lower()
            self.omni_functions.append(func_name_lower)
            self.func_support_map[func_name_lower] = func.get("is_support_func", False)
            self.func_is_supported_types[func_name_lower] = func.get("is_supported_type", [])

            if func_name_lower == "cast" and func.get("cast_is_support_type"):
                self.cast_is_support_type = func.get("cast_is_support_type")

    def _build_patterns(self):
        """构建三种正则表达式匹配模式"""
        if not self.omni_functions:
            logger.warning("No functions loaded, patterns are None")
            self.func_pattern = None
            self.keywords_pattern = None
            self.operator_pattern = None
            return

        keyword_keywords = {
            'or', 'and', 'like', 'between', 'not', 'row', 'date', 'time', 'interval', 'localtime',
            'localtimestamp', 'current_time', 'current_date', 'current_timestamp', 'array', 'map'
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
                r"({})".format("|".join(escaped_ops)),
                re.I
            )
            logger.info(f"Created operator pattern for {len(operator_patterns)} operators: {operator_patterns}")
        else:
            self.operator_pattern = None
