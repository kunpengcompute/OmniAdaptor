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

from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger


class FlinkFunctionParser:
    """
    主要功能：
    1. 加载 flink_function_dictionary.json 函数字典
    2. 构建正则表达式用于匹配函数
    3. 解析 description 中的函数
    """

    def __init__(self):
        self.function_list = []  # 存储字典文件中的所有函数对象列表
        self.omni_functions = []  # 仅存储函数名（字符串）的列表
        self.func_pattern = None  # 正则：匹配函数调用 func(...)
        self.keywords_pattern = None  # 正则：匹配关键字/操作符
        self.func_support_map = {}  # 字典 {函数名: is_support_func}
        self.load_func_list()  # 构造函数实例后立即加载字典

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
            return

        # 1. 初始化两个空容器
        self.omni_functions = []  # 用于存储所有函数名（列表）
        self.func_support_map = {}  # 用于存储函数名 → 是否支持的映射（字典）

        # 2. 遍历函数列表
        for func in self.function_list:
            # 3. 获取当前函数的名称
            func_name = func.get("func_name")
            # 4. 如果函数名为空，跳过这条记录
            if not func_name:
                continue
            # 5. 转成小写（实现大小写不敏感匹配）
            func_name_lower = func_name.lower()
            # 6. 将小写函数名加入列表
            self.omni_functions.append(func_name_lower)
            # 7. 将函数名和是否支持的信息存入字典
            self.func_support_map[func_name_lower] = func.get("is_support_func", False)

        # 构建正则表达式模式
        self.build_patterns()

    def build_patterns(self):
        """
        构建两种正则表达式模式：
        1. func_pattern: 匹配函数调用形式 func(...)
        2. keywords_pattern: 匹配关键字/操作符形式（如 IS NULL, AND, =, + 等）
        """
        if not self.omni_functions:
            logger.warning("No functions loaded, patterns are None")
            self.func_pattern = None
            self.keywords_pattern = None
            return

        # 定义应该被识别为关键字/操作符的判断规则
        keyword_keywords = {
            'or', 'and', 'like', 'between' ,'case' ,'array','map'
        }

        # 分类函数
        func_call_patterns = []  # 函数调用型：func(
        keyword_patterns = []  # 关键字/操作符型：IS NULL, AND, =, +

        for func in self.omni_functions:
            is_keyword = False

            # 如果包含空格，认为是关键字（如 "IS NULL", "NOT BETWEEN"）
            if ' ' in func:
                is_keyword = True
            # 如果是纯符号操作符（如 =, <>, >=, <=, +, -, *, /, ||, %）
            elif len(func) <= 2 and not any(c.isalpha() for c in func):
                is_keyword = True
            # 如果函数名在我们的关键字列表中，也识别为关键字
            elif func.lower() in keyword_keywords:
                is_keyword = True

            if is_keyword:
                keyword_patterns.append(func)
            else:
                func_call_patterns.append(func)

        # 构建函数调用模式: 匹配 func( 形式
        if func_call_patterns:
            escaped_funcs = [re.escape(f) for f in func_call_patterns]
            self.func_pattern = re.compile(
                 r"\b({})\s*\(".format("|".join(escaped_funcs)),
                re.I
            )
            logger.info(f"Created function call pattern for {len(func_call_patterns)} functions")
        else:
            self.func_pattern = None

        # 构建关键字/操作符模式: 匹配独立的关键字
        if keyword_patterns:
            escaped_keywords = [re.escape(f) for f in keyword_patterns]
            self.keywords_pattern = re.compile(
                r"\b({})\b".format("|".join(escaped_keywords)),
                re.I
            )
            logger.info(f"Created keywords pattern for {len(keyword_patterns)} keywords")
        else:
            self.keywords_pattern = None

    def parse_plan_description(self, description):
        """
        解析算子描述，提取所有函数

        :param description: 算子描述字符串，如 "Calc(select=UPPER(name), LOWER(text))"
        :return: 函数列表 [{"func": "upper", "params": [], "type": "func"}, ...]
        """
        if not description:
            return []

        funcs = []

        # 匹配函数调用形式 func(...)
        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                func_name = match.group(1).lower()
                funcs.append({"func": func_name, "params": [], "type": "func"})

        # 匹配关键字/操作符形式（如 IS NULL, AND, = 等）
        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                keyword = match.group(1).lower()
                funcs.append({"func": keyword, "params": [], "type": "keyword"})

        return funcs

    def analyze_unsupported_functions(self, description):
        """
        分析描述中的不支持函数
        只返回 is_support_func=False 的函数

        :param description: 算子描述字符串
        :return: 不支持的函数列表 [{"func_name": "xxx", "times": 1}, ...]
        """
        if not description:
            return []

        func_counter = {}

        # 匹配函数调用形式 func(...)
        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                func_name = match.group(1).lower()
                if func_name in self.func_support_map and not self.func_support_map[func_name]:
                    func_counter[func_name] = func_counter.get(func_name, 0) + 1
                else:
                    if func_name in self.func_support_map:
                        logger.debug(f"函数 '{func_name}' 被过滤（is_support_func=True）")

        # 匹配关键字/操作符形式（如 IS NULL, AND, = 等）
        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                keyword = match.group(1).lower()
                if keyword in self.func_support_map and not self.func_support_map[keyword]:
                    func_counter[keyword] = func_counter.get(keyword, 0) + 1
                else:
                    if keyword in self.func_support_map:
                        logger.debug(f"关键字 '{keyword}' 被过滤（is_support_func=True）")

        # 调试：打印解析到的所有函数
        all_func_matches = list(self.func_pattern.finditer(description)) if self.func_pattern else []
        all_keyword_matches = list(self.keywords_pattern.finditer(description)) if self.keywords_pattern else []
        all_matches = all_func_matches + all_keyword_matches
        if all_matches:
            matched_funcs = [m.group(1).lower() for m in all_matches]
            logger.info(f"从description解析到的所有函数: {matched_funcs}")

        # 转换为标准格式
        results = []
        for func_name, times in func_counter.items():
            results.append({
                "func_name": func_name,
                "times": times
            })

        return results