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
Flink 函数解析模块


模块功能说明:
    本模块负责从 Flink 物理计划描述中解析函数调用信息，支持三种类型的函数识别:
    1. 函数调用形式: func_name(...) - 如 UPPER(col), CAST(... AS INT)
    2. 关键字形式: 关键字如 AND, OR, IS NULL, LIKE 等
    3. 操作符形式: 符号如 =, <>, >=, <=, *, + 等

    核心功能包括:
    - 加载和维护 Flink 函数字典 (flink_function_dictionary.json)
    - 构建正则表达式模式用于函数匹配
    - 解析算子描述中的函数调用
    - 判断函数是否支持以及参数类型是否兼容

设计理念:
    采用策略模式将函数匹配分为三个独立的正则模式，便于扩展和维护。
    通过类型归一化和函数支持映射，实现对函数兼容性的快速判断。
"""

from omnihelper.flink.function.dictionary_loader import FunctionDictionaryLoader
from omnihelper.flink.function.support_checker import FunctionSupportChecker
from omnihelper.flink.function.pattern_matcher import FunctionPatternMatcher


class FlinkFunctionParser:
    """
    Flink 函数解析器 - 核心类

    协调各子模块完成函数解析：
    1. 加载函数字典
    2. 构建匹配模式
    3. 检查函数支持性
    4. 匹配和分析函数
    """

    def __init__(self):
        """初始化函数解析器"""
        loader = FunctionDictionaryLoader()
        data = loader.load()

        self.function_list = data["function_list"]
        self.omni_functions = data["omni_functions"]
        self.func_support_map = data["func_support_map"]
        self.func_is_supported_types = data["func_is_supported_types"]
        self.cast_is_support_type = data["cast_is_support_type"]
        self.func_pattern = data["func_pattern"]
        self.keywords_pattern = data["keywords_pattern"]
        self.operator_pattern = data["operator_pattern"]

        support_checker = FunctionSupportChecker(
            self.func_support_map,
            self.func_is_supported_types,
            self.cast_is_support_type
        )

        self.pattern_matcher = FunctionPatternMatcher(
            self.func_pattern,
            self.keywords_pattern,
            self.operator_pattern,
            support_checker
        )

        self.support_checker = support_checker

    def check_cast_function(self, input_type):
        """校验 CAST 函数的源类型和目标类型是否支持"""
        return self.support_checker.check_cast_function(input_type)

    def is_func_type_supported(self, func_name, param_types):
        """判断函数在给定参数类型下是否支持"""
        return self.support_checker.is_func_type_supported(func_name, param_types)

    def parse_plan_description(self, description):
        """解析计划描述中的所有函数"""
        return self.pattern_matcher.parse_plan_description(description)

    def analyze_unsupported_functions(self, description, param_types_map=None):
        """分析描述中的不支持函数"""
        return self.pattern_matcher.analyze_unsupported_functions(description, param_types_map)
