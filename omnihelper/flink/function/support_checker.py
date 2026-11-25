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
Flink 函数支持性检查模块

负责判断函数及其参数类型是否支持
"""

from omnihelper.flink.schema.type_normalizer import TypeNormalizer


class FunctionSupportChecker:
    """
    函数支持性检查器

    核心职责：
    1. 判断函数是否支持
    2. 检查函数参数类型是否兼容
    3. 校验 CAST 函数的类型转换
    """

    def __init__(self, func_support_map, func_is_supported_types, cast_is_support_type):
        """
        初始化检查器

        :param func_support_map: dict，函数支持状态映射
        :param func_is_supported_types: dict，函数支持类型列表映射
        :param cast_is_support_type: dict，CAST 函数类型转换白名单
        """
        self.func_support_map = func_support_map
        self.func_is_supported_types = func_is_supported_types
        self.cast_is_support_type = cast_is_support_type

    def check_cast_function(self, input_type):
        """
        校验 CAST 函数的源类型和目标类型是否支持（白名单机制）

        :param input_type: list，包含源类型和目标类型的列表
        :return: tuple，(is_supported, unsupported_types)
        """
        if len(input_type) < 2:
            return False, ["Invalid input type"]

        source_type = input_type[0]
        target_type = input_type[1]

        # 对类型进行归一化处理
        normalized_source = TypeNormalizer.normalize_type(source_type)
        normalized_target = TypeNormalizer.normalize_type(target_type)

        if not self.cast_is_support_type:
            return True, []

        if normalized_source not in self.cast_is_support_type:
            return False, [f"{source_type} -> {target_type} (source type not supported)"]

        supported_targets = self.cast_is_support_type.get(normalized_source, [])

        if normalized_target in supported_targets:
            return True, []
        else:
            return False, [f"{source_type} -> {target_type} (target type not supported)"]

    def is_func_type_supported(self, func_name, param_types):
        """
        判断函数在给定参数类型下是否支持

        :param func_name: str，函数名
        :param param_types: list，参数类型列表
        :return: tuple，(is_supported, unsupported_types)
        """
        func_name_lower = func_name.lower() if func_name else ""

        if func_name_lower not in self.func_support_map:
            return False, []

        if not self.func_support_map[func_name_lower]:
            return False, []

        # CAST 函数使用白名单机制
        if func_name_lower == "cast" and self.cast_is_support_type:
            if param_types:
                return self.check_cast_function(param_types)
            return True, []

        is_supported_list = self.func_is_supported_types.get(func_name_lower, [])

        if not is_supported_list:
            return True, []

        unsupported_found = []
        for param_type in (param_types or []):
            if param_type not in is_supported_list:
                normalized = TypeNormalizer.normalize_type(param_type)
                if normalized not in is_supported_list:
                    unsupported_found.append(param_type)

        if unsupported_found:
            return False, unsupported_found

        return True, []
