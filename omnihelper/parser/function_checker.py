"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.
"""
from enum import Enum

from omnihelper.enum.function_enum import FunctionEnum
from omnihelper.parser.type_matcher import TypeEnum, TypeMatcher, NOT_SUPPORTED_TYPE


class TypeLimitEnum(Enum):
    NO_SUPPORT_TYPE = "no_support_type"
    STRING_LITERAL = "string_literal"
    DATE_LITERAL = "date_literal"
    INT_IDENTIFIER = "int_identifier"

class FunctionChecker:

    def __init__(self, function_list):
        self.function_list = function_list
        self.current_rule = None

    def check_support_status(self, func_name, params, input_type, ori_sql):
        """
        检查函数或者表达式的omni支持性
        :return: true:不支持, false:支持
        """
        for rule in self.function_list:
            if not rule.get("func_name").lower() == func_name.lower():
                continue
            self.current_rule = rule
            if not rule.get("is_support_func"):
                # 函数本身不支持
                return True
            if rule.get("param_count") and len(params) != rule.get("param_count"):
                # 函数个数不支持
                return True
            if func_name.lower() == FunctionEnum.CAST.value:
                return self.check_cast_function(input_type)
            if rule.get("param_type_limit"):
                # 有参数指定位类型的限制
                return self.check_param_type_limit(params, input_type, ori_sql)
            if func_name.lower() in [FunctionEnum.FROM_UNIXTIME.value, FunctionEnum.UNIX_TIMESTAMP.value]:
                # 时间函数第三个参数需要限制内容格式
                return self.check_time_param(params)
            if func_name.lower() == FunctionEnum.LIKE.value:
                # like函数第二个函数的内容不能包含"_"以及多个"%"
                return self.check_like_param(params)
            for param_type in input_type:
                # 通用参数类型检查
                if self.is_not_supported_type(param_type):
                    return True
        return False

    def check_cast_function(self, input_type):
        """
        校验cast函数的源和目标类型是否支持
        :return: true: 参数不支持，false: 参数支持
        """
        source_type = input_type[0]
        target_type = input_type[1]
        not_support_type = self.current_rule.get("cast_no_support_type")
        if not not_support_type:
            return False
        if source_type in NOT_SUPPORTED_TYPE or target_type in not_support_type.get(source_type, []) + NOT_SUPPORTED_TYPE:
            return True
        return False

    def is_not_supported_type(self, param_type):
        if param_type in self.current_rule.get("no_support_type", []) + NOT_SUPPORTED_TYPE:
            return True
        return False

    def check_param_type_limit(self, params, input_type, ori_sql):
        """
        校验参数指定位类型的限制
        :return: true: 参数不支持，false: 参数支持
        """
        for key, value in self.current_rule["param_type_limit"].items():
            idx = int(key)
            if idx >= len(input_type):
                return False
            if value == TypeLimitEnum.NO_SUPPORT_TYPE.value:
                if self.is_not_supported_type(input_type[idx]):
                    return True
            if value == TypeLimitEnum.STRING_LITERAL.value:
                if not TypeMatcher.is_string_literal(params[idx]) and not TypeMatcher.is_string_in_ori_sql(params[idx], ori_sql):
                    return True
            if value == TypeLimitEnum.DATE_LITERAL.value:
                if not TypeMatcher.is_date_literal(params[idx]):
                    return True
            if value == TypeLimitEnum.INT_IDENTIFIER.value:
                if not input_type[idx] == TypeEnum.INT.value:
                    return True
        return False

    def check_time_param(self, params):
        """
        时间函数第三个参数的内容需要限定时区
        :return: true:不支持, false:支持
        """
        if not len(params) >= 3:
            return False
        third_param = params[2]
        time_zones = ["GMT+08:00", "Asia/Shanghai", "Asia/Beijing"]
        if not any(time_zone in third_param for time_zone in time_zones):
            return True
        return False

    def check_like_param(self, params):
        """
        校验like函数的第二个参数是否包含_及多个%，包含或者有多个表示不支持
        :return: true:不支持, false:支持
        """
        return "_" in params[1] or params[1].count("%") > 1
