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
import json
import os

from omnihelper.enum.function_enum import FunctionEnum
from omnihelper.enum.type_enum import TypeEnum
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.func_util import replace_predicate_partition


class ReturnTypeParser:
    RETURN_TYPE_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "return_type_dictionary.json")

    def __init__(self):
        self.return_type_list = []
        self.type_handler_map = {
            "ARGUMENT_TYPE": self.handle_argument_type,
            "RESULT_TYPE": self.handle_result_type
        }
        self.load_return_type_list()

    def load_return_type_list(self):
        try:
            with open(self.RETURN_TYPE_PATH, "r", encoding="utf-8") as f:
                self.return_type_list = json.load(f)
        except Exception as e:
            raise Exception("Failed to load the function return type: " + str(e))

    def analyse_return_type(self, pair):
        func_name = pair.get("func")
        if not self.return_type_list:
            return None
        for func in self.return_type_list:
            if func.get("func_name") != func_name:
                continue
            if not func.get("need_param_type"):
                # 确定参数类型的，直接返回函数的参数类型
                return func.get("return_type")
            return self.type_handler_map.get(func["return_type"], lambda x: None)(pair)

    def handle_argument_type(self, pair):
        input_type = pair.get("input_type")
        if len(input_type) == 1:
            return input_type[0]
        if len(input_type) > 1:
            return_type = self.find_common_type_multi(input_type)
            return return_type
        return None

    def handle_result_type(self, pair):
        if pair.get("func") == FunctionEnum.CAST.value and len(pair.get("input_type")) == 2:
            return pair.get("input_type")[1]
        if pair.get("func") == FunctionEnum.IF.value:
            return replace_predicate_partition(pair.get("input_type"))[0]
        if pair.get("func") == FunctionEnum.CASE.value:
            return self.find_common_type_multi(pair.get("input_type"))
        return None

    def find_common_type_multi(self, types):
        if not types:
            return None

        result = types[0]
        for t in types[1:]:
            result = self.find_common_type(result, t)
            if result is None:
                return None
        return result

    def find_common_type(self, type1, type2):
        """ 查找两个 Spark SQL类型的公共类型"""

        # 1、相同类型
        if type1 == type2:
            return type1

        # 2、数值类型
        numeric_priority = {
            TypeEnum.BYTE.value: 1, TypeEnum.SHORT.value: 2, TypeEnum.INT.value: 3,
            TypeEnum.LONG.value: 4, TypeEnum.FLOAT.value: 5, TypeEnum.DOUBLE.value: 6,
            TypeEnum.DECIMAL64.value: 7, TypeEnum.DECIMAL128.value: 8
        }

        if type1 in numeric_priority and type2 in numeric_priority:
            p1 = numeric_priority[type1]
            p2 = numeric_priority[type2]
            # 特殊规则
            if type1 == TypeEnum.BYTE.value and type2 == TypeEnum.BYTE.value:
                return TypeEnum.SHORT.value
            if type1 == TypeEnum.SHORT.value and type2 == TypeEnum.SHORT.value:
                return TypeEnum.INT.value
            return type1 if p1 > p2 else type2

        # 3、String类型
        if TypeEnum.STRING.value in [type1, type2]:
            return TypeEnum.STRING.value

        # 4、日期时间类型
        datetime_priority = {TypeEnum.DATE.value: 1, TypeEnum.TIMESTAMP.value: 2}
        if type1 in datetime_priority and type2 in datetime_priority:
            p1 = datetime_priority[type1]
            p2 = datetime_priority[type2]
            return type1 if p1 > p2 else type2

        # 5、布尔类型
        if TypeEnum.BOOLEAN.value in [type1, type2]:
            return TypeEnum.STRING.value

        return None

