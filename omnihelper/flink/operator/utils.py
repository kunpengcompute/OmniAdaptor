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
Flink 算子解析工具模块

提供通用的工具方法，避免循环依赖
"""


class ExpressionConverter:
    """
    表达式转换器

    负责将表达式对象转换为字符串表示
    """

    @staticmethod
    def expr_to_string(expr):
        """
        将表达式对象转换为字符串表示

        :param expr: dict 或 str，表达式对象
        :return: str，表达式字符串

        支持的表达式类型:
        - FIELD_REFERENCE: field[列名]
        - LITERAL: 字面量值
        - FUNCTION: 函数名(参数)
        - BINARY: 左 操作符 右
        - UNARY: 操作符(表达式)
        """
        if isinstance(expr, str):
            return expr
        if isinstance(expr, dict):
            expr_type = expr.get("exprType", "")
            if expr_type == "FIELD_REFERENCE":
                return f"field[{expr.get('colVal', '?')}]"
            if expr_type == "LITERAL":
                return str(expr.get("value", "?"))
            if expr_type == "FUNCTION":
                name = expr.get("function_name", "?")
                args = expr.get("arguments", [])
                args_str = ", ".join(ExpressionConverter.expr_to_string(a) for a in args)
                return f"{name}({args_str})"
            if expr_type == "BINARY":
                op = expr.get("operator", "?")
                left = ExpressionConverter.expr_to_string(expr.get("left", {}))
                right = ExpressionConverter.expr_to_string(expr.get("right", {}))
                return f"{left} {op} {right}"
            if expr_type == "UNARY":
                op = expr.get("operator", "?")
                inner = ExpressionConverter.expr_to_string(expr.get("expr", {}))
                return f"{op}({inner})"
        return str(expr)
