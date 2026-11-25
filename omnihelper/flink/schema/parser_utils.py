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
Flink 表达式解析工具函数
"""

import re


def extract_function_args(expr_str):
    """
    提取函数参数文本（处理嵌套括号）

    :param expr_str: 函数调用表达式（如 "upper(name)"）
    :return: 括号内的参数文本，无法提取返回 None
    """
    start = expr_str.find("(")
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(expr_str)):
        if expr_str[i] == "(":
            depth += 1
        elif expr_str[i] == ")":
            depth -= 1
            if depth == 0:
                return expr_str[start + 1: i]

    return expr_str[start + 1:]


def split_function_args(args_str):
    """
    按逗号分割函数参数（考虑嵌套括号）

    :param args_str: 函数参数文本（不含括号）
    :return: 参数列表
    """
    if not args_str:
        return []

    parts = []
    depth = 0
    current = []

    for ch in args_str:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)

    if current:
        parts.append("".join(current))

    return parts


def split_select_items(text):
    """
    分割 SELECT 项目列表（考虑嵌套括号和字符串）

    :param text: 逗号分隔的项目列表
    :return: 分割后的项目列表
    """
    items = []
    current = []
    depth = 0
    in_string = False
    string_char = None

    for char in text:
        if not in_string and char in ("'", '"'):
            in_string = True
            string_char = char
            current.append(char)
        elif in_string and char == string_char:
            in_string = False
            string_char = None
            current.append(char)
        elif not in_string and char == "(":
            depth += 1
            current.append(char)
        elif not in_string and char == ")":
            depth -= 1
            current.append(char)
        elif not in_string and char == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(char)

    if current:
        items.append("".join(current))

    return items


def split_alias_from_expr(expr_str, type_keywords=None):
    """
    从表达式中分离别名

    :param expr_str: 表达式字符串（如 "name AS user_name"）
    :param type_keywords: 类型关键字集合，用于过滤
    :return: (original_expr, alias)，如果没有别名则返回 (expr_str, expr_str)
    """
    if type_keywords is None:
        type_keywords = set()

    depth = 0
    last_as_pos = -1
    upper = expr_str.upper()
    i = 0

    while i < len(expr_str):
        c = expr_str[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and upper[i:i + 4] == " AS " and (i + 4 <= len(expr_str)):
            last_as_pos = i
        i += 1

    if last_as_pos >= 0:
        original = expr_str[:last_as_pos].strip()
        alias = expr_str[last_as_pos + 4:].strip()

        if alias and alias.upper() not in type_keywords:
            return original, alias

    return expr_str, expr_str


def find_clauses_with_brackets(text, keyword):
    """
    查找所有 keyword=[...] 子句（处理嵌套括号）

    :param text: 包含 keyword=[] 的文本
    :param keyword: 关键字（如 "select", "fields"）
    :return: 所有子句内容列表（不含 keyword=[] 括号）
    """
    clauses = []
    i = 0
    text_len = len(text)
    keyword_len = len(keyword)
    keyword_lower = keyword.lower()

    while i < text_len:
        end_pos = i + keyword_len + 2  # keyword=[
        if end_pos < text_len and text[i:i + keyword_len].lower() == keyword_lower and text[i + keyword_len] == '=' and text[i + keyword_len + 1] == '[':
            start = end_pos
            depth = 0
            current_clause = []
            j = start

            while j < text_len:
                char = text[j]
                if char == "[":
                    depth += 1
                    current_clause.append(char)
                elif char == "]":
                    if depth == 0:
                        clauses.append("".join(current_clause))
                        break
                    else:
                        depth -= 1
                        current_clause.append(char)
                else:
                    current_clause.append(char)
                j += 1
            i = j + 1
        else:
            i += 1

    return clauses


def extract_function_name(expr_str):
    """
    从表达式中提取函数名

    :param expr_str: 函数调用表达式（如 "upper(name)"）
    :return: 函数名，无法提取返回 None
    """
    match = re.match(r'^([a-zA-Z_]\w*)\s*\(', expr_str)
    return match.group(1) if match else None


def parse_comparison_expr(expr_str):
    """
    解析比较表达式

    :param expr_str: 文本格式的比较表达式
    :return: (left, operator, right)，无法解析返回 None
    """
    match = re.match(r'^(.+?)\s*(=|<>|!=|>=|<=|>|<)\s*(.+)$', expr_str.strip())
    if match:
        return match.group(1).strip(), match.group(2), match.group(3).strip()
    return None
