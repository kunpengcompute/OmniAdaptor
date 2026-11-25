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
Flink 数据类型标准化模块


模块功能说明:
    本模块提供 Flink 数据类型的标准化和解析功能，主要包括:
    1. 类型别名映射 (如 STRING -> VARCHAR)
    2. 类型标准化 (统一大小写和格式)
    3. ROW/ARRAY/MAP 等嵌套类型的解析
    4. 数值类型的优先级比较
    5. 多类型场景下的公共类型推导

类型别名映射表:
    - STRING -> VARCHAR
    - INTEGER -> INT
    - LONG -> BIGINT
    - REAL -> FLOAT
    - 带 NOT NULL 后缀的类型 -> 去除后缀
"""

import re


class TypeNormalizer:
    """
    Flink 数据类型标准化器

    核心职责:
    1. 统一不同格式的类型表示
    2. 解析复杂嵌套类型的内部结构
    3. 提供类型兼容性判断
    4. 推导多类型的公共父类型

    设计原则:
    - 静态方法设计，无需实例化
    - 返回标准化类型，便于比较和判断
    - 支持嵌套类型的递归解析
    """

    TYPE_ALIASES = {
        "STRING": "VARCHAR",
        "INTEGER": "INT",
        "LONG": "BIGINT",
        "REAL": "FLOAT",
        "DOUBLE NOT NULL": "DOUBLE",
        "FLOAT NOT NULL": "FLOAT",
        "INT NOT NULL": "INT",
        "BIGINT NOT NULL": "BIGINT",
        "VARCHAR NOT NULL": "VARCHAR",
        "BOOLEAN NOT NULL": "BOOLEAN",
        "TIMESTAMP NOT NULL": "TIMESTAMP",
        "DATE NOT NULL": "DATE",
        "TINYINT": "TINYINT",
        "SMALLINT": "SMALLINT",
        "DECIMAL": "DECIMAL",
        "BINARY": "BINARY",
        "VARBINARY": "VARBINARY",
        "MULTISET": "MULTISET",
        "ROW": "ROW",
        "ARRAY": "ARRAY",
        "MAP": "MAP",
        "TIME": "TIME",
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE": "TIMESTAMP_LTZ",
        "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    }

    NUMERIC_PRIORITY = {
        "TINYINT": 1,
        "SMALLINT": 2,
        "INT": 3,
        "BIGINT": 4,
        "FLOAT": 5,
        "DOUBLE": 6,
        "DECIMAL": 7,
    }

    @staticmethod
    def normalize_type(type_str):
        """
        标准化数据类型字符串

        参数说明:
        :param type_str: 原始类型字符串
        :return: 标准化后的类型名称

        标准化规则:
        1. 空值返回 "unknown"
        2. 去除首尾空白
        3. ROW/ARRAY/MAP 等复杂类型直接返回
        4. VARCHAR/CHAR 类型直接返回
        5. TIMESTAMP_WITH_LOCAL_TIME_ZONE 转为 TIMESTAMP_LTZ
        6. 其他类型查别名表，无匹配则返回大写形式

        处理优先级 (按顺序):
        1. 空值检查
        2. ROW 类型检查
        3. ARRAY 类型检查
        4. MAP 类型检查
        5. MULTISET 类型检查
        6. DECIMAL 类型检查
        7. VARCHAR 类型检查
        8. CHAR 类型检查
        9. TIMESTAMP_WITH_LOCAL_TIME_ZONE 检查
        10. TIMESTAMP 类型检查
        11. 别名表查找
        """
        if not type_str:
            return "unknown"

        type_str = type_str.strip()

        if type_str.upper().startswith("ROW"):
            return "ROW"

        if type_str.upper().startswith("ARRAY"):
            return "ARRAY"

        if type_str.upper().startswith("MAP"):
            return "MAP"

        if type_str.upper().startswith("MULTISET"):
            return "MULTISET"

        if type_str.upper().startswith("DECIMAL"):
            return "DECIMAL128"

        if type_str.upper().startswith("VARCHAR"):
            return "VARCHAR"

        if type_str.upper().startswith("CHAR"):
            return "CHAR"

        if type_str.upper().startswith("TIMESTAMP_WITH_LOCAL_TIME_ZONE"):
            return "TIMESTAMP_LTZ"

        if type_str.upper().startswith("TIMESTAMP"):
            return "TIMESTAMP"

        upper = type_str.upper()
        return TypeNormalizer.TYPE_ALIASES.get(upper, upper)

    @staticmethod
    def parse_row_type(type_str):
        """
        解析 ROW 类型的内部字段结构

        参数说明:
        :param type_str: ROW 类型字符串，如 "ROW<name VARCHAR, age INT>"
        :return: 字段信息列表，每个字段包含:
            - field_name: 字段名
            - field_type: 标准化后的字段类型
            - original_type: 原始类型字符串
            - nested_fields: 嵌套字段列表 (递归)

        实现流程:
        1. 检查空值和非 ROW 类型
        2. 提取 <> 包围的内容
        3. 分割字段列表 (逗号分隔，忽略括号内逗号)
        4. 解析每个字段的名称和类型
        5. 递归处理嵌套的 ROW/ARRAY/MAP 类型
        """
        if not type_str:
            return []

        type_str = type_str.strip()
        if not type_str.upper().startswith("ROW"):
            return []

        inner = TypeNormalizer._extract_angle_brackets_content(type_str)
        if not inner:
            return []

        return TypeNormalizer._parse_row_fields(inner)

    @staticmethod
    def _extract_angle_brackets_content(type_str):
        """
        提取尖括号包围的内容

        参数说明:
        :param type_str: 包含尖括号的类型字符串
        :return: 括号内容，不包含括号本身

        实现算法:
        1. 找到第一个 < 的位置
        2. 使用深度计数器追踪嵌套
        3. 当深度回到 0 时，定位匹配的 >
        4. 返回 < 和 > 之间的内容

        示例:
        "ROW<name VARCHAR, age INT>" -> "name VARCHAR, age INT"
        """
        start_idx = type_str.find("<")
        if start_idx == -1:
            return None

        depth = 0
        for i in range(start_idx, len(type_str)):
            if type_str[i] == "<":
                depth += 1
            elif type_str[i] == ">":
                depth -= 1
                if depth == 0:
                    return type_str[start_idx + 1 : i]

        return None

    @staticmethod
    def _parse_row_fields(fields_str):
        """
        解析 ROW 类型的字段列表

        参数说明:
        :param fields_str: 字段列表字符串，如 "name VARCHAR, age INT"
        :return: 字段信息字典列表

        解析规则:
        1. 使用 _split_row_fields 分割字段
        2. 每字段格式: "字段名 类型" (空格分隔)
        3. 使用正则提取字段名和类型
        4. 标准化类型
        5. 递归解析嵌套类型
        """
        fields = []
        parts = TypeNormalizer._split_row_fields(fields_str)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            match = re.match(r"^(\w+)\s+(.+)$", part.strip())
            if not match:
                continue

            field_name = match.group(1)
            field_type_str = match.group(2).strip()

            normalized_type = TypeNormalizer.normalize_type(field_type_str)
            nested_fields = TypeNormalizer.parse_row_type(field_type_str)

            fields.append({
                "field_name": field_name,
                "field_type": normalized_type,
                "original_type": field_type_str,
                "nested_fields": nested_fields,
            })

        return fields

    @staticmethod
    def _split_row_fields(fields_str):
        """
        按逗号分割字段列表

        参数说明:
        :param fields_str: 字段列表字符串
        :return: 分割后的字段字符串列表

        关键处理:
        - 忽略 <> 和 () 内的逗号
        - 深度计数器确保正确匹配括号
        """
        parts = []
        current = []
        depth = 0

        for char in fields_str:
            if char == "<":
                depth += 1
                current.append(char)
            elif char == ">":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            parts.append("".join(current))

        return parts

    @staticmethod
    def find_common_type(type1, type2):
        """
        查找两个类型的公共类型

        参数说明:
        :param type1: 第一个类型
        :param type2: 第二个类型
        :return: 公共类型，无法确定返回 None

        类型兼容规则:
        1. 相同类型直接返回
        2. 任一为 unknown，返回 unknown
        3. 标准化后相同，返回该类型
        4. 数值类型: 返回优先级较高的
        5. 字符串类型: 包含 VARCHAR/CHAR 则返回 VARCHAR
        6. 日期时间类型: 返回精度较高的
        7. BOOLEAN 与其他: 返回 VARCHAR

        数值优先级 (从低到高):
        TINYINT < SMALLINT < INT < BIGINT < FLOAT < DOUBLE < DECIMAL
        """
        if not type1 or not type2:
            return None

        if type1 == type2:
            return type1

        if type1 == "unknown" or type2 == "unknown":
            return "unknown"

        t1 = TypeNormalizer.normalize_type(type1)
        t2 = TypeNormalizer.normalize_type(type2)

        if t1 == t2:
            return t1

        if t1 in TypeNormalizer.NUMERIC_PRIORITY and t2 in TypeNormalizer.NUMERIC_PRIORITY:
            p1 = TypeNormalizer.NUMERIC_PRIORITY[t1]
            p2 = TypeNormalizer.NUMERIC_PRIORITY[t2]
            return t1 if p1 > p2 else t2

        if t1 == "VARCHAR" or t2 == "VARCHAR":
            return "VARCHAR"

        if t1 == "CHAR" or t2 == "CHAR":
            return "VARCHAR"

        datetime_priority = {"DATE": 1, "TIMESTAMP": 2, "TIMESTAMP_LTZ": 3}
        if t1 in datetime_priority and t2 in datetime_priority:
            p1 = datetime_priority[t1]
            p2 = datetime_priority[t2]
            return t1 if p1 > p2 else t2

        if t1 == "BOOLEAN" or t2 == "BOOLEAN":
            return "VARCHAR"

        return None

    @staticmethod
    def find_common_type_multi(types):
        """
        查找多个类型的公共类型

        参数说明:
        :param types: 类型列表
        :return: 公共类型，无法确定返回 None

        实现逻辑:
        1. 空列表返回 None
        2. 从第一个类型开始
        3. 依次与后续类型调用 find_common_type
        4. 任何一步返回 None，则整体返回 None
        """
        if not types:
            return None

        result = types[0]
        for t in types[1:]:
            result = TypeNormalizer.find_common_type(result, t)
            if result is None:
                return None

        return result

    @staticmethod
    def expand_row_type(type_str):
        """
        将 ROW 类型递归展开为嵌套字段的类型列表。
        例如: ROW<id INT, name VARCHAR, addr ROW<city VARCHAR, zip INT>>
        返回: ["INT", "VARCHAR", "VARCHAR", "INT"]
        对于非 ROW 类型，返回包含原始类型的列表（保留精度信息）。
        """
        if not type_str:
            return ["unknown"]
        type_str = type_str.strip()
        if not type_str.upper().startswith("ROW"):
            # 对于非 ROW 类型，直接返回原始类型，不进行标准化，保留精度信息
            return [type_str]
        inner = TypeNormalizer._extract_angle_brackets_content(type_str)
        if not inner:
            return ["ROW"]
        parts = TypeNormalizer._split_row_fields(inner)
        expanded = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            match = re.match(r"^(\w+)\s+(.+)$", part.strip())
            if not match:
                continue
            field_type_str = match.group(2).strip()
            # 递归展开嵌套的 ROW 类型
            if field_type_str.upper().startswith("ROW"):
                nested_expanded = TypeNormalizer.expand_row_type(field_type_str)
                expanded.extend(nested_expanded)
            else:
                # 保留原始类型，不进行标准化，保留精度信息（如 TIMESTAMP(3)）
                expanded.append(field_type_str)
        return expanded if expanded else ["ROW"]
