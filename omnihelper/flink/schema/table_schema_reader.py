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
Flink 表结构读取模块


模块功能说明:
    本模块负责从 CSV 文件中读取和解析 Flink 表结构信息，主要功能包括:
    1. 读取 CSV 格式的表结构配置文件
    2. 解析和标准化字段类型（如 ROW, ARRAY, MAP）
    3. 处理嵌套类型的递归解析
    4. 构建字段类型映射用于类型推断
    5. 支持嵌套字段的路径解析

设计理念:
    - 采用静态方法设计，无需实例化即可使用
    - 支持 CSV 中嵌套类型的分段存储（跨行存储）
    - 提供多种字段类型查找方式（字段名、表名.字段名、嵌套路径）
    - 类型标准化与 TypeNormalizer 模块配合使用

CSV 文件格式要求:
    必须包含以下必填列（定义在 CsvColumns 常量中）:
    - TABLE_NAME: 表名
    - FIELD_NAME: 字段名
    - FIELD_TYPE: 字段类型（支持嵌套类型如 ROW<name VARCHAR, age INT>）

    嵌套类型跨行存储支持:
    - 当嵌套类型过长时，可拆分为多个 FIELD_TYPE_1, FIELD_TYPE_2... 列
    - 自动识别括号匹配并合并

使用场景:
    - 为类型解析器提供表结构信息
    - 支持字段类型推断和验证
    - 在表达式解析时查找字段类型

依赖模块:
    - csv: Python 标准库，用于解析 CSV 文件
    - os: 文件路径操作
    - collections.defaultdict: 用于构建表结构字典
    - CsvColumns: Flink 常量定义，包含 CSV 列名常量
    - TypeNormalizer: 类型标准化工具
    - logger: 日志记录工具
"""

# 导入标准库模块
import csv
import os
from collections import defaultdict

# 导入项目内部模块
from omnihelper.constants.flink_constants import CsvColumns
from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.log import logger


class TableSchemaReader:
    """
    Flink 表结构读取器

    核心职责:
    1. 从 CSV 文件加载表结构定义
    2. 解析字段类型并标准化
    3. 构建多种类型的字段映射（字段名→类型、表名.字段名→类型）
    4. 处理嵌套类型的递归解析

    设计特点:
    - 全部使用静态方法，无需创建实例
    - 与 TypeNormalizer 紧密配合完成类型标准化
    - 支持 CSV 文件中嵌套类型的跨行存储

    使用示例:
    ```python
    # 读取表结构
    schema = TableSchemaReader.read_table_schema("schema.csv")

    # 构建字段类型映射
    column_type, table_column_type = TableSchemaReader.build_column_type_mapping(schema)

    # 解析嵌套字段类型
    nested_type = TableSchemaReader.resolve_nested_field_type(col_info, ["address", "city"])
    ```
    """

    @staticmethod
    def read_table_schema(csv_path):
        """
        从 CSV 文件读取表结构

        参数说明:
        :param csv_path: str，CSV 文件路径
        :return: dict，表结构字典，格式为 {表名: [字段信息列表]}
            每个字段信息字典包含:
            - field_name: str，字段名
            - field_type: str，标准化后的字段类型（如 VARCHAR, INT, ROW）
            - original_type: str，原始类型字符串（保留原始格式）
            - nested_fields: list，嵌套字段列表（仅 ROW 类型有值）

        实现流程:
        1. 检查文件路径有效性，不存在则返回空字典
        2. 调用 _read_csv_rows 读取并解析 CSV 行数据
        3. 对每个字段进行类型标准化（TypeNormalizer.normalize_type）
        4. 解析嵌套类型结构（TypeNormalizer.parse_row_type）
        5. 按表名分组，使用 defaultdict(list) 组织数据

        错误处理:
        - 文件不存在: 返回空字典并记录警告日志
        - CSV 解析异常: 捕获所有异常，记录警告并返回已解析的数据

        关键变量说明:
        - table_schema: defaultdict(list)，用于按表名分组存储字段信息
        - normalized_type: 标准化后的类型，便于后续比较和判断
        - nested_fields: 嵌套字段列表，支持递归访问嵌套结构
        """
        # 初始化表结构字典，使用 defaultdict 自动创建列表
        table_schema = defaultdict(list)

        # 参数校验：路径为空或文件不存在
        if not csv_path or not os.path.exists(csv_path):
            logger.warning(f"Table schema CSV file not found: {csv_path}")
            return table_schema

        try:
            # 读取 CSV 行数据
            rows = TableSchemaReader._read_csv_rows(csv_path)

            # 遍历每行数据，构建表结构
            for table_name, field_name, field_type in rows:
                # 跳过空值行
                if not table_name or not field_name or not field_type:
                    continue

                # 类型标准化：将 STRING 转为 VARCHAR，INTEGER 转为 INT 等
                normalized_type = TypeNormalizer.normalize_type(field_type)

                # 解析嵌套类型结构（仅 ROW 类型有嵌套字段）
                nested_fields = TypeNormalizer.parse_row_type(field_type)

                # 添加到表结构字典
                table_schema[table_name].append({
                    "field_name": field_name,
                    "field_type": normalized_type,
                    "original_type": field_type,
                    "nested_fields": nested_fields,
                })

            logger.info(f"Loaded table schema from {csv_path}, total {len(table_schema)} tables")
        except Exception as e:
            logger.warning(f"Failed to read table schema CSV: {e}")

        return table_schema

    @staticmethod
    def _read_csv_rows(csv_path):
        """
        读取 CSV 文件行数据并验证格式

        参数说明:
        :param csv_path: str，CSV 文件路径
        :return: list，(table_name, field_name, field_type) 元组列表

        实现逻辑:
        1. 使用 csv.DictReader 读取 CSV，自动解析列名
        2. 验证必填列是否存在（TABLE_NAME, FIELD_NAME, FIELD_TYPE）
        3. 逐行提取字段信息，跳过空行
        4. 调用 _reconstruct_type_from_row 处理跨行存储的嵌套类型
        5. 返回标准化的行数据列表

        必填列验证规则:
        - 使用集合操作检查 required.issubset(fieldnames)
        - 缺少必填列时记录警告并返回空列表

        类型重建场景:
        - 当嵌套类型字符串过长时，CSV 可能将其拆分为多个列存储
        - 例如 ROW<field1 INT, field2 VARCHAR, field3 BIGINT> 可能拆分为 FIELD_TYPE, FIELD_TYPE_1, FIELD_TYPE_2
        - _reconstruct_type_from_row 负责合并这些分段
        """
        rows = []

        # 打开 CSV 文件，使用 utf-8 编码
        with open(csv_path, "r", encoding="utf-8") as f:
            # 创建 DictReader，自动解析第一行为列名
            reader = csv.DictReader(f)

            # 获取必填列集合
            required = set(CsvColumns.get_required_columns())

            # 验证必填列是否存在
            if not required.issubset(set(reader.fieldnames or [])):
                logger.warning(
                    f"CSV file missing required columns. Required: {required}, "
                    f"Found: {set(reader.fieldnames or [])}"
                )
                return rows

            # 逐行处理
            for row in reader:
                # 提取字段值并去除首尾空白
                table_name = row.get(CsvColumns.TABLE_NAME, "").strip()
                field_name = row.get(CsvColumns.FIELD_NAME, "").strip()
                field_type = row.get(CsvColumns.FIELD_TYPE, "").strip()

                # 跳过空值行
                if not table_name or not field_name or not field_type:
                    continue

                # 重建可能被拆分的类型字符串
                field_type = TableSchemaReader._reconstruct_type_from_row(field_type, row)

                # 添加到结果列表
                rows.append((table_name, field_name, field_type))

        return rows

    @staticmethod
    def _reconstruct_type_from_row(field_type, row):
        """
        从 CSV 行重建完整的类型字符串（处理跨行存储）

        参数说明:
        :param field_type: str，初始字段类型（可能不完整）
        :param row: dict，CSV 行字典，包含所有列
        :return: str，重建后的完整类型字符串

        实现原因:
        CSV 文件中嵌套类型（如 ROW<field1 INT, field2 VARCHAR, ...>）可能因长度限制被拆分到多个列存储。
        例如:
        - FIELD_TYPE: "ROW<field1 INT"
        - FIELD_TYPE_1: "field2 VARCHAR"
        - FIELD_TYPE_2: "field3 BIGINT>"

        处理算法:
        1. 统计类型字符串中 < 和 > 的数量
        2. 如果 < 数量 > > 数量，说明类型不完整
        3. 查找所有以 FIELD_TYPE_ 开头的额外列（按数字排序）
        4. 按顺序拼接这些列的值
        5. 如果仍有未闭合的 <，添加缺失的 > 符号

        关键变量:
        - open_angle: 开括号 < 的数量
        - close_angle: 闭括号 > 的数量
        - extra_parts: 存储从额外列收集的类型片段
        """
        # 空值直接返回
        if not field_type:
            return field_type

        # 统计括号数量
        open_angle = field_type.count("<")
        close_angle = field_type.count(">")

        # 括号已匹配，无需重建
        if open_angle <= close_angle:
            return field_type

        # 收集额外列的内容
        extra_parts = []
        # 按列名排序，确保 FIELD_TYPE_1, FIELD_TYPE_2 按顺序拼接
        for key in sorted(row.keys()):
            # 匹配 FIELD_TYPE_ 开头但不是 FIELD_TYPE 的列
            if key.startswith(CsvColumns.FIELD_TYPE) and key != CsvColumns.FIELD_TYPE:
                val = row.get(key, "").strip()
                if val:
                    extra_parts.append(val)

        # 拼接额外部分
        if extra_parts:
            field_type = field_type + "," + ",".join(extra_parts)

        # 检查是否需要补充闭括号
        if field_type.count("<") > field_type.count(">"):
            field_type = field_type + ">"

        return field_type

    @staticmethod
    def build_column_type_mapping(table_schema, tables_used=None):
        """
        构建字段类型映射

        参数说明:
        :param table_schema: dict，表结构字典 {表名: [字段信息列表]}
        :param tables_used: set 或 None，可选，指定要处理的表名集合
        :return: tuple，(column_type, table_column_type)
            - column_type: dict，{字段名小写: 类型}，用于快速查找
            - table_column_type: dict，{表名.字段名: 类型}，用于精确查找

        映射规则:
        1. 表名和字段名统一转为小写，确保大小写不敏感
        2. 字段名冲突处理：当同一字段名出现在多个表中时，记录调试信息
        3. 优先使用 table_name.field_name 格式避免歧义

        使用场景:
        - 在类型解析时快速查找字段类型（使用 column_type）
        - 在多表查询中精确查找字段类型（使用 table_column_type）

        示例:
        ```python
        column_type["name"]           # -> "VARCHAR"
        table_column_type["user.name"] # -> "VARCHAR"
        ```

        冲突处理:
        当不同表有相同字段名时，column_type 只保存第一个遇到的类型，
        并记录调试日志建议使用 table_name.field_name 格式。
        """
        # 初始化两个映射字典
        column_type = {}
        table_column_type = {}

        # 确定要处理的表（支持过滤）
        tables_to_process = (
            {t: table_schema[t] for t in tables_used if t in table_schema}
            if tables_used
            else table_schema
        )

        # 遍历每个表的字段
        for table_name, columns in tables_to_process.items():
            for col_info in columns:
                field_name = col_info["field_name"]
                # 对于 ROW 类型，使用标准化后的类型（避免存储完整的 ROW 定义）
                # 对于其他类型，使用原始类型保留精度信息（如 TIMESTAMP(3)）
                if col_info["field_type"] == "ROW":
                    field_type = col_info["field_type"]
                else:
                    field_type = col_info["original_type"]

                # 构建表名.字段名格式的键（小写）
                key = f"{table_name}.{field_name}".lower()
                table_column_type[key] = field_type

                # 构建字段名格式的键（小写）
                name_lower = field_name.lower()
                if name_lower not in column_type:
                    column_type[name_lower] = field_type
                else:
                    # 字段名冲突，记录调试信息
                    logger.debug(
                        f"Column name conflict: '{field_name}' exists in multiple tables. "
                        f"Use table_name.field_name format for disambiguation."
                    )

        return column_type, table_column_type

    @staticmethod
    def resolve_nested_field_type(column_info, field_path):
        """
        解析嵌套字段的类型（递归查找）

        参数说明:
        :param column_info: dict，列信息字典，必须包含 field_type 和 nested_fields
        :param field_path: list，字段路径列表，如 ["address", "city"]
        :return: str，字段类型字符串，找不到返回 "unknown"

        实现逻辑:
        1. 参数校验：路径为空或无嵌套字段时返回基础类型
        2. 初始化 nested 为最外层嵌套字段列表
        3. 按路径逐级遍历，每级查找匹配的字段名
        4. 找到后更新 nested 为该字段的嵌套字段列表
        5. 任何一级找不到则返回 "unknown"
        6. 遍历完成后返回最终找到的字段类型

        使用示例:
        假设有如下类型定义:
        ROW<address ROW<city VARCHAR, street VARCHAR>>

        column_info = {
            "field_type": "ROW",
            "nested_fields": [
                {
                    "field_name": "address",
                    "field_type": "ROW",
                    "nested_fields": [
                        {"field_name": "city", "field_type": "VARCHAR"},
                        {"field_name": "street", "field_type": "VARCHAR"}
                    ]
                }
            ]
        }

        resolve_nested_field_type(column_info, ["address", "city"]) -> "VARCHAR"

        递归终止条件:
        - field_path 为空列表
        - nested_fields 为空列表
        - 某一级字段名找不到
        """
        # 参数校验：路径为空或无嵌套字段
        if not field_path or not column_info.get("nested_fields"):
            return column_info.get("field_type", "unknown")

        # 当前层级的嵌套字段列表
        nested = column_info["nested_fields"]

        # 保存最终找到的类型
        result_type = "unknown"

        # 按路径逐级查找
        for part in field_path:
            found = False
            for field in nested:
                if field["field_name"] == part:
                    # 进入下一层嵌套
                    nested = field.get("nested_fields", [])
                    # 优先使用原始类型，保留精度信息（如 TIMESTAMP(3)）
                    result_type = field.get("original_type") or field["field_type"]
                    found = True
                    break
            # 某一级找不到，返回 unknown
            if not found:
                return "unknown"

        return result_type
