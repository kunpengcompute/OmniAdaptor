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
Flink Schema 链构建模块

负责构建和维护数据流的 Schema 链，追踪字段类型变化，包括：
- 解析算子描述，构建输入/输出 Schema
- 合并上游上下文信息
- 处理数据源算子和普通算子
"""

import re

from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.flink.schema.table_schema_reader import TableSchemaReader


class SchemaChainBuilder:
    """
    Schema 链构建器

    核心职责：
    1. 从算子描述中提取表源信息
    2. 构建每个算子的输入/输出 Schema
    3. 维护别名映射
    4. 合并上游上下文
    """

    SOURCE_OP_TYPES = {"Csv Source", "KafKa Source", "TableSourceScan", "Source"}
    PASS_THROUGH_OP_TYPES = {"Deduplicate", "Expand", "WatermarkAssigner",
                              "StreamRecordTimestampInserter", "ConstraintEnforcer"}

    def __init__(self, type_resolver):
        self.type_resolver = type_resolver

    def _extract_op_type(self, desc_item):
        """从描述项中提取算子类型"""
        if not isinstance(desc_item, str):
            return None
        op_match = re.match(r'\[(\d+)\]:([A-Za-z]+)', desc_item)
        if not op_match:
            return None
        return op_match.group(2)

    def _is_source_or_first_op(self, op_type, schema_chain, current_input):
        """判断是否为源算子或第一个算子"""
        return (
            op_type in self.SOURCE_OP_TYPES
            or (
                not schema_chain
                and op_type is None
                and not current_input
            )
        )

    def _update_table_column_types(self, tables):
        """更新表列类型映射"""
        for table_name in tables:
            table_cols = self.type_resolver.table_schema.get(table_name, [])
            if not table_cols:
                continue

            col_type, table_col_type = (
                TableSchemaReader.build_column_type_mapping(self.type_resolver.table_schema, {table_name})
            )
            self.type_resolver.update_column_type(col_type, table_col_type)

    def _append_source_schema_chain(self, schema_chain, op_type, tables, output_schema, alias_map):
        """追加源 Schema 链"""
        schema_chain.append({
            "op_type": op_type or "Source",
            "input_schema": [],
            "output_schema": output_schema,
            "tables": tables,
            "alias_map": dict(alias_map),
        })
        self._update_table_column_types(tables)

    def _append_upstream_or_source(self, schema_chain, current_input, description_data, alias_map):
        """追加上游或源 Schema"""
        if current_input:
            schema_chain.append({
                "op_type": "Upstream",
                "input_schema": [],
                "output_schema": current_input,
                "alias_map": dict(alias_map),
            })
            return

        if not description_data:
            return

        tables, output_schema = (self.type_resolver.extract_table_source_info(description_data))
        if not output_schema:
            return

        self._append_source_schema_chain(
            schema_chain=schema_chain,
            op_type="Source",
            tables=tables,
            output_schema=output_schema,
            alias_map=alias_map,
        )

    def build_schema_chain(self, description_data, upstream_context=None):
        """
        构建当前 vertex 的 schema chain

        :param description_data: list，描述数据列表
        :param upstream_context: dict，上游上下文
        :return: list，schema chain
        """
        schema_chain = []
        upstream_context = upstream_context or {}
        upstream_alias_map = upstream_context.get("alias_map", {})
        upstream_output_schema = upstream_context.get("output_schema")

        current_input = upstream_output_schema
        accumulated_alias_map = dict(upstream_alias_map)

        if accumulated_alias_map:
            self.type_resolver.update_alias_map(accumulated_alias_map)

        for desc_item in description_data:
            op_type = self._extract_op_type(desc_item)

            if self._is_source_or_first_op(op_type, schema_chain, current_input):
                tables, output_schema = (self.type_resolver.extract_table_source_info(description_data))
                if output_schema:
                    self._append_source_schema_chain(
                        schema_chain=schema_chain,
                        op_type=op_type,
                        tables=tables,
                        output_schema=output_schema,
                        alias_map=accumulated_alias_map,
                    )
                    current_input = output_schema
                continue

            if op_type and current_input is not None:
                # 只传入当前算子描述和JSON描述，避免不同算子间的select/groupBy等子句交叉干扰
                filtered_desc = [desc_item] + [d for d in description_data if isinstance(d, dict)]
                output_schema = self.type_resolver.build_output_schema(op_type, filtered_desc, current_input)
                op_alias_map = (self.type_resolver.extract_alias_map_from_description(filtered_desc))
                accumulated_alias_map.update(op_alias_map)
                self.type_resolver.update_alias_map(accumulated_alias_map)

                schema_chain.append({
                    "op_type": op_type,
                    "input_schema": current_input,
                    "output_schema": output_schema,
                    "alias_map": dict(accumulated_alias_map),
                })
                current_input = output_schema

        if not schema_chain:
            self._append_upstream_or_source(
                schema_chain=schema_chain,
                current_input=current_input,
                description_data=description_data,
                alias_map=accumulated_alias_map,
            )

        return schema_chain

    @staticmethod
    def merge_upstream_context(upstream_ids, vertex_context):
        """
        合并上游顶点的上下文信息

        :param upstream_ids: list，上游顶点 ID 列表
        :param vertex_context: dict，顶点上下文
        :return: dict，合并后的上下文
        """
        merged_alias_map = {}
        merged_output_schema = []

        for uid in upstream_ids:
            ctx = vertex_context.get(uid)
            if not ctx:
                continue
            merged_alias_map.update(ctx.get("alias_map", {}))
            if ctx.get("output_schema"):
                if not merged_output_schema:
                    merged_output_schema = list(ctx["output_schema"])
                else:
                    merged_output_schema.extend(ctx["output_schema"])

        return {
            "alias_map": merged_alias_map,
            "output_schema": merged_output_schema,
        }

    def get_input_schema_for_op(self, op_type, schema_chain):
        """
        获取算子的输入 schema

        :param op_type: str，算子类型
        :param schema_chain: list，schema 链
        :return: list 或 None，输入 schema
        """
        if not schema_chain:
            return None

        for i, entry in enumerate(schema_chain):
            if entry.get("op_type") == op_type:
                return entry.get("input_schema")

        if schema_chain:
            last = schema_chain[-1]
            if op_type in self.PASS_THROUGH_OP_TYPES:
                return last.get("output_schema")
            return last.get("output_schema")

        return None

    def extract_input_types_from_schema_entry(self, chain_entry, description_data):
        """
        从 schema_chain entry 中提取 input_types_str

        :param chain_entry: dict，schema chain 条目
        :param description_data: list，描述数据
        :return: str，输入类型字符串
        """
        op_type = chain_entry.get("op_type", "")
        output_schema = chain_entry.get("output_schema", [])
        input_types = []

        if output_schema:
            for f in output_schema:
                field_type = f.get("field_type", "unknown")
                if field_type == "ROW":
                    original_type = f.get("original_type", "")
                    if original_type and original_type.upper().startswith("ROW"):
                        input_types.extend(TypeNormalizer.expand_row_type(original_type))
                    else:
                        input_types.append(field_type)
                else:
                    input_types.append(f.get("original_type") or field_type)

        input_str = ",".join(t for t in input_types if t)

        if not input_str and description_data:
            input_str = self._extract_types_from_description(op_type, description_data, chain_entry.get("input_schema"))

        return input_str

    def _extract_types_from_description(self, op_type, description_data, input_schema):
        """从描述中提取类型信息"""
        for desc in description_data:
            if isinstance(desc, dict):
                input_types = desc.get("inputTypes", [])
                if input_types:
                    return ",".join(TypeNormalizer.normalize_type(t) for t in input_types if t)
                output_types = desc.get("outputTypes", [])
                if output_types:
                    return ",".join(TypeNormalizer.normalize_type(t) for t in output_types if t)

            if isinstance(desc, str):
                select_match = re.search(r'select=\[(.*?)\]', desc, re.I)
                if select_match:
                    select_content = select_match.group(1)
                    types = self._extract_types_from_select(select_content, input_schema)
                    if types:
                        return ",".join(types)

                field_match = re.search(r'field=\[(.*?)\]', desc, re.I)
                if field_match:
                    field_content = field_match.group(1)
                    types = self._extract_types_from_field(field_content, input_schema)
                    if types:
                        return ",".join(types)

        return ""

    def _extract_types_from_select(self, select_str, input_schema):
        """从 SELECT 语句中提取类型"""
        types = []
        items = self.type_resolver._split_select_items(select_str)

        for item in items:
            item = item.strip()
            if not item:
                continue

            original_expr, alias_name = self.type_resolver._wrap_split_alias_from_expr(item)
            field_type = self.type_resolver.resolve_text_expr_type(original_expr, input_schema, 0)

            if field_type != "unknown":
                original_type = self._get_original_type_from_schema(original_expr, input_schema)
                if original_type and original_type.upper().startswith("ROW"):
                    expanded = TypeNormalizer.expand_row_type(original_type)
                    types.extend(expanded)
                else:
                    types.append(field_type)
            elif input_schema:
                types.append("unknown")
            else:
                types.append("unknown")

        return types

    def _extract_types_from_field(self, field_str, input_schema):
        """从字段列表中提取类型"""
        types = []
        fields = field_str.split(",")

        for field in fields:
            field = field.strip()
            if not field:
                continue

            if input_schema:
                field_type = self.type_resolver.resolve_expression_type(field, input_schema)
                if field_type == "ROW":
                    original_type = self._get_original_type_from_schema(field, input_schema)
                    if original_type and original_type.upper().startswith("ROW"):
                        expanded = TypeNormalizer.expand_row_type(original_type)
                        types.extend(expanded)
                    else:
                        types.append(field_type if field_type else "unknown")
                else:
                    types.append(field_type if field_type else "unknown")
            else:
                types.append("unknown")

        return types

    def _get_original_type_from_schema(self, field_expr, input_schema):
        """从 input_schema 中查找字段的原始类型"""
        if not input_schema or not field_expr:
            return None

        field_name = field_expr.strip().lower()
        field_name = re.sub(r'\[\d+\]$', '', field_name)

        if field_name in self.type_resolver.column_type:
            col_name = field_name
        else:
            col_name = None

        if col_name:
            for table_name, columns in self.type_resolver.table_schema.items():
                for col_info in columns:
                    if col_info["field_name"].lower() == col_name:
                        original = col_info.get("original_type", "")
                        if original and original.upper().startswith("ROW"):
                            return original

        for field in input_schema:
            if field.get("field_name", "").lower() == field_name:
                return field.get("original_type")

        return None
