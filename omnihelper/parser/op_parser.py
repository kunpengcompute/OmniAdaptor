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
import re
import hashlib
from collections import defaultdict
import time

from omnihelper.parser.type_matcher import TypeMatcher, TypeEnum
from omnihelper.util.common_util import CommonUtil


class OpParser:
    MAPPING_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "omni_opname_mapping_dictionary.json")
    DICTIONARY_PATH = os.path.join(CommonUtil.get_execute_path(), "resources", "omni_op_dictionary.json")

    def __init__(self):
        self.opname_mapping = {}
        self.op_dictionary = {}
        self.omni_ops = {}

        self._load_op_mapping()
        self._load_op_dictionary()

    def _load_op_mapping(self):
        try:
            with open(self.MAPPING_PATH, "r", encoding="utf-8") as f:
                self.opname_mapping = json.load(f)
            self.omni_ops = self.opname_mapping["omni_op_list"]
        except FileNotFoundError:
            raise FileNotFoundError(f"Opname mapping file not found: {self.MAPPING_PATH}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in mapping file: {self.MAPPING_PATH}")
        except Exception as e:
            raise Exception(f"Unexpected error while loading mapping file: {self.MAPPING_PATH}, error: {e}")

    def _load_op_dictionary(self):
        try:
            with open(self.DICTIONARY_PATH, "r", encoding="utf-8") as f:
                self.op_dictionary = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Op dictionary file not found: {self.DICTIONARY_PATH}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in dictionary file: {self.DICTIONARY_PATH}")
        except Exception as e:
            raise Exception(f"Unexpected error while loading dictionary file: {self.DICTIONARY_PATH}, error: {e}")

    def _process_node_metrics(self, node_metrics):
        """
        处理node_metrics信息，提取节点和集群信息
        :param node_metrics: 包含节点指标信息的字符串
        :return: 包含处理后的nodes和node_name_mapping字典
        """
        nodes = {}
        node_name_mapping = {}

        if not node_metrics:
            return nodes, node_name_mapping

        # 处理node_metrics内容
        plan_part, subgraph_part = node_metrics.split("\n\n[SubGraph]")

        # 处理计划部分
        plan_part = plan_part.split("[PlanMetric]\n")[1]
        splited_plan_part = plan_part.split("\n\n")

        op_block_pattern = re.compile(r'^id:(\d+)\s+name:([^\s]+).*')

        for block in splited_plan_part:
            block = block.strip()
            block_match = op_block_pattern.match(block)
            if not block_match:
                continue

            node_id = int(block_match.group(1).strip())
            name_match = block_match.group(2).lower()
            if self.opname_mapping.get(name_match):
                name_match = self.opname_mapping.get(name_match)
            node_name_mapping.setdefault(name_match, []).append(node_id)
            nodes[node_id] = {
                'id': node_id,
                'name': name_match,
                'number_of_output_rows': 0,
                'duration': None,
                'duration_seconds': 0,
                'size': None,
                'size_mb': 0,
                'cluster': [],
            }

            lines = block.strip().split('\n')
            for line in lines:
                # 处理WholeStageCodegen情况
                codegen_match = re.search(r'WholeStageCodegen\s+\(\d+\)', line)
                if codegen_match:
                    codegen_name_match = codegen_match.group(0)
                    nodes[node_id]['name'] = codegen_name_match

                # 处理指标信息
                metric_match = re.search(r'SQLPlanMetric\s*([^)]+)', line)
                if metric_match:
                    metric_content = metric_match.group(1)
                    if 'number of output rows' in metric_content:
                        num_match = re.search(r'number of output rows\s*,(.*?),\s*sum', metric_content)
                        if num_match:
                            nodes[node_id]['number_of_output_rows'] = int(num_match.group(1).replace(",", ""))
                    elif 'duration' in metric_content:
                        dur_match = re.search(r'duration\s*,(.*?),\s*timing', metric_content)
                        if dur_match:
                            time_str = dur_match.group(1)
                            seconds = CommonUtil.parse_time_to_seconds(time_str)
                            nodes[node_id]['duration'] = time_str
                            nodes[node_id]['duration_seconds'] = seconds
                    elif 'size of files read' in metric_content:
                        size_match = re.search(r'\(\s*size of files read\s*,(.*?),\s*size', metric_content)
                        if size_match:
                            size_str = size_match.group(1)
                            mb = CommonUtil.parse_size_to_mb(size_str)
                            nodes[node_id]['size'] = size_str
                            nodes[node_id]['size_mb'] = mb

        # 处理集群信息
        cluster_lines = subgraph_part.strip().split('\n')
        for line in cluster_lines:
            if 'cluster' in line:
                cluster_match = re.search(r'cluster\s+(\d+)\s*:\s*(.+)', line)
                if cluster_match:
                    cluster_id = int(cluster_match.group(1))
                    node_ids = [int(x.strip()) for x in cluster_match.group(2).split()]
                    for node_id in node_ids:
                        nodes[node_id]['cluster'].append(cluster_id)

        return nodes, node_name_mapping

    def parse_event(self, event):
        """
        单事件表达式、函数解析核心逻辑
        :return:
        """
        nodes = {}
        node_name_mapping = {}
        analysis_result = []
        param_type_mapping = {}
        physical_plan = event.get("physical plan")
        if not physical_plan:
            print("no physical plan")
            return False, []
        if event.get("node metrics"):
            TypeMatcher.extract_param_type(event.get("node metrics"), param_type_mapping)
            nodes, node_name_mapping = self._process_node_metrics(event.get("node metrics"))
        update_physical_plan = self.preprocess_physical_plan(physical_plan)
        sql_hash = hashlib.sha256(event.get("original query").encode("utf-8")).hexdigest()[-6:]
        for index, block in enumerate(update_physical_plan):
            if "ReadSchema" in block:
                # 更新参数类型映射表
                TypeMatcher.extract_param_type(block, param_type_mapping)

            opname = block.split("\n")[0].split()[1].lower()

            # 如果包含omni算子，直接跳过整个json
            if opname in self.omni_ops:
                return True, []

            # 提取输入列表
            input_pattern = re.compile(r'Input\s*\[\d+\]:\s*\[([^\]]+)\]')
            input_match = input_pattern.search(block)
            input_list = CommonUtil.parse_param_list(input_match, param_type_mapping)
            is_supported_op = self.evaluate_support_status(opname, input_list)
            if is_supported_op:
                continue

            # 提取输出列表
            opname = self.opname_mapping.get(opname)
            output_pattern = re.compile(r'Output\s*\[\d+\]:\s*\[([^\]]+)\]')
            output_match = output_pattern.search(block)
            output_list = CommonUtil.parse_param_list(output_match, param_type_mapping)

            # 构建time字符串
            time_str_parts = []
            total_seconds = 0
            output_rows = 0
            output_sizes = 0

            # 查找当前操作名在node_name_mapping中对应的节点ID
            node_ids = node_name_mapping.get(opname, [])
            for node_id in node_ids:
                node_info = nodes.get(node_id)
                output_rows += node_info['number_of_output_rows']
                output_sizes += node_info['size_mb']
                if len(node_info['cluster']) > 0:
                    for cluster_id in node_info['cluster']:
                        cluster_time_str = f"{nodes.get(cluster_id)['name']}:{nodes.get(cluster_id)['duration']}"
                        if cluster_time_str not in time_str_parts:  # 检查是否已存在
                            time_str_parts.append(cluster_time_str)
                else:
                    total_seconds += node_info['duration_seconds']
            time_str_parts.append(f"{total_seconds} s")

            analysis_result.append(
                {
                    "op_name": opname,
                    "sql_hash": sql_hash,
                    "input_list": input_list,
                    "output_list": output_list,
                    "output_rows": output_rows,
                    "output_sizes": round(output_sizes, 9),
                    "running_time": "\n".join(time_str_parts),
                }
            )
        return False, self.count_op_times(analysis_result)

    def preprocess_physical_plan(self, physical_plan):
        split_phy_plan = physical_plan.split("\n\n")
        op_block_pattern = re.compile(r'^\(\d+\).*')
        preprocess_phy_plan = [line.strip() for line in split_phy_plan if op_block_pattern.match(line.strip())]
        return preprocess_phy_plan

    def evaluate_support_status(self, opname, input_list):
        if not opname in self.opname_mapping:
            return True

        real_op_name = self.opname_mapping.get(opname)
        op_supported_list = self.op_dictionary.get(real_op_name, {})

        if len(input_list) == 0:
            if len(op_supported_list.get("supported_list", [])) == 0:
                return False
            return True

        if all(item in op_supported_list.get("supported_list", []) for item in input_list):
            return True
        return False

    def count_op_times(self, event_result):
        counter = defaultdict(int)

        for item in event_result:
            key = (item["op_name"],
                   item["sql_hash"],
                   tuple(item["input_list"]),
                   tuple(item["output_list"]),
                   item["running_time"],
                   item["output_rows"],
                   item["output_sizes"])
            counter[key] += 1

        update_event_result = []
        for (op_name, sql_hash, input_list, output_list, running_time, output_rows, output_sizes), times \
                in counter.items():
            update_event_result.append({
                "op_name": op_name,
                "sql_hash": sql_hash,
                "input_list": input_list,
                "output_list": output_list,
                "running_time": running_time,
                "output_rows": output_rows,
                "output_sizes": output_sizes,
                "times": times
            })
        return sorted(update_event_result, key=lambda x: x["op_name"])