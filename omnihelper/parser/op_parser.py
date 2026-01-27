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
        self.param_type_mapping = {}

        self._load_op_mapping()
        self._load_op_dictionary()

    def _load_op_mapping(self):
        try:
            with open(self.MAPPING_PATH, "r", encoding="utf-8") as f:
                self.opname_mapping = json.load(f)
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

    def parse_event(self, event):
        """
        单事件表达式、函数解析核心逻辑
        :return:
        """
        analysis_result = []
        physical_plan = event.get("physical plan")
        if not physical_plan:
            print("no physical plan")
            return ""
        if event.get("node_metrics"):
            TypeMatcher.extract_param_type(event.get("node_metrics"), self.param_type_mapping)
        update_physical_plan = self.preprocess_physical_plan(physical_plan)
        sql_hash = hashlib.sha256(event.get("original query").encode("utf-8")).hexdigest()[-6:]
        for index, block in enumerate(update_physical_plan):
            if "ReadSchema" in block:
                # 更新参数类型映射表
                TypeMatcher.extract_param_type(block, self.param_type_mapping)

            opname = block.split("\n")[0].split()[1].lower()
            input_list = []
            output_list = []

            # 提取输入列表
            input_pattern = re.compile(r'Input\s*\[\d+\]:\s*\[([^\]]+)\]')
            input_match = input_pattern.search(block)
            if input_match:
                input_list = [
                    TypeMatcher.judge_param_type(item.strip(), self.param_type_mapping)
                    for item in input_match.group(1).split(',')
                    if item.strip()
                ]

            is_supported_op = self.evaluate_support_status(opname, input_list)
            if is_supported_op:
                continue

            # 提取输出列表
            output_pattern = re.compile(r'Output\s*\[\d+\]:\s*\[([^\]]+)\]')
            output_match = output_pattern.search(block)
            if output_match:
                output_list = [
                    TypeMatcher.judge_param_type(item.strip(), self.param_type_mapping)
                    for item in output_match.group(1).split(',')
                    if item.strip()
                ]

            analysis_result.append(
                {
                    "op_name": self.opname_mapping.get(opname),
                    "sql_hash": sql_hash,
                    "input_list": input_list,
                    "output_list": output_list
                }
            )
        return self.count_op_times(analysis_result)

    def preprocess_physical_plan(self, physical_plan):
        split_phy_plan = physical_plan.split("\n\n")
        op_block_pattern = re.compile(r'^\(\d+\).*')
        preprocess_phy_plan = [line.strip() for line in split_phy_plan if op_block_pattern.match(line.strip())]
        return preprocess_phy_plan

    def evaluate_support_status(self, opname, input_list):
        if not opname in self.opname_mapping:
            return True

        real_op_name = self.opname_mapping.get(opname)
        op_supported_list = self.op_dictionary.get(real_op_name)

        if op_supported_list is not None and all(item in op_supported_list for item in input_list):
            return True
        return False

    def count_op_times(self, event_result):
        counter = defaultdict(int)

        for item in event_result:
            key = (item["op_name"], item["sql_hash"], tuple(item["input_list"]), tuple(item["output_list"]))
            counter[key] += 1

        update_event_result = []
        for (op_name, sql_hash, input_list, output_list), times in counter.items():
            update_event_result.append({
                "op_name": op_name,
                "sql_hash": sql_hash,
                "input_list": input_list,
                "output_list": output_list,
                "times": times
            })
        return update_event_result