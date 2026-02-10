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
import os
import re
import sys
from omnihelper.parser.type_matcher import TypeMatcher, TypeEnum


class CommonUtil:
    @staticmethod
    def get_execute_path():
        if getattr(sys, 'frozen', False):
            return os.path.dirname(os.path.realpath(sys.executable))
        return os.path.dirname(__file__)

    @staticmethod
    def print_failed_files(failed_files, all_files_length):
        # 统一展示失败结果
        if failed_files:
            print(f"Processing completed with {len(failed_files)} failed file{'s' if len(failed_files) != 1 else ''}:")
            for i, (filepath, error) in enumerate(failed_files, 1):
                print(f"{i}. [File]: {filepath}")
                print(f"   [Error]: {error}")

            # 所有文件都处理失败
            if all_files_length == len(failed_files):
                print(f"\nAll {all_files_length} file{'s' if all_files_length != 1 else ''} processed failed!")
            else:
                print(f"\nSummary: {all_files_length - len(failed_files)} succeeded, {len(failed_files)} failed")
        else:
            print(f"All {all_files_length} file{'s' if all_files_length != 1 else ''} processed successfully!")
        print("-" * 60)

    @staticmethod
    def parse_time_to_seconds(time_str):
        """
        将时间字符串转换为秒
        支持的格式:
        - "[total:6.3 m, ...]" -> 提取total部分
        - "631 ms" -> 直接转换
        - "2.2 m" -> 分钟转秒
        """
        try:
            # 如果是列表格式 [total:6.3 m, ...]
            if time_str.startswith('['):
                # 提取total部分
                total_match = re.search(r'total:\s*([^,]+)', time_str)
                if total_match:
                    time_str = total_match.group(1)

            # 去除可能的空格和特殊字符
            time_str = time_str.strip()

            # 处理小时
            if 'h' in time_str.lower():
                # 提取数字部分
                match = re.search(r'([\d\.]+)\s*h', time_str, re.IGNORECASE)
                if match:
                    hours = float(match.group(1))
                    return round(hours * 3600, 3)

            # 处理分钟
            elif 'm' in time_str.lower() and 'ms' not in time_str.lower():
                # 提取数字部分
                match = re.search(r'([\d\.]+)\s*m', time_str, re.IGNORECASE)
                if match:
                    minutes = float(match.group(1))
                    return round(minutes * 60, 3)

            # 处理秒
            elif 's' in time_str.lower() and 'ms' not in time_str.lower():
                # 提取数字部分
                match = re.search(r'([\d\.]+)\s*s', time_str, re.IGNORECASE)
                if match:
                    seconds = float(match.group(1))
                    return round(seconds, 3)

            # 处理毫秒
            elif 'ms' in time_str.lower():
                # 提取数字部分
                match = re.search(r'([\d\.]+)\s*ms', time_str, re.IGNORECASE)
                if match:
                    ms = float(match.group(1))
                    return round(ms / 1000, 6)
            else:
                return 0
        except Exception as e:
            print(f"Error parsing time '{time_str}': {e}")
            return 0

    @staticmethod
    def parse_size_to_mb(size_str):
        """
        将大小字符串转换为MB
        支持的格式:
        - "[total:822.5 MiB, ...]" -> 提取total部分
        - "2.3 GiB" -> GB转MB
        - "1026.9 KiB" -> KB转MB
        - "512 B" -> 字节转MB
        """
        try:
            # 如果是列表格式 [total:822.5 MiB, ...]
            if size_str.startswith('['):
                # 提取total部分
                total_match = re.search(r'total:\s*([^,]+)', size_str)
                if total_match:
                    size_str = total_match.group(1)

            size_str = size_str.strip()

            # 处理GB
            if 'TiB' in size_str:
                gb = float(size_str.replace('TiB', '').strip())
                return round(gb * 1024 * 1024, 3)  # 1TB = 1024GB

            # 处理GB
            elif 'GiB' in size_str:
                gb = float(size_str.replace('GiB', '').strip())
                return round(gb * 1024, 3)  # 1GB = 1024MB

            # 处理MB
            elif 'MiB' in size_str:
                mb = float(size_str.replace('MiB', '').strip())
                return round(mb, 3)

            # 处理KB
            elif 'KiB' in size_str:
                kb = float(size_str.replace('KiB', '').strip())
                return round(kb / 1024, 6)  # 1MB = 1024KB

            # 处理B
            elif 'B' in size_str:
                b = float(size_str.replace('B', '').strip())
                return round(b / (1024 * 1024), 9)  # 1MB = 1024*1024B

            else:
                return 0
        except Exception as e:
            print(f"Error parsing size '{size_str}': {e}")
            return 0

    @staticmethod
    def build_result_item(
            show_op_details,
            app_id='',
            sql_hash='',
            op_name='',
            op_inputs='',
            op_outputs='',
            op_times='',
            op_running_time='',
            op_output_sizes='',
            op_output_rows='',
            func_name='',
            func_inputs='',
            func_times='',
            spark_version='',
            error_info='',
    ) -> dict:
        """
        构建结果项字典

        参数:
            show_op_details: 是否显示详细信息
            app_id: 应用ID
            sql_hash: SQL哈希值
            op_name: 算子名称
            op_inputs: 算子输入列表
            op_outputs: 算子输出列表
            op_times: 算子出现次数
            op_running_time: 算子运行时间
            op_output_sizes: 算子输出文件大小
            op_output_rows: 算子输出行数
            func_name: 函数名称
            func_inputs: 函数输入列表
            func_times: 函数出现次数

        返回:
            构建好的结果字典
        """
        if op_inputs is None:
            op_inputs = []
        if op_outputs is None:
            op_outputs = []
        if func_inputs is None:
            func_inputs = []

        result_item = {
            'ApplicationID+SQL ID': app_id,
            'SQL Hash': sql_hash,
            'Omni不支持的算子名称': op_name,
            'Omni不支持的算子Input': ",".join(op_inputs),
            'Omni不支持的算子Output': ",".join(op_outputs),
            'Omni不支持的算子出现频次': op_times,
            'Omni不支持的算子运行时间': op_running_time,
        }

        # 仅在show_op_details为True时添加这两个字段
        if show_op_details:
            result_item['Omni不支持的算子文件大小'] = op_output_sizes
            result_item['Omni不支持的算子Output rows'] = op_output_rows

        result_item.update({
            'Omni不支持的表达式/内置函数名称': func_name,
            'Omni不支持的表达式/内置函数Input': ",".join(func_inputs),
            'Omni不支持的表达式/内置函数出现频次': func_times,
            'Spark版本': spark_version,
            '异常信息/备注': error_info
        })

        return result_item

    @staticmethod
    def split_complex_items(text):
        """使用索引而非字符串拼接的高效版本"""
        items = []
        start = 0
        level = 0

        for i, char in enumerate(text):
            if char == '(':
                level += 1
            elif char == ')':
                level -= 1
            elif char == ',' and level == 0:
                item = text[start:i].strip()
                if item:
                    items.append(item)
                start = i + 1

        # 处理最后一项
        last_item = text[start:].strip()
        if last_item:
            items.append(last_item)

        return items

    @staticmethod
    def parse_param_list(param_match, param_type_mapping):
        """
        解析输入列表，处理包含嵌套括号的复杂表达式
        :param param_match: 正则匹配结果对象
        :param param_type_mapping: 参数类型映射字典
        :return: 解析后的输入列表
        """
        if not param_match:
            return []

        param_list = []
        for item in CommonUtil.split_complex_items(param_match.group(1)):
            stripped_item = item.strip()
            if not stripped_item:
                continue

            param_type = TypeMatcher.judge_param_type(stripped_item, param_type_mapping)
            if param_type.upper().startswith("DECIMAL"):
                param_type = TypeEnum.DECIMAL.value
            param_list.append(param_type)
        return param_list