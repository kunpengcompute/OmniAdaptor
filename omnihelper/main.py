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
import csv
import os
import re
import sys
import json
import time
import hashlib
from collections import defaultdict

import pandas as pd
import argparse
import subprocess
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from omnihelper.parser.function_parser import FunctionParser
from omnihelper.parser.op_parser import OpParser
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.excel_util import ExcelWriterWithStyle


class LogParser:
    EXECUTE_PATH = CommonUtil.get_execute_path()
    TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
    TMP_PATH = os.path.join(EXECUTE_PATH, "tmp", TIMESTAMP)
    FILE_PATTERN = re.compile(r".*(application_|local-)[0-9]+(_[0-9])*.*(\\.lz4|\\.zstd)?$")
    TABLE_SCHEMA_PATH = os.path.join(EXECUTE_PATH, "resources", "spark_table_schema.csv")

    def __init__(self):
        self.parser = None  # 存储ArgumentParser
        self.args = None  # 存储命令行参数
        self.op_parser = None
        self.expr_parser = None
        self.compressed_files = []  # 存储待处理的压缩文件
        self.input_is_file = False  # 标记输入是否为文件
        self.input_file_path = None  # 如果是文件，存储文件路径
        self.excel_writer = ExcelWriterWithStyle()  # 用于表格写
        self.json_files = []  # 存储生成的json文件
        self.analysis_result = []  # 存储最终的分析结果，用于表格写入

        self._create_parser()  # 初始化命令行解析
        self._get_arguments()  # 解析命令行
        self.print_arguments()  # 打印初始化页面及参数

    def _create_parser(self):
        self.parser = argparse.ArgumentParser(
            description='Big Data Operator Scanning Command Line Tool',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Usage Examples:
  ./omnihelper -i ./input_data -o ./output_dir 
    --java-path /path/to/java/bin/java 
    --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:\
/path/to/spark-3.4.3-bin-hadoop3/jars/*

  ./omnihelper -i ./input_data/eventlog.lz4 -o ./output_dir
    --java-path /path/to/java/bin/java 
    --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:\
/path/to/spark-3.4.3-bin-hadoop3/jars/*
"""
        )

        # 必需参数
        self.parser.add_argument(
            '--input_data', '-i',
            type=str,
            required=True,
            help='Input directory path or single file path (required). '
                 'If a single .lz4 or .zstd file is provided, only that file will be processed.'
        )

        # 可选参数
        self.parser.add_argument(
            '--output_dir', '-o',
            type=str,
            default=None,
            help='Output directory path (default: ./output)'
        )

        # 可选参数，无需显式传值
        self.parser.add_argument(
            '--show-op-details', '-s',
            action='store_false',  # 默认为 True，传参时设为 False
            help='Disable displaying op file sizes and output rows'
        )

        # Java相关参数组
        java_group = self.parser.add_argument_group('Java Configuration')

        # Java可执行文件路径
        java_group.add_argument(
            '--java-path',
            type=str,
            default="java",
            help='Java executable path (default: "java" from system PATH)'
        )

        # Java Class 路劲
        java_group.add_argument(
            '--class-path',
            type=str,
            required=True,
            help='Complete Java classpath string'
        )

    def _get_arguments(self):
        self.args = self.parser.parse_args()

        # 检查输入是文件还是目录
        if os.path.isfile(self.args.input_data):
            self.input_is_file = True
            self.input_file_path = os.path.realpath(self.args.input_data)

            # 检查文件扩展名
            filename_lower = os.path.basename(self.args.input_data).lower()

            if not self.FILE_PATTERN.match(filename_lower):
                self.parser.error(
                    f"Provided file invalid: {os.path.basename(self.args.input_data)}"
                )
        elif os.path.isdir(self.args.input_data):
            self.input_is_file = False
        else:
            self.parser.error(f"Input path is neither a file nor a directory: {self.args.input_data}")

        # 输出目录默认值
        if self.args.output_dir is None:
            self.args.output_dir = os.path.join(self.EXECUTE_PATH, "output")

        os.makedirs(self.args.output_dir, exist_ok=True)
        os.makedirs(self.TMP_PATH, exist_ok=True)

    def print_arguments(self):
        # 打印配置信息
        print("=" * 60)
        print("  Big Data Operator Scanning Tool")
        print("=" * 60)

        if self.input_is_file:
            print(f"Input File: {self.input_file_path}")
        else:
            print(f"Input Directory: {os.path.realpath(self.args.input_data)}")

        print(f"Output Directory: {os.path.realpath(self.args.output_dir)}")
        print(f"Temporary Directory: {os.path.realpath(self.TMP_PATH)}")
        print(f"Show Op Details: {self.args.show_op_details}")
        print(f"Java Path: {self.args.java_path}")
        print(f"Java Class Path: {self.args.class_path}")
        print("-" * 60)

    def parse_single_file(self, input_file_path: str, output_file_path: str, filename: str):
        cmd = [
            self.args.java_path,
            "-cp",
            self.args.class_path,
            "org.apache.spark.deploy.history.ParseLog",
            input_file_path,
            output_file_path,
            filename
        ]
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
            stdout, stderr = process.communicate()
            return process.returncode == 0, stdout, stderr
        except Exception as e:
            return False, "", str(e)

    def find_compressed_files(self):
        """查找目录/文件中的所有.lz4和.zstd文件"""
        if self.input_is_file:
            # 处理单个文件
            input_file_path = os.path.dirname(self.input_file_path)
            filename = os.path.basename(self.input_file_path)
            output_file_path = self.TMP_PATH

            self.compressed_files.append({
                "input_file_path": input_file_path,
                "output_file_path": output_file_path,
                "filename": filename
            })
        else:
            # 处理目录
            for root, _, files in os.walk(self.args.input_data):
                for filename in files:
                    if self.FILE_PATTERN.match(filename.lower()):
                        input_file_path = Path(root)
                        output_file_path = self.TMP_PATH / Path(root).relative_to(self.args.input_data)
                        self.compressed_files.append({
                            "input_file_path": os.path.realpath(input_file_path),
                            "output_file_path": os.path.realpath(output_file_path),
                            "filename": filename
                        })

    def get_execution_plan(self):
        failed_files = []  # 存储失败的文件信息
        print("Start parsing event log...")

        with tqdm(total=len(self.compressed_files), desc="Processing ") as pbar:
            for compressed_file in self.compressed_files:
                input_file_path = compressed_file["input_file_path"]
                output_file_path = compressed_file["output_file_path"]
                filename = compressed_file["filename"]
                pbar.set_description(f"Processing: {filename[:40]}{'...' if len(filename) > 40 else ''}")
                os.makedirs(output_file_path, exist_ok=True)
                success, stdout, stderr = self.parse_single_file(input_file_path, output_file_path, filename)
                if not success:
                    failed_files.append((os.path.join(input_file_path, filename), stderr))
                else:
                    self.json_files.append(os.path.join(output_file_path, filename + ".json"))
                pbar.update(1)
            pbar.close()  # 确保关闭

        CommonUtil.print_failed_files(failed_files, len(self.compressed_files))

        return len(self.compressed_files) == len(failed_files)

    @classmethod
    def read_table_schema(cls):
        """
        解析Spark导出的CSV表结构文件
        """
        table_schema = defaultdict(list)
        if not os.path.exists(cls.TABLE_SCHEMA_PATH):
            return table_schema

        with open(cls.TABLE_SCHEMA_PATH, "r") as f:
            reader = csv.DictReader(f)
            required_columns = {'full_table_name', 'column_name', 'data_type'}
            if not required_columns.issubset(set(reader.fieldnames)):
                print(f"Failed to read spark_table_schema.csv.")
                return table_schema
            for row in reader:
                table_name = row.get("full_table_name")
                col_name = row.get("column_name")
                col_type = row.get("data_type")
                if not table_name or not col_name or not col_type:
                    continue
                column_info = {
                    "column_name": col_name,
                    "data_type": col_type
                }
                table_schema[table_name].append(column_info)

        return table_schema

    @classmethod
    def get_column_type(cls, table_schema, physical_plan):
        """
        获取物理执行计划中使用的所有表的列类型
        """
        column_type = {}
        patterns = [
            r"Scan hive (\w+)\.(\w+)",
            r"Scan orc (\w+)\.(\w+)",
            r"InsertIntoHiveTable [`]?(\w+)[`]?[.]`?(\w+)`?",
            r"FileScan \w+ (\w+)\.(\w+)",
            r"Scan JDBCRelation\((\w+)\.(\w+)\)",
            r"Scan delta (\w+)\.(\w+)",
            r"Scan (\w+)\.(\w+) \["
        ]
        for pattern in patterns:
            matches = re.findall(pattern, physical_plan)
            for match in matches:
                db_name = match[0]
                table_name = match[1]
                if db_name.lower() in ("local", "system", "info"):
                    continue
                for column_info in table_schema.get(f"{db_name.lower()}.{table_name.lower()}", []):
                    column_type[column_info["column_name"].lower()] = column_info["data_type"]
        return column_type

    def parse_json_file(self, file_path: str, application_id):
        try:
            with open(file_path, "r") as f:
                app_data = json.load(f)
        except Exception as e:
            return False, f"Failed to load json file: {file_path}, ex: {e}"

        table_schema = {}
        try:
            table_schema = self.read_table_schema()
        except Exception as e:
            print(f"Failed to read spark_table_schema.csv: {e}")

        try:
            analysis_result = []
            for event in app_data:
                # 获取物理执行计划中使用的所有表的列类型
                column_type = self.get_column_type(table_schema, event.get("physical plan"))
                # 获取sql_hash和app_id信息
                sql_hash = hashlib.sha256(event.get("original query").encode("utf-8")).hexdigest()[-6:]
                app_id = application_id + "_" + event.get("executionId")
                contain_omni_op, op_event_result = self.op_parser.parse_event(event, column_type)
                if contain_omni_op:
                    result_item = CommonUtil.build_result_item(self.args.show_op_details, application_id,
                                                               error_info=f"Json file contains omni op")
                    self.analysis_result.append(result_item)
                    return True, f"Json file contains omni op: {file_path}"
                expr_event_result = self.expr_parser.parse_event(event, column_type)
                if not expr_event_result and not op_event_result:
                    continue
                max_list_length = max(len(op_event_result), len(expr_event_result))
                for i in range(max_list_length):
                    func_name = expr_event_result[i].get('func_name', '') if i < len(expr_event_result) else ''
                    func_inputs = expr_event_result[i].get('input', []) if i < len(expr_event_result) else []
                    not_supported_line = expr_event_result[i].get('not_supported_line', []) if i < len(expr_event_result) else []
                    func_times = expr_event_result[i].get('times', 0) if i < len(expr_event_result) else ''
                    is_udf = ''
                    if i < len(expr_event_result):
                        is_udf = "是" if expr_event_result[i].get('is_udf') else "否"

                    op_name = op_event_result[i].get('op_name', '') if i < len(op_event_result) else ''
                    op_inputs = op_event_result[i].get('input_list', []) if i < len(op_event_result) else []
                    op_outputs = op_event_result[i].get('output_list', []) if i < len(op_event_result) else []
                    op_times = op_event_result[i].get('times', 0) if i < len(op_event_result) else ''
                    op_running_time = op_event_result[i].get('running_time', '') if i < len(op_event_result) else ''
                    op_output_rows = op_event_result[i].get('output_rows', 0) if i < len(op_event_result) else ''
                    op_output_sizes = op_event_result[i].get('output_sizes', 0) if i < len(op_event_result) else ''

                    result_item = CommonUtil.build_result_item(self.args.show_op_details, app_id, sql_hash,
                                                               op_name, op_inputs, op_outputs, op_times,
                                                               op_running_time, op_output_sizes, op_output_rows,
                                                               func_name, func_inputs, not_supported_line,
                                                               func_times, is_udf)
                    analysis_result.append(result_item)

            self.analysis_result.extend(analysis_result)
        except Exception as e:
            return False, f"Failed to parse json file: {file_path}, ex: {e}"
        return True, ""

    def parse_event_log(self):
        failed_json_files = []  # 存储失败的文件信息
        print("Start parsing expr and op...")

        try:
            self.op_parser = OpParser()
            self.expr_parser = FunctionParser()
        except Exception as e:
            print(f"Failed to initial parser caused by: \n\t{e}")
            return

        sys.stdout.flush()  # 强制刷新缓冲区
        time.sleep(0.1)  # 短暂延迟
        with tqdm(total=len(self.json_files), desc="Processing2 ") as pbar:
            for file_path in self.json_files:
                filename = os.path.basename(file_path)
                application_id = filename.split(".")[0]
                pbar.set_description(f"Processing: {filename[:40]}{'...' if len(filename) > 40 else ''}")

                success, stdout = self.parse_json_file(file_path, application_id)
                if not success:
                    failed_json_files.append((file_path, stdout))
                pbar.update(1)
            pbar.close()  # 确保关闭

        CommonUtil.print_failed_files(failed_json_files, len(self.json_files))

        if not self.analysis_result:
            print("Result is empty, No data to display.")
            return

        df = pd.DataFrame(self.analysis_result)
        output_excel_path = os.path.join(self.args.output_dir, f"Omni_Analysis_All_Report_{self.TIMESTAMP}.xlsx")
        self.excel_writer.write_to_excel(df, output_excel_path, self.args.show_op_details)


def main():
    logparser = LogParser()
    logparser.find_compressed_files()
    is_all_failed = logparser.get_execution_plan()
    if is_all_failed:
        return
    logparser.parse_event_log()
    print("-" * 60)


if __name__ == "__main__":
    main()