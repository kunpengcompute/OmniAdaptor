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
import json
import time
import hashlib
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


def check_and_get_architecture():
    """
    检查并获取系统架构，如果不支持则退出程序

    Returns:
    --------
    str: 系统架构 ('x86' 或 'arm')
    """
    success, arch = CommonUtil.get_architecture()

    if not success:
        print(f"[ERROR] Unsupported CPU architecture: {arch}")
        print("[ERROR] Currently only x86 (x86_64/amd64) and ARM (aarch64/arm64) architectures are supported")
        sys.exit(1)

    return arch


class LogParser:

    ARCHITECTURE = check_and_get_architecture()
    EXECUTE_PATH = CommonUtil.get_execute_path()
    TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
    TMP_PATH = os.path.join(EXECUTE_PATH, "tmp", TIMESTAMP)
    FILE_PATTERN = re.compile(r".*(application_|local-)[0-9]+(_[0-9])*.*(\\.lz4|\\.zstd)?$")

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
            epilog=f"""
Usage Examples:
  ./omnihelper_{self.ARCHITECTURE} -i ./input_data -o ./output_dir 
    --java-path /path/to/java/bin/java 
    --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:\
/path/to/boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar:\
/path/to/spark-3.4.3-bin-hadoop3/jars/*

  ./omnihelper_{self.ARCHITECTURE} -i ./input_data/eventlog.lz4 -o ./output_dir
    --java-path /path/to/java/bin/java 
    --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:\
/path/to/boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar:\
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

    def parse_json_file(self, file_path: str, application_id):
        try:
            with open(file_path, "r") as f:
                app_data = json.load(f)
        except Exception as e:
            return False, f"Failed to load json file: {file_path}, ex: {e}"

        try:
            for event in app_data:
                # 获取sql_hash和app_id信息
                sql_hash = hashlib.sha256(event.get("original query").encode("utf-8")).hexdigest()[-6:]
                app_id = application_id + "_" + event.get("executionId")
                op_event_result = self.op_parser.parse_event(event)
                expr_event_result = self.expr_parser.parse_event(event)
                if not expr_event_result and not op_event_result:
                    continue
                max_list_length = max(len(op_event_result), len(expr_event_result))
                for i in range(max_list_length):
                    func_name = expr_event_result[i].get('func_name', '') if i < len(expr_event_result) else ''
                    func_inputs = expr_event_result[i].get('input', []) if i < len(expr_event_result) else []
                    func_times = expr_event_result[i].get('times', 0) if i < len(expr_event_result) else ''

                    op_name = op_event_result[i].get('op_name', '') if i < len(op_event_result) else ''
                    op_inputs = op_event_result[i].get('input_list', []) if i < len(op_event_result) else []
                    op_outputs = op_event_result[i].get('output_list', []) if i < len(op_event_result) else []
                    op_times = op_event_result[i].get('times', 0) if i < len(op_event_result) else ''

                    self.analysis_result.append(
                        {
                            'ApplicationID+SQL ID': app_id,
                            'SQL Hash': sql_hash,
                            'Omni不支持的算子名称': op_name,
                            'Omni不支持的算子Input': ",".join(op_inputs),
                            'Omni不支持的算子Output': ",".join(op_outputs),
                            'Omni不支持的算子出现频次': op_times,
                            'Omni不支持的算子运行时间': '',
                            'Omni不支持的算子Output rows': '',
                            'Omni不支持的表达式/内置函数名称': CommonUtil.safe_excel_value(func_name),
                            'Omni不支持的表达式/内置函数Input': ",".join(func_inputs),
                            'Omni不支持的表达式/内置函数出现频次': func_times,
                            'Spark版本': '',  # 原始数据中没有Spark版本信息
                            '异常信息/备注': ''  # 添加函数名作为备注
                        }
                    )
        except Exception as e:
            return False, f"Failed to parse json file: {file_path}, ex: {e}"
        return True, ""

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

        df = pd.DataFrame(self.analysis_result)
        output_excel_path = os.path.join(self.args.output_dir, f"Omni_Analysis_All_Report_{self.TIMESTAMP}.xlsx")
        self.excel_writer.write_to_excel(df, output_excel_path)


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