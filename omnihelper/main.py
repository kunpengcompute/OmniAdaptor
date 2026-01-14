import os
import argparse
import subprocess
from tqdm import tqdm
from pathlib import Path


class LogParser:
    def __init__(self):
        self.parser = None
        self.args = None
        self.compressed_files = []

        self._create_parser()
        self._get_arguments()

    def _create_parser(self):
        self.parser = argparse.ArgumentParser(
            description='Big Data Operator Scanning Command Line Tool',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Usage Examples:
  python script.py -i ./input_data -o ./output_data 
    --java-path /path/to/java/bin/java 
    --class-path "/path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar;\
/path/to/boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar;\
/path/to/spark-3.4.3-bin-hadoop3/jars/*"
"""
        )

        # 必需参数
        self.parser.add_argument(
            '--input_dir', '-i',
            type=Path,
            required=True,
            help='Input directory path (required)'
        )

        # 可选参数
        self.parser.add_argument(
            '--output_dir', '-o',
            type=Path,
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
            type=Path,
            required=True,
            help='Complete Java classpath string'
        )

    def _get_arguments(self):
        self.args = self.parser.parse_args()

        # 验证参数
        if not os.path.exists(self.args.input_dir):
            self.parser.error(f"Input directory does not exist: {self.args.input_dir}")

        # 输出目录默认值
        if self.args.output_dir is None:
            self.args.output_dir = os.path.join(os.getcwd(), "output")

    def print_arguments(self):
        # 打印配置信息
        print("=" * 60)
        print("  Big Data Operator Scanning Tool")
        print("=" * 60)
        print(f"Input Directory:  {self.args.input_dir}")
        print(f"Output Directory: {self.args.output_dir}")
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
        """查找目录中的所有.lz4和.zstd文件"""
        for root, _, files in os.walk(self.args.input_dir):
            for filename in files:
                if filename.endswith(('.lz4', '.zstd')):
                    input_file_path = Path(root)
                    output_file_path = self.args.output_dir / Path(root).relative_to(self.args.input_dir)
                    self.compressed_files.append({
                        "input_file_path": input_file_path,
                        "output_file_path": output_file_path,
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
                    failed_files.append((filename, stderr))
                pbar.update(1)

        # 统一展示失败结果
        if failed_files:
            print(f"Processing completed with {len(failed_files)} failed files:")
            for i, (filename, error) in enumerate(failed_files, 1):
                print(f"{i}. [File]: {filename}")
                print(f"   [Error]: {error}")
            print(f"\nSummary: {len(self.compressed_files) - len(failed_files)} succeeded, {len(failed_files)} failed")
        else:
            print(f"\nAll {len(self.compressed_files)} files processed successfully!")
        print("-" * 60)


def main():
    logparser = LogParser()
    logparser.print_arguments()
    logparser.find_compressed_files()
    logparser.get_execution_plan()


if __name__ == "__main__":
    main()