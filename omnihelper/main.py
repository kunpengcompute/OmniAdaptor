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
import argparse

from omnihelper.spark_log_parser import LogParser
from omnihelper.flink_log_parser import FlinkLogParser


def _create_spark_parser(subparsers):
    """创建 Spark 子命令的参数解析器"""
    spark_parser = subparsers.add_parser('spark', help='Spark log analysis')

    spark_parser.add_argument(
        '--input_data', '-i',
        type=str,
        required=True,
        help='Input directory path or single file path (required). '
             'If a single .lz4 or .zstd file is provided, only that file will be processed.'
    )

    spark_parser.add_argument(
        '--output_dir', '-o',
        type=str,
        default=None,
        help='Output directory path (default: ./output)'
    )

    spark_parser.add_argument(
        '--show-op-details', '-s',
        action='store_false',
        help='Disable displaying op file sizes and output rows'
    )

    java_group = spark_parser.add_argument_group('Java Configuration')

    java_group.add_argument(
        '--java-path',
        type=str,
        default="java",
        help='Java executable path (default: "java" from system PATH)'
    )

    java_group.add_argument(
        '--class-path',
        type=str,
        required=True,
        help='Complete Java classpath string'
    )

    return spark_parser


def _create_flink_parser(subparsers):
    """创建 Flink 子命令的参数解析器"""
    flink_parser = subparsers.add_parser('flink', help='Flink log analysis')

    flink_parser.add_argument(
        '--url', '-u',
        type=str,
        required=True,
        help='Flink dashboard URL (required), e.g., http://127.0.0.1:8081 or https://127.0.0.1:8081'
    )

    flink_parser.add_argument(
        '--interval', '-i',
        type=int,
        default=100,
        help='API call interval in milliseconds (default: 1000)'
    )

    flink_parser.add_argument(
        '--timeout', '-t',
        type=int,
        default=30,
        help='API call timeout in seconds (default: 30)'
    )

    flink_parser.add_argument(
        '--output_dir', '-o',
        type=str,
        default=None,
        help='Output directory path (default: ./output)'
    )

    flink_parser.add_argument(
        '--show-op-details', '-s',
        action='store_false',
        help='Disable displaying op file sizes and output rows'
    )

    flink_parser.add_argument(
        '--no-ssl-verify',
        action='store_true',
        default=False,
        help='Skip SSL certificate verification (default: False, i.e., verify SSL by default)'
    )

    return flink_parser


def main():
    parser = argparse.ArgumentParser(
        description='Big Data Operator Scanning Command Line Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage Examples:
  Spark Log Analysis:
    ./omnihelper spark -i ./input_data -o ./output_dir \\
      --java-path /path/to/java/bin/java \\
      --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*

  Flink Log Analysis:
    ./omnihelper flink --url http://127.0.0.1:8081
    ./omnihelper flink -u https://127.0.0.1:8081 -o ./output_dir
"""
    )

    subparsers = parser.add_subparsers(dest='command', help='Subcommands')
    subparsers.required = True

    _create_spark_parser(subparsers)
    _create_flink_parser(subparsers)

    args = parser.parse_args()

    if args.command == 'spark':
        logparser = LogParser(args)
        logparser.find_compressed_files()
        is_all_failed = logparser.get_execution_plan()
        if is_all_failed:
            return
        logparser.parse_event_log()
        print("-" * 60)
    elif args.command == 'flink':
        flink_log_parser = FlinkLogParser(args)
        flink_log_parser.analyze_flink_logs()
        flink_log_parser.generate_report()
        print("-" * 60)


if __name__ == "__main__":
    main()
