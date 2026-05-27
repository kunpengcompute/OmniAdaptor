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
import html
from datetime import datetime
import urllib.parse
import pandas as pd

from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger
from omnihelper.util.flink_excel_util import FlinkExcelWriterWithStyle
from omnihelper.flink.flink_request import FlinkRequester
from omnihelper.flink.operator.op_parse import FlinkParser
from omnihelper.flink.schema.table_schema_reader import TableSchemaReader
from omnihelper.constants.flink_constants import TaskStatus, ExcelColumns, MetricType


class FlinkLogParser:
    EXECUTE_PATH = CommonUtil.get_execute_path()
    TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, args):
        self.args = args  # 存储命令行参数
        self.requester = None
        self.excel_writer = FlinkExcelWriterWithStyle()  # 用于表格写
        self.analysis_result = []  # 存储最终的分析结果，用于表格写入
        self.parser = FlinkParser()
        self.target_metrics = MetricType.get_target_metrics()
        self.table_schema = {}
        self.column_type = {}
        self.table_column_type = {}
        self.args_valid = self._get_arguments()
        if self.args_valid:
            self._load_table_schema()
            self.print_arguments()

    @staticmethod
    def _get_description(vid, plan_nodes):
        """获取节点描述信息"""
        desc = plan_nodes.get(vid, {}).get('description', '')
        return html.unescape(desc) if desc else ''

    @staticmethod
    def _parse_description_data(description):
        if not description:
            return []
        raw_parts = re.split(r"<br/>[\s:*\+\-]*|\n", description)
        return [
            parsed_line for line in raw_parts
            if (parsed_line := FlinkParser.parse_single_description_line(line)) is not None
        ]

    @staticmethod
    def _create_vertex_result(vid, jid, vertex, status, description, stats, description_data=None, upstream_ids=None):
        if status != TaskStatus.SUCCESS:
            logger.warning(f"Vertex {vid} in job {jid} status: {status}")

        result = {
            "status": status,
            "vertex_name": vertex.get("name"),
            "logic_metadata": {
                "full_description": description
            },
            "summary_metrics": stats
        }

        if description_data is not None:
            result["description_data"] = description_data

        if upstream_ids is not None:
            result["upstream_ids"] = upstream_ids

        return result

    def _load_table_schema(self):
        csv_path = getattr(self.args, 'input_data', None)
        if not csv_path:
            default_path = os.path.join(self.EXECUTE_PATH, "resources", "flink_table_schema.csv")
            if os.path.exists(default_path):
                csv_path = default_path
                logger.info(f"Using default table schema CSV: {default_path}")
            else:
                logger.info("No table schema CSV provided and default not found, type resolution will be limited")
                return

        if not os.path.exists(csv_path):
            logger.warning(f"Table schema CSV file not found: {csv_path}")
            return

        self.table_schema = TableSchemaReader.read_table_schema(csv_path)
        if self.table_schema:
            self.column_type, self.table_column_type = TableSchemaReader.build_column_type_mapping(
                self.table_schema
            )
            self.parser.set_table_schema(self.table_schema, self.column_type, self.table_column_type)
            logger.info(f"Loaded table schema: {len(self.table_schema)} tables, "
                        f"{len(self.column_type)} column types")

    def _get_arguments(self):
        # 输出目录默认值
        if self.args.output_dir is None:
            self.args.output_dir = os.path.join(self.EXECUTE_PATH, "output")

        os.makedirs(self.args.output_dir, exist_ok=True)

        # 1. 校验 URL 参数
        if not self._validate_url():
            return False

        # 2. 校验数值参数
        if not self._validate_numeric_args():
            return False

        # 3. 校验其他参数
        if not self._validate_other_args():
            return False

        # 处理 SSL 验证参数
        self.args.ssl_verify = not getattr(self.args, 'no_ssl_verify', False)

        # 解析自定义请求头
        self.args.parsed_headers = self._parse_headers()
        if self.args.parsed_headers is None:
            return False

        return True

    def _validate_url(self):
        """校验 URL 参数"""
        url = self.args.url
        try:
            parsed_url = urllib.parse.urlparse(url)

            # 校验协议
            if not parsed_url.scheme:
                # 如果没有协议，直接报错
                print("Error: Invalid URL: missing scheme. Please include http:// or https://.")
                return False
            elif parsed_url.scheme not in ['http', 'https']:
                print(f"Error: Invalid URL scheme: {parsed_url.scheme}. Only http and https are supported.")
                return False

            # 校验主机名
            if not parsed_url.netloc:
                print("Error: Invalid URL: missing host and port.")
                return False

            # 提取 host, port, use_https 信息，保持向后兼容
            self.args.host = parsed_url.hostname
            self.args.port = parsed_url.port
            if self.args.port is None:
                self.args.port = 80 if parsed_url.scheme == 'http' else 443
            self.args.use_https = parsed_url.scheme == 'https'

            # 校验端口范围
            if self.args.port < 1 or self.args.port > 65535:
                print(f"Error: Invalid port: {self.args.port}. Port must be between 1 and 65535.")
                return False
            return True
        except Exception as e:
            print(f"Error: Invalid URL format: {e}. Please check your URL and try again.")
            return False

    def _validate_numeric_args(self):
        """校验数值参数"""
        # 校验 interval
        if not isinstance(self.args.interval, int) or self.args.interval < 0 or self.args.interval > 30000:
            print(
                f"Error: Invalid interval: {self.args.interval}. Interval must be an integer between 0 and 30000 (ms).")
            return False

        # 校验 timeout
        if not isinstance(self.args.timeout, int) or self.args.timeout < 1 or self.args.timeout > 300:
            print(f"Error: Invalid timeout: {self.args.timeout}. Timeout must be an integer between 1 and 300 (s).")
            return False
        return True

    def _validate_other_args(self):
        """校验其他参数"""
        # 校验 jobid
        if self.args.jobid:
            for jobid in self.args.jobid:
                if not jobid or not isinstance(jobid, str):
                    print("Error: Invalid jobid: jobid must be a non-empty string.")
                    return False

        # 校验 output_dir 路径格式（基本校验）
        if self.args.output_dir:
            if not isinstance(self.args.output_dir, str):
                print("Error: Invalid output_dir: must be a string.")
                return False
            # 确保路径可以创建
            try:
                os.makedirs(self.args.output_dir, exist_ok=True)
            except Exception as e:
                print(f"Error: Invalid output_dir: {e}.")
                return False
        return True

    def _parse_headers(self):
        """解析 --header 参数为字典"""
        raw_headers = getattr(self.args, 'header', None)
        if not raw_headers:
            return {}
        headers = {}
        for h in raw_headers:
            if ':' not in h:
                print(f"Error: Invalid header format: '{h}'. Expected 'Key: Value'.")
                return None
            key, _, value = h.partition(':')
            key = key.strip()
            value = value.strip()
            if not key:
                print(f"Error: Invalid header format: '{h}'. Header key cannot be empty.")
                return None
            headers[key] = value
        return headers

    def print_arguments(self):
        # 打印配置信息
        print("=" * 60)
        print("  Flink Log Analysis Tool")
        print("=" * 60)

        print(f"Flink Dashboard URL: {self.args.url}")
        print(f"API Call Interval: {self.args.interval} ms")
        print(f"API Call Timeout: {self.args.timeout} s")
        print(f"SSL Verify: {self.args.ssl_verify}")
        print(f"Kerberos Auth: {getattr(self.args, 'kerberos', False)}")
        if getattr(self.args, 'kerberos', False):
            print(f"Kerberos Mutual Auth: {getattr(self.args, 'kerberos_mutual_auth', 'OPTIONAL')}")
        print(f"Output Directory: {os.path.realpath(self.args.output_dir)}")
        print(f"Show Op Details: {self.args.show_op_details}")
        print(f"Table Schema CSV: {getattr(self.args, 'input_data', 'Not provided')}")
        if self.table_schema:
            print(f"Tables Loaded: {len(self.table_schema)}")
        print("-" * 60)

    def fetch_metrics(self, jid, vid, metrics, batch_size=10):
        results = []
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i + batch_size]
            metric_values = self.requester.get_vertex_metrics(jid, vid, batch) if batch else []
            if metric_values:
                results.extend(metric_values)
        return results

    def _get_job_ids(self, job_ids):
        if job_ids is not None:
            logger.info(f"Using provided job IDS: {job_ids}")
            return job_ids
        overview = self.requester.get_jobs_overview()
        if not overview:
            logger.warning("No jobs overview data received")
            return []
        all_jobs = overview.get('jobs', [])
        return [j['jid'] for j in all_jobs]

    def _process_job(self, jid):
        detail = self.requester.get_job_detail(jid)
        if not detail:
            logger.warning(f"Failed to get detail for job {jid}, error: {self.requester.last_error}")
            return None
        plan = detail.get("plan", "")
        if not plan:
            logger.warning(f"Failed to get plan for job {jid}")
            return None
        job_name = detail.get('name', 'Unknown')
        plan_nodes = {node['id']: node for node in plan.get('nodes', [])}
        vertices = {}
        for vertex in detail.get("vertices", []):
            vid = vertex["id"]
            res = self._process_vertex(vid, vertex, plan_nodes, jid, detail)
            # 如果 res 是 None，给它一个带状态的占位字典
            vertices[vid] = res if res is not None else {
                "status": res.get("status", TaskStatus.VERTEX_METRICS_FAILED),
                "vertex_name": vertex.get("name"),
                "logic_metadata": {"full_description": ""},
                "summary_metrics": {"operators": {}, "summary": {}, "analysis": {}}
            }
        return {
            "job_name": job_name,
            "vertices": vertices
        }

    def _process_vertex(self, vid, vertex, plan_nodes, jid, detail):
        """处理单个 vertex 的解析"""
        # 预定义空指标结构，避免重复书写
        metrics = self._get_vertex_metrics(vid, jid)
        description = self._get_description(vid, plan_nodes)
        description_data = self._parse_description_data(description)
        plan_node = plan_nodes.get(vid, {})
        raw_inputs = plan_node.get("inputs", [])
        upstream_ids = []
        for inp in raw_inputs:
            if isinstance(inp, dict):
                upstream_ids.append(inp.get("id", ""))
            elif isinstance(inp, str):
                upstream_ids.append(inp)
        error_status = metrics.get("error_status")
        metric_values = metrics.get("values")
        empty_payload = {"operators": {}, "summary": {}, "analysis": {}}
        # 统一错误处理逻辑
        # 如果有显式错误状态，或者没有获取到具体的 values，则判定为失败
        if error_status or not metric_values:
            status = error_status or TaskStatus.VERTEX_METRICS_FAILED
            return self._create_vertex_result(
                vid, jid, vertex, status, description, empty_payload, description_data,upstream_ids
            )
        try:
            stats = self._parse_performance_stats(vid, metrics["values"], detail, jid)
            status = TaskStatus.SUCCESS
        except Exception as e:
            logger.error(f"Failed to parse performance stats for vertex {vid}: {e}")
            stats = {"operators": {}, "summary": {}, "analysis": {}}
            status = TaskStatus.OPERATOR_PARSE_FAILED

        return self._create_vertex_result(vid, jid, vertex, status, description, stats, description_data, upstream_ids)

    def _parse_performance_stats(self, vid, metrics_values, detail, jid):
        """解析性能统计数据"""
        return self.parser.parse_performance_stats(vid, metrics_values,
                                                   self.parser.get_description(detail, jid))

    def _get_vertex_metrics(self, vid, jid):
        available = self.requester.get_vertex_metrics(jid, vid)
        if not available:
            logger.warning(f"No metrics available for vertex {vid} in job {jid}, error: {self.requester.last_error}")
            # 返回具体的错误状态
            error_status = self.requester.last_error or TaskStatus.VERTEX_METRICS_FAILED

            return {"error_status": error_status, "ids": [], "values": []}

        needed_ids = self.parser.filter_num_data(available, self.target_metrics)
        # 当 show_op_details 为 False 时，过滤掉 runtime、numBytesIn、numBytesOut 相关的指标 ID，
        # 减少下游 fetch_metrics 的 API 批量请求
        if not getattr(self.args, 'show_op_details', True) and needed_ids:
            # 假设 MetricType 里的 key 或 Flink 原始指标 ID 包含如下特征关键字，将其踢出请求队列
            exclude_keywords = ["runtime", "BytesIn", "BytesOut"]
            needed_ids = [
                m_id for m_id in needed_ids
                if not any(kw in m_id for kw in exclude_keywords)
            ]
            logger.debug(
                f"Optimization: show-op-details is disabled. Filtered API metrics batch to {len(needed_ids)} items.")

        if not needed_ids:
            logger.warning(f"No needed metrics found for vertex {vid} in job {jid}")
        return {
            "ids": needed_ids,
            "values": self.fetch_metrics(jid, vid, needed_ids) if needed_ids else []
        }

    def analyze_flink_logs(self):
        """
        实现 Flink 日志分析的核心功能
        """
        if not self.args_valid:
            print("Error: Invalid arguments. Please check your input and try again.")
            return

        self.requester = FlinkRequester(
            url=self.args.url,
            timeout=self.args.timeout,
            ssl_verify=self.args.ssl_verify,
            interval=self.args.interval,
            kerberos=getattr(self.args, 'kerberos', False),
            kerberos_mutual_auth=getattr(self.args, 'kerberos_mutual_auth', 'OPTIONAL'),
            headers=getattr(self.args, 'parsed_headers', {}),
        )

        full_report = {}
        jobs_ids = self._get_job_ids(self.args.jobid)
        for jid in jobs_ids:
            job_data = self._process_job(jid)
            if job_data:
                full_report[jid] = job_data

        self.analysis_result = self.parser.parse_job_data(full_report)
        logger.info(f"Generated report with {len(self.analysis_result)}")

    def generate_report(self):
        """
        生成分析报告
        """
        if not self.analysis_result:
            print("Result is empty, No data to display.")
            return

        # 定义输出列顺序（包含重复列名）
        output_columns = [
            ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
            ExcelColumns.OPERATOR_NAME, ExcelColumns.INPUT, ExcelColumns.OUTPUT,
            ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME, ExcelColumns.INPUT_DATA_SIZE,
            ExcelColumns.OUTPUT_DATA_SIZE, ExcelColumns.FUNC_NAME, ExcelColumns.INPUT,
            ExcelColumns.NESTED_CONTENT, ExcelColumns.FREQUENCY
        ]

        # 处理重复列名：临时列名映射
        temp_columns = [
            ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
            ExcelColumns.OPERATOR_NAME, ExcelColumns.INPUT, ExcelColumns.OUTPUT,
            ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME, ExcelColumns.INPUT_DATA_SIZE,
            ExcelColumns.OUTPUT_DATA_SIZE, ExcelColumns.FUNC_NAME,
            f"{ExcelColumns.INPUT}_2", ExcelColumns.NESTED_CONTENT,
            f"{ExcelColumns.FREQUENCY}_2"
        ]

        # 数据字段映射：原始字段 -> 临时列名
        field_mapping = [
            (ExcelColumns.JOB_ID, ExcelColumns.JOB_ID),
            (ExcelColumns.TASK_ID, ExcelColumns.TASK_ID),
            (ExcelColumns.STATUS, ExcelColumns.STATUS),
            (ExcelColumns.OPERATOR_NAME, ExcelColumns.OPERATOR_NAME),
            (ExcelColumns.INPUT, ExcelColumns.INPUT),
            (ExcelColumns.OUTPUT, ExcelColumns.OUTPUT),
            (ExcelColumns.FREQUENCY, ExcelColumns.FREQUENCY),
            (ExcelColumns.RUNTIME, ExcelColumns.RUNTIME),
            (ExcelColumns.INPUT_DATA_SIZE, ExcelColumns.INPUT_DATA_SIZE),
            (ExcelColumns.OUTPUT_DATA_SIZE, ExcelColumns.OUTPUT_DATA_SIZE),
            (ExcelColumns.FUNC_NAME, ExcelColumns.FUNC_NAME),
            (ExcelColumns.FUNC_INPUT, f"{ExcelColumns.INPUT}_2"),
            (ExcelColumns.NESTED_CONTENT, ExcelColumns.NESTED_CONTENT),
            (ExcelColumns.FUNC_FREQUENCY, f"{ExcelColumns.FREQUENCY}_2"),
        ]

        # 处理数据
        processed_data = self._process_report_data(field_mapping)

        # 创建 DataFrame
        df = pd.DataFrame(processed_data, columns=temp_columns)

        # 重命名列，将临时后缀去掉，实现重复列名
        df.columns = output_columns

        output_excel_path = os.path.join(self.args.output_dir, f"Omni_Analysis_All_Report_{self.TIMESTAMP}.xlsx")
        self.excel_writer.write_to_excel(df, output_excel_path)

    def _process_report_data(self, field_mapping):
        """处理报告数据，按照字段映射转换"""
        show_op_details = getattr(self.args, 'show_op_details', True)
        # 算子 OUTPUT列数据不输出
        exclude_fields = {ExcelColumns.OUTPUT}
        if not show_op_details:
            exclude_fields.update({
                ExcelColumns.RUNTIME,
                ExcelColumns.INPUT_DATA_SIZE,
                ExcelColumns.OUTPUT_DATA_SIZE,
            })

        return [
            {
                # 如果当前字段在黑名单中，直接赋空字符串 ""，否则从数据对象中正常获取
                target_field: ("" if source_field in exclude_fields else item.get(source_field))
                for source_field, target_field in field_mapping
            }
            for item in self.analysis_result
        ]
