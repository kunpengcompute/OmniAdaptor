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
Flink 日志解析主模块


模块功能说明:
    本模块是 Flink 作业分析工具的主入口，负责:
    1. 解析命令行参数
    2. 初始化 API 请求器
    3. 获取和解析 Flink 作业数据
    4. 调用解析器分析作业
    5. 生成 Excel 格式的分析报告

工作流程:
    1. 解析命令行参数并校验
    2. 加载表结构配置 (可选)
    3. 创建 FlinkRequester 连接 Flink API
    4. 获取作业列表或指定作业
    5. 遍历每个作业获取详情和指标
    6. 调用 FlinkParser 解析作业数据
    7. 将结果导出为 Excel 报告

命令行参数:
    - --url: Flink Dashboard URL
    - --jobid: 指定作业 ID (可选)
    - --output-dir: 输出目录
    - --interval: API 调用间隔 (毫秒)
    - --timeout: 请求超时时间 (秒)
    - --input-data: 表结构 CSV 路径
    - --show-op-details: 是否显示算子详情
    - --kerberos: 启用 Kerberos 认证
    - --header: 自定义请求头
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
    """
    Flink 日志解析器

    核心职责:
    1. 参数校验和初始化
    2. 表结构加载
    3. API 数据获取协调
    4. 作业数据解析流程控制
    5. Excel 报告生成

    成员变量说明:
    - args: 命令行参数对象
    - requester: FlinkRequester 实例
    - excel_writer: Excel 写入器
    - analysis_result: 分析结果列表
    - parser: FlinkParser 解析器
    - target_metrics: 目标指标列表
    - table_schema: 表结构字典
    - column_type: 字段类型映射
    - table_column_type: 表字段类型映射
    """

    EXECUTE_PATH = CommonUtil.get_execute_path()
    TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, args):
        """
        初始化日志解析器

        参数说明:
        :param args: 命令行参数命名空间

        初始化流程:
        1. 存储命令行参数
        2. 创建 Excel 写入器
        3. 创建解析器实例
        4. 加载表结构配置
        5. 校验参数有效性
        """
        self.args = args
        self.requester = None
        self.excel_writer = FlinkExcelWriterWithStyle()
        self.analysis_result = []
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
        """
        获取指定顶点的描述信息

        参数说明:
        :param vid: 顶点 ID
        :param plan_nodes: 计划节点字典 {vid: node}
        :return: HTML 反转义后的描述文本
        """
        desc = plan_nodes.get(vid, {}).get('description', '')
        return html.unescape(desc) if desc else ''

    @staticmethod
    def _parse_description_data(description):
        """
        解析描述文本为结构化数据

        参数说明:
        :param description: 原始描述文本
        :return: 解析后的数据列表

        解析规则:
        1. 按 <br/> 或换行符分割
        2. 清理分隔符
        3. 尝试 JSON 解析
        """
        if not description:
            return []
        raw_parts = re.split(r"<br/>[\s:*\+\-]*|\n", description)
        return [
            parsed_line for line in raw_parts
            if (parsed_line := FlinkParser.parse_single_description_line(line)) is not None
        ]

    @staticmethod
    def _create_vertex_result(vid, jid, vertex, status, description, stats, description_data=None, upstream_ids=None):
        """
        创建顶点解析结果对象

        参数说明:
        :param vid: 顶点 ID
        :param jid: 作业 ID
        :param vertex: 顶点信息
        :param status: 解析状态
        :param description: 完整描述
        :param stats: 统计信息
        :param description_data: 解析后的描述数据
        :param upstream_ids: 上游顶点 ID 列表
        :return: 结果字典
        """
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
        """
        加载表结构配置

        加载顺序:
        1. 检查 --input-data 参数指定的路径
        2. 如果未指定，检查默认路径 resources/flink_table_schema.csv
        3. 如果都不存在，类型解析能力受限

        """
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
        """
        获取并校验命令行参数

        返回值: True 表示参数有效，False 表示无效

        校验步骤:
        1. 设置输出目录默认值
        2. 校验 URL 参数
        3. 校验数值参数
        4. 校验其他参数
        5. 处理 SSL 验证参数
        6. 解析自定义请求头
        """
        if self.args.output_dir is None:
            self.args.output_dir = os.path.join(self.EXECUTE_PATH, "output")

        os.makedirs(self.args.output_dir, exist_ok=True)

        if not self._validate_url():
            return False

        if not self._validate_numeric_args():
            return False

        if not self._validate_other_args():
            return False

        self.args.ssl_verify = not getattr(self.args, 'no_ssl_verify', False)

        self.args.parsed_headers = self._parse_headers()
        if self.args.parsed_headers is None:
            return False

        return True

    def _validate_url(self):
        """
        校验 URL 参数

        校验规则:
        1. 必须包含协议 (http/https)
        2. 必须包含主机名
        3. 端口必须在 1-65535 范围内
        4. 提取 host, port, use_https 信息
        """
        url = self.args.url
        try:
            parsed_url = urllib.parse.urlparse(url)

            if not parsed_url.scheme:
                print("Error: Invalid URL: missing scheme. Please include http:// or https://.")
                return False
            elif parsed_url.scheme not in ['http', 'https']:
                print(f"Error: Invalid URL scheme: {parsed_url.scheme}. Only http and https are supported.")
                return False

            if not parsed_url.netloc:
                print("Error: Invalid URL: missing host and port.")
                return False

            self.args.host = parsed_url.hostname
            self.args.port = parsed_url.port
            if self.args.port is None:
                self.args.port = 80 if parsed_url.scheme == 'http' else 443
            self.args.use_https = parsed_url.scheme == 'https'

            if self.args.port < 1 or self.args.port > 65535:
                print(f"Error: Invalid port: {self.args.port}. Port must be between 1 and 65535.")
                return False
            return True
        except Exception as e:
            print(f"Error: Invalid URL format: {e}. Please check your URL and try again.")
            return False

    def _validate_numeric_args(self):
        """
        校验数值参数

        校验项:
        - interval: 0-30000 毫秒
        - timeout: 1-300 秒
        """
        if not isinstance(self.args.interval, int) or self.args.interval < 0 or self.args.interval > 30000:
            print(
                f"Error: Invalid interval: {self.args.interval}. Interval must be an integer between 0 and 30000 (ms).")
            return False

        if not isinstance(self.args.timeout, int) or self.args.timeout < 1 or self.args.timeout > 300:
            print(f"Error: Invalid timeout: {self.args.timeout}. Timeout must be an integer between 1 and 300 (s).")
            return False
        return True

    def _validate_other_args(self):
        """
        校验其他参数

        校验项:
        - jobid: 非空字符串列表
        - output_dir: 可创建的有效路径
        """
        if self.args.jobid:
            for jobid in self.args.jobid:
                if not jobid or not isinstance(jobid, str):
                    print("Error: Invalid jobid: jobid must be a non-empty string.")
                    return False

        if self.args.output_dir:
            if not isinstance(self.args.output_dir, str):
                print("Error: Invalid output_dir: must be a string.")
                return False
            try:
                os.makedirs(self.args.output_dir, exist_ok=True)
            except Exception as e:
                print(f"Error: Invalid output_dir: {e}.")
                return False
        return True

    def _parse_headers(self):
        """
        解析自定义请求头

        格式要求: "Key: Value"
        返回: 解析后的请求头字典
        """
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
        """
        打印配置信息摘要
        """
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
        """
        分批获取指标数据

        参数说明:
        :param jid: 作业 ID
        :param vid: 顶点 ID
        :param metrics: 指标 ID 列表
        :param batch_size: 每批数量，默认 10
        :return: 所有指标值列表
        """
        results = []
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i + batch_size]
            metric_values = self.requester.get_vertex_metrics(jid, vid, batch) if batch else []
            if metric_values:
                results.extend(metric_values)
        return results

    def _get_job_ids(self, job_ids):
        """
        获取要处理的作业 ID 列表

        参数说明:
        :param job_ids: 用户指定的作业 ID 列表 (None 表示获取全部)
        :return: 要处理的作业 ID 列表
        """
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
        """
        处理单个作业

        参数说明:
        :param jid: 作业 ID
        :return: 作业处理结果或 None
        """
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
                "status": TaskStatus.VERTEX_METRICS_FAILED,
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
        """
        处理单个顶点

        参数说明:
        :param vid: 顶点 ID
        :param vertex: 顶点信息
        :param plan_nodes: 计划节点字典
        :param jid: 作业 ID
        :param detail: 作业详情
        :return: 顶点处理结果
        """
        metrics = self._get_vertex_metrics(vid, jid)
        # ★ 修改点：获取 description 和算子信息（即使指标获取失败也需要）
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
        # ★ 修改点：检查是否返回了错误状态
        if metrics.get("error_status"):
            error_status = metrics["error_status"]
            logger.warning(f"Vertex {vid} in job {jid} failed with status: {error_status}")
            # 构建空的 operators 但保留算子名称信息
            empty_stats = {"operators": {}, "summary": {}, "analysis": {}}
            return self._create_vertex_result(vid, jid, vertex, error_status, description,
                                              empty_stats, description_data, upstream_ids)
        if not metrics.get("values"):
            empty_stats = {"operators": {}, "summary": {}, "analysis": {}}
            return self._create_vertex_result(vid, jid, vertex,
                                              TaskStatus.VERTEX_METRICS_EMPTY,
                                              description, empty_stats, description_data, upstream_ids)
        try:
            stats = self._parse_performance_stats(vid, metrics["values"], detail, jid)
            status = TaskStatus.SUCCESS
        except Exception as e:
            logger.error(f"Failed to parse performance stats for vertex {vid}: {e}")
            stats = {"operators": {}, "summary": {}, "analysis": {}}
            status = TaskStatus.OPERATOR_PARSE_FAILED
        return self._create_vertex_result(vid, jid, vertex, status, description, stats, description_data, upstream_ids)

    def _parse_performance_stats(self, vid, metrics_values, detail, jid):
        """
        解析性能统计数据
        """
        return self.parser.parse_performance_stats(vid, metrics_values,
                                                   self.parser.get_description(detail, jid))

    def _get_vertex_metrics(self, vid, jid):
        """
        获取顶点指标

        参数说明:
        :param vid: 顶点 ID
        :param jid: 作业 ID
        :return: {"ids": [...], "values": [...]}

        优化逻辑:
        当 show_op_details=False 时，过滤掉 runtime, numBytesIn, numBytesOut
        相关的指标，减少 API 请求量
        """
        available = self.requester.get_vertex_metrics(jid, vid)
        if not available:
            logger.warning(f"No metrics available for vertex {vid} in job {jid}, error: {self.requester.last_error}")
            # ★ 修改点：返回具体错误状态，让调用方知道失败原因
            error_status = self.requester.last_error or TaskStatus.VERTEX_METRICS_FAILED
            return {"error_status": error_status, "ids": [], "values": []}
        needed_ids = self.parser.filter_num_data(available, self.target_metrics)
        if not getattr(self.args, 'show_op_details', True) and needed_ids:
            exclude_keywords = ["runtime", "BytesIn", "BytesOut"]
            needed_ids = [
                m_id for m_id in needed_ids
                if not any(kw in m_id for kw in exclude_keywords)
            ]
            logger.debug(
                f"Optimization: show-op-details is disabled. Filtered API metrics batch to {len(needed_ids)} items.")
        if not needed_ids:
            logger.warning(f"No needed metrics found for vertex {vid} in job {jid}")
            values = available if isinstance(available, list) else []
        else:
            values = self.fetch_metrics(jid, vid, needed_ids)
        return {
            "ids": needed_ids,
            # ★ 修改点：needed_ids 为空时使用 available，避免正常数据被误判为异常
            "values": values
        }

    def analyze_flink_logs(self):
        """
        执行 Flink 日志分析

        执行流程:
        1. 参数校验
        2. 创建 API 请求器
        3. 获取作业列表
        4. 遍历处理每个作业
        5. 调用解析器生成报告数据
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
        生成 Excel 分析报告

        输出格式:
        1. 定义列顺序 (包含重复列名 INPUT, FREQUENCY)
        2. 创建临时列名处理重复
        3. 映射字段到临时列
        4. 处理数据 (根据 show_op_details 过滤)
        5. 转换为 DataFrame
        6. 重命名列还原重复列名
        7. 写入 Excel 文件
        """
        if not self.analysis_result:
            print("Result is empty, No data to display.")
            return

        output_columns = [
            ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
            ExcelColumns.OPERATOR_NAME, ExcelColumns.INPUT, ExcelColumns.OUTPUT,
            ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME, ExcelColumns.INPUT_DATA_SIZE,
            ExcelColumns.OUTPUT_DATA_SIZE, ExcelColumns.FUNC_NAME, ExcelColumns.INPUT,
            ExcelColumns.NESTED_CONTENT, ExcelColumns.FREQUENCY
        ]

        temp_columns = [
            ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
            ExcelColumns.OPERATOR_NAME, ExcelColumns.INPUT, ExcelColumns.OUTPUT,
            ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME, ExcelColumns.INPUT_DATA_SIZE,
            ExcelColumns.OUTPUT_DATA_SIZE, ExcelColumns.FUNC_NAME,
            f"{ExcelColumns.INPUT}_2", ExcelColumns.NESTED_CONTENT,
            f"{ExcelColumns.FREQUENCY}_2"
        ]

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

        processed_data = self._process_report_data(field_mapping)

        df = pd.DataFrame(processed_data, columns=temp_columns)

        df.columns = output_columns

        output_excel_path = os.path.join(self.args.output_dir, f"Omni_Analysis_All_Report_{self.TIMESTAMP}.xlsx")
        self.excel_writer.write_to_excel(df, output_excel_path)

    def _process_report_data(self, field_mapping):
        """
        处理报告数据，应用字段过滤

        参数说明:
        :param field_mapping: 字段映射列表
        :return: 处理后的数据列表

        过滤规则:
        - OUTPUT 列始终不输出
        - show_op_details=False 时额外过滤 RUNTIME, INPUT_DATA_SIZE, OUTPUT_DATA_SIZE
        """
        show_op_details = getattr(self.args, 'show_op_details', True)
        exclude_fields = {ExcelColumns.OUTPUT}
        if not show_op_details:
            exclude_fields.update({
                ExcelColumns.RUNTIME,
                ExcelColumns.INPUT_DATA_SIZE,
                ExcelColumns.OUTPUT_DATA_SIZE,
            })

        return [
            {
                target_field: ("" if source_field in exclude_fields else item.get(source_field))
                for source_field, target_field in field_mapping
            }
            for item in self.analysis_result
        ]
