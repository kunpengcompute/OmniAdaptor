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
Flink REST API 请求模块


模块功能说明:
    本模块封装了与 Flink REST API 的通信功能，提供:
    1. HTTP/HTTPS 请求封装
    2. 自动重试机制
    3. Kerberos 认证支持
    4. 自定义请求头
    5. 请求间隔控制
    6. SSL 证书验证控制

API 端点:
    - jobs/overview: 获取作业概览
    - jobs/{jid}: 获取作业详情
    - jobs/{jid}/vertices/{vid}/metrics: 获取算子指标
"""

import time
import requests
from json import JSONDecodeError
from urllib.parse import urljoin
from omnihelper.util.log import logger
from omnihelper.constants.flink_constants import TaskStatus


class FlinkRequester:
    """
    Flink REST API 请求器

    核心职责:
    1. 管理 HTTP 会话和连接配置
    2. 提供可重试的 API 调用机制
    3. 处理认证和请求头
    4. 统一错误处理和日志记录

    成员变量说明:
    - base_url: Flink REST API 基础 URL
    - session: requests Session 实例
    - timeout: 请求超时时间 (秒)
    - max_retries: 最大重试次数
    - ssl_verify: SSL 证书验证标志
    - interval: API 调用间隔 (毫秒)
    - custom_headers: 自定义请求头
    - last_error: 最近一次错误信息
    """

    def __init__(self, url, timeout=5, ssl_verify=True, interval=100,
                 max_retries=3, kerberos=False, kerberos_mutual_auth="OPTIONAL",
                 headers=None):
        """
        初始化 Flink 请求器

        参数说明:
        :param url: Flink REST API 基础 URL
        :param timeout: 请求超时时间 (秒)，默认 5 秒
        :param ssl_verify: 是否验证 SSL 证书，默认 True
        :param interval: API 调用间隔 (毫秒)，默认 100ms
        :param max_retries: 最大重试次数，默认 3 次
        :param kerberos: 是否启用 Kerberos 认证，默认 False
        :param kerberos_mutual_auth: Kerberos  mutual auth 模式
            可选值: OPTIONAL, REQUIRED, DISABLED
        :param headers: 自定义请求头字典

        初始化流程:
        1. 设置基础 URL 和超时时间
        2. 创建 requests Session
        3. 配置 Kerberos 认证 (可选)
        4. 应用自定义请求头
        """
        self.base_url = url
        self.session = requests.Session()
        self.timeout = int(timeout) if timeout else 5
        self.max_retries = max_retries
        self.ssl_verify = ssl_verify
        self.interval = interval
        self.custom_headers = headers or {}
        self.last_error = None

        if kerberos:
            try:
                from requests_kerberos import HTTPKerberosAuth, OPTIONAL, REQUIRED, DISABLED
                mutual_auth_map = {
                    "OPTIONAL": OPTIONAL,
                    "REQUIRED": REQUIRED,
                    "DISABLED": DISABLED,
                }
                mutual_auth_value = mutual_auth_map.get(
                    kerberos_mutual_auth.upper(), OPTIONAL
                )
                self.session.auth = HTTPKerberosAuth(
                    mutual_authentication=mutual_auth_value
                )
                logger.info("Kerberos authentication enabled (mutual_auth=%s)",
                            kerberos_mutual_auth)
            except ImportError:
                raise ImportError(
                    "requests-kerberos is required for Kerberos authentication. "
                    "Install it with: pip install omnihelper[kerberos]"
                )

        if self.custom_headers:
            self.session.headers.update(self.custom_headers)
            logger.info("Custom headers applied: %s", list(self.custom_headers.keys()))

    def _get_json(self, endpoint, params=None):
        """
        发送 GET 请求并返回 JSON 响应

        参数说明:
        :param endpoint: API 端点路径 (不含 base_url)
        :param params: URL 查询参数字典
        :return: 解析后的 JSON 对象，失败返回 None

        实现机制:
        1. 在请求间添加间隔延迟 (interval / 1000 秒)
        2. 最多重试 max_retries 次
        3. 成功 (200) 返回 JSON
        4. 其他状态码记录警告并返回 None
        5. 异常处理: 超时、网络错误、JSON 解析错误
        6. 每次失败更新 last_error 并记录日志

        错误类型映射:
        - Timeout: TaskStatus.REQUEST_TIMEOUT
        - RequestException: TaskStatus.NETWORK_ERROR
        - JSONDecodeError: "JSON解析失败"
        - 其他异常: TaskStatus.UNKNOWN_ERROR
        """
        url = urljoin(self.base_url, endpoint.lstrip('/'))
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.interval / 1000)
                resp = self.session.get(url, params=params, timeout=self.timeout, verify=self.ssl_verify)
                if resp.status_code == 200:
                    self.last_error = None
                    return resp.json()
                logger.warning(f"[API Error] {endpoint} Status: {resp.status_code}")
                self.last_error = f"HTTP {resp.status_code}"
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying {endpoint} (attempt {attempt + 1})")
            except requests.exceptions.Timeout as e:
                logger.error(f"[Timeout Error] {endpoint} Failed: {e}")
                self.last_error = TaskStatus.REQUEST_TIMEOUT
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying {endpoint} (attempt {attempt + 1})")
                continue
            except requests.exceptions.RequestException as e:
                logger.error(f"[Network Error] {endpoint} Failed: {e}")
                self.last_error = TaskStatus.NETWORK_ERROR
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying {endpoint} (attempt {attempt + 1})")
                continue
            except JSONDecodeError as e:
                logger.error(f"[JSON Decode Error] {endpoint} Failed: {e}")
                self.last_error = f"JSON解析失败: {e}"
                break
            except Exception as e:
                logger.error(f"[UnException Error] {endpoint} Failed: {e}")
                self.last_error = f"{TaskStatus.UNKNOWN_ERROR}: {e}"
                break
        return None

    def get_jobs_overview(self):
        """
        获取作业概览

        返回值:
        :return: 作业概览 JSON 对象
            典型结构:
            {
                "jobs": [
                    {"jid": "xxx", "name": "xxx", "state": "RUNNING", ...},
                    ...
                ]
            }

        API: GET /jobs/overview
        """
        return self._get_json("jobs/overview")

    def get_job_detail(self, jid):
        """
        获取指定作业的详细信息

        参数说明:
        :param jid: 作业 ID (Job ID)
        :return: 作业详情 JSON 对象
            典型结构:
            {
                "jid": "xxx",
                "name": "xxx",
                "state": "RUNNING",
                "plan": {
                    "nodes": [
                        {"id": "xxx", "description": "xxx", ...},
                        ...
                    ]
                },
                "vertices": [...],
                ...
            }

        API: GET /jobs/{jid}
        """
        return self._get_json(f"jobs/{jid}")

    def get_vertex_metrics(self, jid, vid, metric_ids=None):
        """
        获取指定算子的性能指标

        参数说明:
        :param jid: 作业 ID (Job ID)
        :param vid: 算子 ID (Vertex ID)
        :param metric_ids: 指标 ID 列表 (可选)
            常用指标:
            - numRecordsIn: 输入记录数
            - numRecordsOut: 输出记录数
            - numRecordsInPerSecond: 输入记录速率
            - numRecordsOutPerSecond: 输出记录速率
            - numBytesIn: 输入字节数
            - numBytesOut: 输出字节数
        :return: 指标值列表
            典型结构:
            [
                {"id": "xxx.numRecordsIn", "value": "12345"},
                {"id": "xxx.numRecordsOut", "value": "12300"},
                ...
            ]

        API: GET /jobs/{jid}/vertices/{vid}/metrics?get=metric1,metric2,...
        """
        if isinstance(metric_ids, list):
            params = {"get": ",".join(metric_ids)} if metric_ids else {}
        else:
            params = {"get": metric_ids} if metric_ids else {}
        result = self._get_json(f"jobs/{jid}/vertices/{vid}/metrics", params=params)
        # ★ 修复点：处理两种返回格式
        # 不传参数时返回 ["metric.id.1", "metric.id.2", ...]（字符串列表）
        # 传参数时返回 [{"id": "xxx", "value": "yyy"}, ...]（字典列表）
        if result is not None and isinstance(result, list):
            if len(result) > 0 and isinstance(result[0], str):
                # 转换为统一的字典格式
                return [{"id": m_id, "value": None} for m_id in result]
        return result
