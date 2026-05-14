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
import time
import requests
from json import JSONDecodeError
from urllib.parse import urljoin
from omnihelper.util.log import logger
from omnihelper.constants.flink_constants import TaskStatus


class FlinkRequester:
    """
    Requester Layer: Network Communication
    """

    def __init__(self, url, timeout=5, ssl_verify=True, interval=100,
                 max_retries=3, kerberos=False, kerberos_mutual_auth="OPTIONAL",
                 headers=None):
        self.base_url = url
        self.session = requests.Session()
        self.timeout = int(timeout) if timeout else 5
        self.max_retries = max_retries  # 最大尝试次数
        self.ssl_verify = ssl_verify  # 校验SSL证书
        self.interval = interval  # 接口调用间隔
        self.custom_headers = headers or {}
        self.last_error = None  # 最近一次错误信息

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
        """获取作业概览"""
        return self._get_json("jobs/overview")

    def get_job_detail(self, jid):
        """获取作业详情"""
        return self._get_json(f"jobs/{jid}")

    def get_vertex_metrics(self, jid, vid, metric_ids=None):
        """
        :param jid: 作业ID
        :param vid: 任务ID
        :param metric_ids: 算子指标ID列表
        :return: 算子指标列表
        """
        if isinstance(metric_ids, list):
            params = {"get": ",".join(metric_ids)} if metric_ids else {}
        else:
            params = {"get": metric_ids} if metric_ids else {}
        return self._get_json(f"jobs/{jid}/vertices/{vid}/metrics", params=params)
