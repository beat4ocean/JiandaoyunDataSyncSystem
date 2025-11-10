# -*- coding: utf-8 -*-
"""
简道云 API 客户端 (v5)
"""
import logging
import threading

import requests
import json
import time
from datetime import datetime, timedelta

from requests import RequestException, HTTPError

from app import Config
from app.utils import TZ_UTC_8

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ApiClient:
    """
    简道云 API 客户端基类
    内置了身份验证、请求发送、错误处理、速率限制和失败重试的核心逻辑。

    采用二级限流策略：
    1. 全局限流 (Global): 限制 Config.GLOBAL_API_QPS，防止 8303 错误。
    2. 端点限流 (Endpoint): 限制单个接口的 QPS (self.qps)。
    """

    # 端点限流 (原逻辑)，为其增加一个锁
    _rate_limit_records = {}
    _endpoint_lock = threading.Lock()

    # 全局 (部门级别) 速率限制
    _global_last_call_time = None
    _global_lock = threading.Lock()
    _global_min_interval = timedelta(seconds=0)

    try:
        if Config.GLOBAL_API_QPS > 0:
            _global_min_interval = timedelta(seconds=1 / Config.GLOBAL_API_QPS)
            logger.info(f"全局 API 限流已启用：{Config.GLOBAL_API_QPS} QPS")
        else:
            logger.warning("Config.GLOBAL_API_QPS 未设置或为 0。禁用全局速率限制。")
    except (AttributeError, ZeroDivisionError, TypeError):
        logger.warning("Config.GLOBAL_API_QPS 加载失败。禁用全局速率限制。")
        pass

    def __init__(self, api_key, host, qps=10, retry_count=3, retry_delay=5):
        """
        初始化客户端。
        :param api_key: 租户专属的 API Key
        :param host: API Host (e.g., https://api.jiandaoyun.com)
        :param qps: 此类API的 *端点* QPS限制
        :param retry_count: 失败重试次数
        :param retry_delay: 失败重试延迟（秒）
        """
        if not api_key or not host:
            raise ValueError("必须提供 API 密钥和主机。")
        self.api_key = api_key
        self.host = host
        self.qps = qps
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.min_interval = timedelta(seconds=1 / qps) if qps > 0 else timedelta(seconds=0)

    def _throttle(self, endpoint):
        """
        根据端点和全局设置控制 API 调用速率 (二级节流)。
        """
        now = datetime.now(TZ_UTC_8)

        # --- 1. 全局节流 (保护 Code=8303) ---
        if ApiClient._global_min_interval.total_seconds() > 0:
            with ApiClient._global_lock:
                last_call = ApiClient._global_last_call_time
                if last_call:
                    elapsed = now - last_call
                    wait_time = ApiClient._global_min_interval - elapsed
                    if wait_time.total_seconds() > 0:
                        logger.debug(
                            f"GLOBAL Throttling for {wait_time.total_seconds():.3f}s (Limit: {Config.GLOBAL_API_QPS} QPS)")
                        time.sleep(wait_time.total_seconds())
                        # 更新 'now'，因为我们刚刚等待过
                        now = datetime.now(TZ_UTC_8)

                # 记录本次调用时间
                ApiClient._global_last_call_time = now

        # --- 2. 端点节流 (原逻辑) ---
        # (确保 self.qps (端点QPS) 也被遵守)
        if self.min_interval.total_seconds() > 0:
            with ApiClient._endpoint_lock:
                last_call_time = ApiClient._rate_limit_records.get(endpoint)

                if last_call_time:
                    elapsed = now - last_call_time
                    wait_time = self.min_interval - elapsed
                    if wait_time.total_seconds() > 0:
                        logger.debug(
                            f"ENDPOINT Throttling for {endpoint} for {wait_time.total_seconds():.3f}s (Limit: {self.qps} QPS)")
                        time.sleep(wait_time.total_seconds())
                        # 更新 'now'
                        now = datetime.now(TZ_UTC_8)

        ApiClient._rate_limit_records[endpoint] = now  # 更新时间戳

    def _send_request(self, endpoint, data):
        """
        发送 POST 请求的私有核心方法，包含重试和速率限制逻辑。

        [!! 修改 !!]
        - 增加对 8303 错误的特定重试逻辑。
        """
        headers = {
            'Authorization': 'Bearer ' + self.api_key,
            'Content-Type': 'application/json;charset=utf-8'
        }
        url = self.host.rstrip('/') + '/' + endpoint.lstrip('/')
        # 注意：此处不再 dumps，requests 会自动处理 dict
        payload_dict = data  # Keep it as dict for logging

        last_exception = None
        for attempt in range(self.retry_count + 1):
            try:
                self._throttle(endpoint)  # 调用新的二级节流
                logger.debug(
                    f"Sending request to {url} with payload: {json.dumps(payload_dict, ensure_ascii=False)}")  # Debugging line
                res = requests.post(url=url, json=payload_dict, headers=headers,
                                    timeout=30)  # Increase timeout, use json parameter

                # --- 增强错误日志 ---
                if res.status_code >= 400:
                    error_code = None
                    try:
                        error_info = res.json()
                        error_code = error_info.get('code')  # 提取 code
                        logger.error(f"API 错误: Code={error_code}, Msg={error_info.get('msg')}")
                        # 打印请求体以帮助诊断 400 Bad Request
                        logger.error(f"请求失败的 Payload: {json.dumps(payload_dict, ensure_ascii=False, indent=2)}")
                    except json.JSONDecodeError:
                        logger.error(f"API 请求失败，状态码: {res.status_code}, 响应内容非JSON: {res.text}")
                        logger.error(f"请求失败的 Payload: {json.dumps(payload_dict, ensure_ascii=False, indent=2)}")

                    # 如果是团队频率限制 (8303)，提前警告
                    if error_code == 8303:
                        logger.warning(f"触发团队频率限制 (Code=8303)。将应用更长的延迟重试...")

                    res.raise_for_status()  # 引发 HTTPError
                # --- 结束增强 ---

                # Handle potential empty successful response (e.g., delete)
                try:
                    # 尝试解析JSON，即使是空响应也应该返回一个空字典或特定成功结构
                    json_response = res.json()
                    # 如果API没有返回任何内容但状态码是成功的 (e.g., 204 No Content),
                    # 或者返回了非标准的成功响应体，提供一个默认成功结构。
                    if not json_response and res.ok:
                        return {"status": "success", "_raw_status_code": res.status_code}
                    return json_response
                except json.JSONDecodeError:
                    # 如果响应体为空或者不是JSON，但是状态码表示成功
                    if res.ok:
                        logger.error(f"警告：API 请求成功 (状态码 {res.status_code}) 但响应体为空或非JSON。")
                        return {"status": "success", "_raw_status_code": res.status_code, "_raw_response": res.text}
                    else:
                        # 理论上 raise_for_status 应该已经处理了非OK状态码
                        # 但为了健壮性，这里也处理一下
                        logger.error(f"错误：API 请求失败 (状态码 {res.status_code}) 且响应体非JSON: {res.text}")
                        # 重新抛出，让上层知道出错了
                        res.raise_for_status()


            except (RequestException, HTTPError) as e:
                last_exception = e

                # 1. 检查是否是 8303 错误
                is_rate_limit_error = False
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        error_info = e.response.json()
                        if error_info.get('code') == 8303:
                            is_rate_limit_error = True
                    except json.JSONDecodeError:
                        pass  # 不是json

                # 2. 计算延迟
                if is_rate_limit_error:
                    # 对于 8303 错误，使用更长的基础延迟 (e.g., 10秒) + 指数退避
                    current_delay = (self.retry_delay + 5) * (2 ** attempt)
                    logger.warning(f"检测到 Code=8303 错误。应用更长的重试延迟。")
                else:
                    # 默认指数退避
                    current_delay = self.retry_delay * (2 ** attempt)

                logger.error(f"API 请求到 '{endpoint}' 失败 (尝试 {attempt + 1}/{self.retry_count + 1}): {e}")

                if attempt < self.retry_count:
                    logger.warning(f"将在 {current_delay:.2f} 秒后重试...")
                    time.sleep(current_delay)
                else:  # All retries failed
                    logger.error(f"对 '{endpoint}' 的所有重试均失败。最后错误: {e}")
                    # 尝试打印详细错误 (如果可用)
                    if hasattr(e, 'response') and e.response is not None:
                        try:
                            logger.error(f"失败响应详情: {e.response.text}")
                        except Exception:
                            pass  # Ignore if response text cannot be read
                    raise last_exception  # Re-raise the last exception after all retries fail

        # This part should ideally not be reached if retries are configured > 0
        logger.error(f"警告: _send_request 意外退出重试循环 for {endpoint}")
        if last_exception:
            raise last_exception
        else:
            # Should not happen, but raise a generic error if it does
            raise Exception(f"Unknown error in _send_request for {endpoint} after retries")


# ==============================================================================
# 表单接口 (Form APIs)
# ==============================================================================
class FormApi(ApiClient):
    """
    封装了与简道云「表单结构」相关的接口。
    """

    def __init__(self, api_key, host, **kwargs):
        # 表单字段查询接口V5, QPS限制为30
        qps = kwargs.get("qps", 30)
        retry_count = kwargs.get("retry_count", 3)
        retry_delay = kwargs.get("retry_delay", 5)
        super().__init__(api_key, host, qps=qps, retry_count=retry_count, retry_delay=retry_delay)

    def get_form_widgets(self, app_id, entry_id):
        """
        表单字段查询接口 (V5)
        获取指定表单的所有字段信息。
        文档: https://api.jiandaoyun.com/api/v5/app/entry/widget/list
        """
        endpoint = "api/v5/app/entry/widget/list"
        data = {"app_id": app_id, "entry_id": entry_id}
        return self._send_request(endpoint, data)


# ==============================================================================
# 数据接口 (Data APIs)
# ==============================================================================
class DataApi(ApiClient):
    """
    封装了与简道云「表单数据」相关的增删改查接口。
    """

    # 定义批处理上限
    BATCH_LIMIT = 100

    def __init__(self, api_key, host, qps, **kwargs):
        """
        :param qps: 必须为特定操作指定QPS, e.g., 30 for list, 10 for batch_create.
        """
        retry_count = kwargs.get("retry_count", 3)
        retry_delay = kwargs.get("retry_delay", 5)  # 基础延迟
        super().__init__(api_key, host, qps=qps, retry_count=retry_count, retry_delay=retry_delay)

    def get_single_data(self, app_id, entry_id, data_id):
        """查询单条数据 (V5, QPS: 30)"""
        if self.qps > 30: logger.warning("警告: QPS可能设置错误，查询单条数据应为 30")
        endpoint = "api/v5/app/entry/data/get"
        data = {"app_id": app_id, "entry_id": entry_id, "data_id": data_id}
        return self._send_request(endpoint, data)

    def query_list_data(self, app_id, entry_id, limit=100, data_id=None, fields=None, filter=None):
        """查询多条数据 (V5, QPS: 30)"""
        if self.qps > 30: logger.warning("警告: QPS可能设置错误，查询多条数据应为 30")
        endpoint = "api/v5/app/entry/data/list"
        data = {
            "app_id": app_id,
            "entry_id": entry_id,
            "limit": limit
        }
        if data_id:
            data['data_id'] = data_id
        if fields:
            # Ensure fields is a list
            if isinstance(fields, str):
                fields = [fields]
            data['fields'] = fields
        if filter:
            # Filter should be a dictionary
            if not isinstance(filter, dict):
                logger.warning("警告: query_list_data 中的 filter 参数必须是字典。")
            else:
                data['filter'] = filter
        return self._send_request(endpoint, data)

    def create_single_data(self, app_id, entry_id, data_payload, **kwargs):
        """新建单条数据 (V5, QPS: 20)"""
        if self.qps > 20: logger.warning("警告: QPS可能设置错误，新建单条数据应为 20")
        endpoint = "api/v5/app/entry/data/create"
        data = {"app_id": app_id, "entry_id": entry_id, "data": data_payload, **kwargs}
        return self._send_request(endpoint, data)

    def create_batch_data(self, app_id, entry_id, data_list, **kwargs):
        """
        新建多条数据 (V5, QPS: 10)
        自动按100条/批次分割。
        """
        if self.qps > 10: logger.warning("警告: QPS可能设置错误，新建多条数据应为 10")
        endpoint = "api/v5/app/entry/data/batch_create"

        results = []
        # 按 BATCH_LIMIT (100) 切分 data_list
        for i in range(0, len(data_list), self.BATCH_LIMIT):
            chunk = data_list[i:i + self.BATCH_LIMIT]
            data = {"app_id": app_id, "entry_id": entry_id, "data_list": chunk, **kwargs}

            logger.info(f"[CreateBatch] 正在发送批次 {i // self.BATCH_LIMIT + 1}，包含 {len(chunk)} 条数据...")
            result = self._send_request(endpoint, data)
            results.append(result)

        return results

    def update_single_data(self, app_id, entry_id, data_id, data_payload, **kwargs):
        """修改单条数据 (V5, QPS: 20)"""
        if self.qps > 20: logger.warning("警告: QPS可能设置错误，修改单条数据应为 20")
        endpoint = "api/v5/app/entry/data/update"
        data = {"app_id": app_id, "entry_id": entry_id, "data_id": data_id, "data": data_payload, **kwargs}
        return self._send_request(endpoint, data)

    def update_batch_data(self, app_id, entry_id, data_ids, data_payload, **kwargs):
        """
        修改多条数据 (V5, QPS: 10)
        自动按100条/批次分割。
        """
        if self.qps > 10: logger.warning("警告: QPS可能设置错误，修改多条数据应为 10")
        endpoint = "api/v5/app/entry/data/batch_update"

        # 确保 data_ids 是列表
        if isinstance(data_ids, str):
            data_ids = [data_ids]

        results = []
        # 按 BATCH_LIMIT (100) 切分 data_ids
        for i in range(0, len(data_ids), self.BATCH_LIMIT):
            chunk = data_ids[i:i + self.BATCH_LIMIT]
            data = {"app_id": app_id, "entry_id": entry_id, "data_ids": chunk, "data": data_payload, **kwargs}

            logger.info(f"[UpdateBatch] 正在更新批次 {i // self.BATCH_LIMIT + 1}，包含 {len(chunk)} 条数据...")
            result = self._send_request(endpoint, data)
            results.append(result)

        return results

    def delete_single_data(self, app_id, entry_id, data_id, **kwargs):
        """删除单条数据 (V5, QPS: 20)"""
        if self.qps > 20: logger.warning("警告: QPS可能设置错误，删除单条数据应为 20")
        endpoint = "api/v5/app/entry/data/delete"
        data = {"app_id": app_id, "entry_id": entry_id, "data_id": data_id, **kwargs}
        return self._send_request(endpoint, data)

    def delete_batch_data(self, app_id, entry_id, data_ids):
        """
        删除多条数据 (V5, QPS: 10)
        自动按100条/批次分割。
        """
        if self.qps > 10: logger.warning("警告: QPS可能设置错误，删除多条数据应为 10")
        endpoint = "api/v5/app/entry/data/batch_delete"

        # 确保 data_ids 是列表
        if isinstance(data_ids, str):
            data_ids = [data_ids]

        results = []
        # 按 BATCH_LIMIT (100) 切分 data_ids
        for i in range(0, len(data_ids), self.BATCH_LIMIT):
            chunk = data_ids[i:i + self.BATCH_LIMIT]
            data = {"app_id": app_id, "entry_id": entry_id, "data_ids": chunk}

            logger.info(f"[DeleteBatch] 正在删除批次 {i // self.BATCH_LIMIT + 1}，包含 {len(chunk)} 条数据...")
            result = self._send_request(endpoint, data)
            results.append(result)

        return results
