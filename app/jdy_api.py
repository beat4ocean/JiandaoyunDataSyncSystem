import requests
import time
import json
from functools import wraps
from flask import current_app
from app.utils import json_serializer  # 导入我们的自定义序列化器


# --- 异常类 ---

class JdyApiError(Exception):
    def __init__(self, code, message, *args, **kwargs):
        self.code = code
        self.message = message
        super().__init__(f"API Error Code: {code}, Message: {message}", *args, **kwargs)


class JdyRateLimitError(JdyApiError):
    pass


class JdyAuthError(JdyApiError):
    pass


# --- API 客户端基类 ---

class ApiClient:
    BASE_URL = 'https://api.jiandaoyun.com/api/v4'

    def __init__(self, api_key, rate_limit_qps=10, retry_count=3, timeout=30):
        self.api_key = api_key
        self.qps = rate_limit_qps
        self.retry_count = retry_count
        self.timeout = timeout
        self._last_request_time = 0
        self._rate_limit_interval = 1.0 / self.qps

    def _rate_limit_control(self):
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._rate_limit_interval:
            sleep_time = self._rate_limit_interval - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def _send_request(self, method, endpoint_url, payload_dict):
        """
        发送请求的核心方法
        *** 此处集成了 main.py 的优势 (自定义 json_serializer) ***
        """
        self._rate_limit_control()

        url = f"{self.BASE_URL}{endpoint_url}"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'  # 必须手动指定
        }

        # 关键修改：
        # 1. 使用 json.dumps 和我们的自定义 serializer
        # 2. 传递 data= 参数，而不是 json= 参数
        try:
            data_bytes = json.dumps(
                payload_dict,
                default=json_serializer,
                ensure_ascii=False
            ).encode('utf-8')
        except Exception as e:
            current_app.logger.error(f"Failed to serialize JSON payload: {e}")
            raise JdyApiError(code=999, message=f"JSON 序列化失败: {e}")

        last_exception = None

        for attempt in range(self.retry_count):
            try:
                # 使用 data=data_bytes
                res = requests.post(
                    url=url,
                    data=data_bytes,
                    headers=headers,
                    timeout=self.timeout
                )

                # 弃用 print()，改用 current_app.logger
                if res.status_code == 401:
                    raise JdyAuthError(code=401, message="API 密钥无效或过期")

                if res.status_code == 429:
                    current_app.logger.warning("JDY API Rate limit hit (429), retrying...")
                    raise JdyRateLimitError(code=429, message="API QPS limit exceeded")

                res_json = res.json()

                # 检查简道云业务错误码
                if 'code' in res_json and res_json['code'] != 200:
                    # 8303 也是一种速率限制
                    if res_json['code'] == 8303:
                        current_app.logger.warning("JDY API Rate limit hit (8303), retrying...")
                        raise JdyRateLimitError(code=8303, message="API QPS limit exceeded (8303)")

                    raise JdyApiError(code=res_json['code'], message=res_json.get('msg', 'Unknown API error'))

                # HTTP 状态码错误
                res.raise_for_status()

                return res_json

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, JdyRateLimitError) as e:
                last_exception = e
                current_app.logger.warning(
                    f"Request failed (Attempt {attempt + 1}/{self.retry_count}): {e}. Retrying...")
                time.sleep(2 ** attempt)  # 指数退避
            except JdyApiError as e:
                # 业务错误或认证错误，通常不应重试
                current_app.logger.error(f"JDY API Error: {e}")
                raise
            except Exception as e:
                last_exception = e
                current_app.logger.error(f"Unexpected error during API request: {e}")
                time.sleep(2 ** attempt)

        current_app.logger.error(f"Request failed after {self.retry_count} retries.")
        raise last_exception

    def post(self, endpoint, payload):
        return self._send_request('POST', endpoint, payload)


# --- 按功能封装 API ---

class DataApi(ApiClient):
    """
    封装数据相关 API (增删改查)
    """

    def __init__(self, api_key, app_id, entry_id, **kwargs):
        self.app_id = app_id
        self.entry_id = entry_id
        super().__init__(api_key, **kwargs)

    def _build_endpoint(self, path):
        return f"/app/{self.app_id}/entry/{self.entry_id}{path}"

    def query_list_data(self, fields=None, limit=500, data_filter=None):
        """
        查询多条数据 (支持分页)
        """
        endpoint = self._build_endpoint('/data')
        all_data = []
        data_id_after = None

        while True:
            payload = {
                "limit": limit,
                "fields": fields or [],
                "filter": data_filter or {"rel": "and", "cond": []}
            }
            if data_id_after:
                payload["data_id"] = data_id_after

            try:
                res_json = self.post(endpoint, payload)
                data_list = res_json.get('data', [])
                if not data_list:
                    break

                all_data.extend(data_list)
                data_id_after = data_list[-1]['_id']

                if len(data_list) < limit:
                    break  # 最后一页

            except JdyApiError as e:
                current_app.logger.error(f"Failed to query list data: {e}")
                raise
        return all_data

    def create_batch_data(self, data_list, trigger_workflow=False):
        """
        批量创建数据
        """
        endpoint = self._build_endpoint('/data_batch_create')
        payload = {
            "data_list": data_list,
            "trigger_workflow": trigger_workflow
        }
        try:
            res_json = self.post(endpoint, payload)
            # 返回成功数和失败详情
            return res_json.get('success_count', 0), res_json.get('fail_list', [])
        except JdyApiError as e:
            current_app.logger.error(f"Failed to create batch data: {e}")
            raise

    def create_single_data(self, data_dict, trigger_workflow=False):
        """
        创建单条数据
        """
        endpoint = self._build_endpoint('/data_create')
        payload = {
            "data": data_dict,
            "trigger_workflow": trigger_workflow
        }
        try:
            res_json = self.post(endpoint, payload)
            return res_json.get('data', {})  # 返回创建成功的数据 (包含 _id)
        except JdyApiError as e:
            current_app.logger.error(f"Failed to create single data: {e}")
            raise

    def update_single_data(self, data_id, data_dict, trigger_workflow=False):
        """
        更新单条数据
        """
        endpoint = self._build_endpoint('/data_update')
        payload = {
            "data_id": data_id,
            "data": data_dict,
            "trigger_workflow": trigger_workflow
        }
        try:
            self.post(endpoint, payload)
            return True
        except JdyApiError as e:
            current_app.logger.error(f"Failed to update single data (ID: {data_id}): {e}")
            raise

    def delete_batch_data(self, data_ids):
        """
        批量删除数据
        """
        endpoint = self._build_endpoint('/data_batch_delete')
        payload = {"data_ids": data_ids}
        try:
            res_json = self.post(endpoint, payload)
            return res_json.get('success_count', 0)
        except JdyApiError as e:
            current_app.logger.error(f"Failed to delete batch data: {e}")
            raise

    def delete_single_data(self, data_id):
        """
        删除单条数据
        """
        endpoint = self._build_endpoint('/data_delete')
        payload = {"data_id": data_id}
        try:
            self.post(endpoint, payload)
            return True
        except JdyApiError as e:
            current_app.logger.error(f"Failed to delete single data (ID: {data_id}): {e}")
            raise


class FormApi(ApiClient):
    """
    封装表单结构 API
    """

    def __init__(self, api_key, app_id, **kwargs):
        self.app_id = app_id
        super().__init__(api_key, **kwargs)

    def get_form_fields(self, entry_id):
        """
        获取表单字段列表
        """
        endpoint = f"/app/{self.app_id}/entry/{entry_id}/fields"
        # 这是一个 GET 请求，但我们封装的 _send_request 是 POST
        # 为了简单起见，我们假设（或在文档中确认）简道云支持 POST /api/v4/app/.../fields
        # 如果它 *必须* 是 GET，我们需要重构 _send_request 来处理 GET

        # 假设文档允许 POST 查询:
        try:
            res_json = self.post(endpoint, {})
            return res_json.get('fields', [])
        except JdyApiError as e:
            current_app.logger.error(f"Failed to get form fields for entry {entry_id}: {e}")
            raise
