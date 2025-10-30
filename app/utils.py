import json
import requests
import time
from decimal import Decimal
import datetime
import functools
from flask import current_app
from app.models import db, SyncErrLog


def json_serializer(obj):
    """
    自定义 JSON 序列化器
    处理 Decimal 和 datetime 对象
    """
    if isinstance(obj, Decimal):
        # 将 Decimal 转换为 float 或 string
        # 注意：转为 float 可能有精度损失，但对于简道云通常是可接受的
        # 如果需要高精度，应考虑转为字符串
        return float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        # 严格转换为 UTC 时间，并使用 'Z' 结尾的 ISO 格式
        # 简道云要求此格式
        if isinstance(obj, datetime.datetime):
            # 假设本地时间是 UTC（如果不是，需要更复杂的时区转换）
            # 或者，如果它是 naive datetime，我们假设它是本地时间并转为 UTC
            # 一个更健壮的方法是确保所有 datetime 都是 aware 的
            # 简单处理：
            dt_utc = obj.astimezone(datetime.timezone.utc)
        else:  # date 对象
            dt_utc = datetime.datetime(obj.year, obj.month, obj.day, tzinfo=datetime.timezone.utc)

        return dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    raise TypeError(f"Type {type(obj)} not serializable")


def retry(retries=3, delay=5, backoff=2):
    """
    重试装饰器
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _retries, _delay = retries, delay
            while _retries > 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    _retries -= 1
                    if _retries == 0:
                        current_app.logger.error(f"Function {func.__name__} failed after {retries} retries: {e}")
                        raise
                    msg = f"Function {func.__name__} failed with {e}, retrying in {_delay}s... ({_retries} retries left)"
                    current_app.logger.warning(msg)
                    time.sleep(_delay)
                    _delay *= backoff

        return wrapper

    return decorator


def send_wecom_notification(title, message):
    """
    发送企业微信通知
    """
    bot_key = current_app.config.get('WECOM_BOT_KEY')
    if not bot_key:
        current_app.logger.warning("WECOM_BOT_KEY not set, skipping notification.")
        return

    url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={bot_key}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"### {title}\n> {message}"
        }
    }
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=5)
        if response.json().get("errcode") != 0:
            current_app.logger.error(f"Failed to send WeCom notification: {response.text}")
    except Exception as e:
        current_app.logger.error(f"Error sending WeCom notification: {e}")


def log_sync_error(task_id, error_message, data=None):
    """
    重构后的错误日志记录函数
    使用 db.session 和 SyncErrLog ORM 模型
    **必须在 Flask App 上下文中调用**
    """
    try:
        data_content = json.dumps(data, default=str) if data else None

        err_log = SyncErrLog(
            task_id=task_id,
            error_message=str(error_message),
            data_content=data_content
        )

        db.session.add(err_log)
        db.session.commit()

        current_app.logger.error(f"[Task {task_id}] Error logged: {error_message} | Data: {data_content}")

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(
            f"CRITICAL: Failed to write to SyncErrLog! Original error: {error_message}. Logging error: {e}")