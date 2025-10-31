import datetime
import json
import time
import time as time_module
import traceback
from datetime import datetime, time as time_obj, date, timezone, timedelta
from decimal import Decimal
from functools import wraps

import requests
from sqlalchemy.exc import OperationalError

TZ_UTC_8 = timezone(timedelta(hours=8))


def json_serializer(obj):
    """
    自定义JSON序列化器，处理日期、时间和Decimal对象。
    核心逻辑：将所有 date 和 datetime 对象从东八区（UTC+8）转换为标准的UTC时间，
    并格式化为带 'Z' 的ISO 8601字符串，以满足简道云API的要求。
    同时，将 Decimal 对象转换为 float 类型。
    """
    # 如果是 datetime 对象
    if isinstance(obj, datetime):
        # 假设从数据库获取的 naive datetime 是东八区时间，为其附加时区信息
        aware_obj = obj.replace(tzinfo=TZ_UTC_8)
        # 转换为 UTC 时间
        utc_obj = aware_obj.astimezone(timezone.utc)
        # 格式化为带 'Z' 的 ISO 8601 格式 (例如: '2025-09-13T08:09:07Z')
        return utc_obj.isoformat().replace('+00:00', 'Z')

    # 如果是 date 对象
    if isinstance(obj, date):
        # 将 date 视为东八区当天的午夜
        aware_obj = datetime.combine(obj, time_obj.min).replace(tzinfo=TZ_UTC_8)
        # 转换为 UTC 时间
        utc_obj = aware_obj.astimezone(timezone.utc)
        # 格式化为带 'Z' 的 ISO 8601 格式 (例如: '2025-09-12T16:00:00Z')
        return utc_obj.isoformat().replace('+00:00', 'Z')

    # 如果是 time 对象，通常不带时区，保持原样
    if isinstance(obj, time_obj):
        return obj.strftime('%H:%M:%S')

    # 新增：如果是 Decimal 对象，将其转换为 float
    if isinstance(obj, Decimal):
        return float(obj)

    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# --- 失败重试装饰器 ---
def retry(max_retries=3, delay=5, backoff=2, exceptions=(OperationalError, requests.exceptions.RequestException)):
    """
    一个装饰器，用于在函数引发特定异常时进行重试。
    :param max_retries: 最大重试次数。
    :param delay: 初始延迟时间（秒）。
    :param backoff: 每次重试后延迟时间的倍增因子。
    :param exceptions: 一个包含需要重试的异常类型的元组。
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 延迟导入以避免循环依赖
            from app.models import SyncTask

            _max_retries, _delay = max_retries, delay
            last_exception = None  # 跟踪最后一次异常

            for attempt in range(_max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:  # 如果这是最后一次尝试
                        break  # 跳出循环以记录并重新抛出

                    current_delay = _delay * (backoff ** attempt)
                    print(
                        f"函数 {func.__name__} 因 {type(e).__name__} 失败。将在 {current_delay:.2f} 秒后重试 ({max_retries - attempt - 1} 次剩余)...")
                    time_module.sleep(current_delay)

            # 所有重试次数已用完
            print(f"函数 {func.__name__} 在 {max_retries} 次重试后失败。最后错误: {last_exception}")
            # 尝试从 kwargs 或 args 中获取 task_config
            task_config = kwargs.get('task_config')
            if not task_config and args:
                for arg in args:
                    if isinstance(arg, SyncTask):
                        task_config = arg
                        break
            # 调用修复后的 log_sync_error
            log_sync_error(
                task_config=task_config,
                error=last_exception,
                extra_info=f"Function {func.__name__} failed after {max_retries} retries."
            )
            raise last_exception  # 将原始异常抛出

        return wrapper

    return decorator


# --- 错误处理与日志 ---

def send_wecom_notification(wecom_url: str, content: str):
    """
    发送格式化的错误消息到企业微信机器人。
    :param wecom_url: 动态获取的企微机器人 URL
    :param content: 格式化后的 Markdown 内容
    """
    if not wecom_url:
        return
    try:
        data = {"msgtype": "markdown", "markdown": {"content": content}}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(wecom_url, headers=headers, data=json.dumps(data), timeout=10)
        response.raise_for_status()
        print("已成功发送错误日志到企业微信。")
    except Exception as e:
        print(f"发送企业微信通知失败: {e}")


# --- 使用字符串类型提示 'SyncTask' ---
def log_sync_error(task_config: 'SyncTask' = None,
                   error: Exception = None, payload: dict = None, extra_info: str = None):
    """
    将同步错误记录到数据库，并触发企微通知。
    """
    # --- 延迟导入 ---
    from app.models import ConfigSession, SyncErrLog
    # 同样延迟导入 flask.g 用于上下文检查
    from flask import g

    session = None
    session_created = False

    # 从 task_config 提取信息
    task_id = None
    app_id = None
    entry_id = None
    table_name = None
    department_name = None

    if task_config:
        # 延迟导入 SyncTask 以进行类型检查
        from app.models import SyncTask
        if isinstance(task_config, SyncTask):
            task_id = task_config.task_id
            app_id = task_config.jdy_app_id
            entry_id = task_config.jdy_entry_id
            table_name = task_config.source_table
            department_name = task_config.department_name

    try:
        # --- 安全地尝试获取会话 ---
        try:
            # 只有在有应用上下文且 g 对象包含 config_session 时才使用它
            if g and hasattr(g, 'config_session') and g.config_session.is_active:
                session = g.config_session
                # print("log_sync_error: 使用来自 g 对象的现有会话。")
            else:
                raise RuntimeError("No active session in g")  # 跳到 except 块
        except (RuntimeError, AttributeError):
            # 如果发生 RuntimeError (Working outside...) 或 g 不存在/没有 config_session
            # print("log_sync_error: 不在请求上下文中或 g 中无会话，创建新会话。")
            session = ConfigSession()
            session_created = True

        # 1. 准备日志数据
        error_message = f"{extra_info}\n{str(error)}" if extra_info and error else (
            str(error) if error else extra_info or "未知错误")
        traceback_str = traceback.format_exc() if error and isinstance(error, Exception) else None  # 仅在有异常时记录堆栈

        def json_serializer_default(obj):
            if isinstance(obj, (datetime, date, time)):
                return obj.isoformat()
            try:
                # 添加对 bytes 的处理
                if isinstance(obj, bytes):
                    return obj.decode('utf-8', errors='replace')  # 尝试解码，失败则替换
                return str(obj)  # 更通用的回退
            except Exception:
                return f"<Not Serializable: {type(obj).__name__}>"

        payload_str = json.dumps(payload, ensure_ascii=False, indent=2,
                                 default=json_serializer_default) if payload else "{}"

        # 2. 插入数据库
        new_log = SyncErrLog(
            task_id=task_id,
            app_id=app_id,
            entry_id=entry_id,
            table_name=table_name,
            department_name=department_name,
            error_message=error_message,
            traceback=traceback_str,  # 使用格式化后的字符串
            payload=payload_str,
            timestamp=datetime.now(TZ_UTC_8)
        )
        session.add(new_log)
        session.commit()
        print(f"错误日志已成功写入数据库: (Task {task_id or 'N/A'})")

        # 3. 动态发送企微通知
        from app.models import SyncTask
        if isinstance(task_config,
                      SyncTask) and task_config.send_error_log_to_wecom and task_config.wecom_robot_webhook_url:
            payload_snippet = (payload_str[:1000] + '...') if len(payload_str) > 1000 else payload_str
            content = f"""
            **简道云数据同步错误告警**
            > **任务ID**: {task_id or 'N/A'}
            > **部门**: {department_name or 'N/A'}
            > **时间**: {datetime.now(TZ_UTC_8).strftime('%Y-%m-%d %H:%M:%S')}
            > **表单/表名**: {table_name or 'N/A'}
            > **App ID**: {app_id or 'N/A'}
            > **Entry ID**: {entry_id or 'N/A'}

            **错误信息**:
            <font color="warning">{error_message}</font>

            **数据片段**:
            `{payload_snippet}`

            请及时处理！
            """
            send_wecom_notification(task_config.wecom_robot_webhook_url, content)

    except Exception as db_err:
        print(f"!!! 写入错误日志到数据库时发生严重错误: {db_err}")
        print(f"原始错误信息: {error_message}")
        # 如果连日志会话都有问题，尝试回滚并打印更详细的错误
        if session:
            try:
                session.rollback()
            except Exception as rb_err:
                print(f"!!! 回滚日志会话失败: {rb_err}")
        # 打印数据库错误的堆栈信息
        print("--- Database Error Traceback ---")
        traceback.print_exc()
        print("-------------------------------")

    finally:
        # 仅当此函数自己创建了会话时才关闭它
        if session and session_created:
            # print("log_sync_error: 关闭独立创建的会话。")
            session.close()
