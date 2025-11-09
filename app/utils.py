import datetime
import hashlib
import json
import logging
import re
import time
import time as time_module
import traceback
from datetime import date, time, timedelta, timezone
from datetime import datetime
from datetime import time as time_obj
from decimal import Decimal
from functools import wraps
from typing import Dict, Any
from urllib.parse import quote_plus

import requests
from pypinyin import pinyin, Style
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TZ_UTC_8 = timezone(timedelta(hours=8))


def json_serializer(obj):
    """
    自定义JSON序列化器，处理日期、时间和Decimal对象。
    核心逻辑：将所有 date 和 datetime 对象（或字符串）从东八区（UTC+8）转换为标准的UTC时间，
    并格式化为带 'Z' 的ISO 8601字符串，以满足简道云API的要求。
    同时，将 Decimal 对象转换为 float 类型。
    """
    # MySQL 时间 "2025-07-08 00:21:30" (字符串) 被传入。
    # 步骤 1 将其解析为 datetime_obj = datetime(..., 0, 21, 30)。
    # 步骤 2 将其视为 UTC+8，并转换为 UTC 时间 2025-07-07T16:21:30Z。
    # db2jdy_services.py 收到这个 UTC 字符串。
    # 简道云 API 收到 2025-07-07T16:21:30Z。
    # 简道云 UI (UTC+8) 显示：16:21:30Z + 8小时 = 2025-07-08 00:21:30。

    datetime_obj = None

    # --- 1. 检查是否为字符串，并尝试解析 (UTC+8) ---
    if isinstance(obj, str):
        obj_str = obj.strip()

        # 综合正则表达式，匹配多种日期时间格式
        # "2024-01-15 10:30:45"      # 空格分隔
        # "2024-01-15T10:30:45"      # T分隔
        # "2024-01-15 10:30:45.123"
        # "2024-01-15T10:30:45.123456"
        # "2024-01-15 10:30:45.1"
        # "2024-01-15 10:30:45Z"        # UTC时间
        # "2024-01-15T10:30:45z"        # UTC时间（小写z）
        # "2024-01-15 10:30:45+08:00"   # 东八区
        # "2024-01-15T10:30:45-05:00"   # 西五区
        # "2024-01-15 10:30:45+0800"    # 无冒号的时区
        # "2024-01-15T10:30:45.123456Z"
        # "2024-01-15 10:30:45.123+08:00"
        # "2024-01-15T10:30:45.1-05:00"
        pattern = r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:?\d{2})?$'  # 标准格式

        # r'^\d{4}-\d{2}-\d{2}$',  # 仅日期
        # r'^\d{2}:\d{2}:\d{2}$',  # 仅时间
        if len(obj_str) >= 19 and re.match(pattern, obj_str):
            try:
                # 标准化格式
                iso_str = obj_str.replace(' ', 'T').replace('Z', '+00:00')
                datetime_obj = datetime.fromisoformat(iso_str)
            except (ValueError, TypeError):
                # 不是有效的日期时间字符串，保持 obj 为原始字符串
                pass

    # --- 2. 处理 datetime 对象 (无论是原始的还是刚从字符串解析的) ---
    if isinstance(obj, datetime) or datetime_obj:
        dt_to_process = obj if isinstance(obj, datetime) else datetime_obj

        aware_obj = dt_to_process

        # 2a. 检查是否是 Naive (无时区信息)
        if dt_to_process.tzinfo is None or dt_to_process.tzinfo.utcoffset(dt_to_process) is None:
            # 假设 naive datetime 是东八区时间 (本地时间)，为其附加时区信息
            aware_obj = dt_to_process.replace(tzinfo=TZ_UTC_8)

        # 2b. 转换为 UTC 时间 (e.g., UTC+8 -> UTC)
        utc_obj = aware_obj.astimezone(timezone.utc)

        # 2c. 格式化为带 'Z' 的 ISO 8601 格式
        return utc_obj.isoformat().replace('+00:00', 'Z')

    # --- 3. 处理 date 对象 ---
    if isinstance(obj, date):
        # 将 date 视为东八区当天的午夜
        aware_obj = datetime.combine(obj, time_obj.min).replace(tzinfo=TZ_UTC_8)
        # 转换为 UTC 时间
        utc_obj = aware_obj.astimezone(timezone.utc)
        # 格式化为带 'Z' 的 ISO 8601 格式
        return utc_obj.isoformat().replace('+00:00', 'Z')

    # --- 4. 处理 time 对象 ---
    if isinstance(obj, time_obj):
        return obj.strftime('%H:%M:%S')

    # --- 5. 处理 Decimal 对象 ---
    if isinstance(obj, Decimal):
        return float(obj)

    # --- 6. 如果是普通字符串 (未被解析为日期时间)，则原样返回 ---
    if isinstance(obj, str):
        return obj

    # --- 7. 如果是其他不支持的类型 ---
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
                    logger.warning(
                        f"Function {func.__name__} failed due to {type(e).__name__}. Retrying in {current_delay:.2f} seconds ({max_retries - attempt - 1} attempts remaining)...")
                    time_module.sleep(current_delay)

            # 所有重试次数已用完
            logger.error(f"Function {func.__name__} failed after {max_retries} retries. Last error: {last_exception}")
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
        logger.info("Successfully sent error log to WeCom.")
    except Exception as e:
        logger.error(f"Failed to send WeCom notification: {e}")


# --- 使用字符串类型提示 SyncTask ---
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
    department_id = None
    department_name = None
    sync_type = None  # 移除错误的默认值

    if task_config:
        # 延迟导入 SyncTask 以进行类型检查
        from app.models import SyncTask
        if isinstance(task_config, SyncTask):
            task_id = task_config.id
            app_id = task_config.app_id
            entry_id = task_config.entry_id
            table_name = task_config.table_name
            sync_type = task_config.sync_type
            if task_config.department:
                department_id = task_config.department.id
                department_name = task_config.department.department_name

    try:
        # --- 安全地尝试获取会话 ---
        try:
            # 只有在有应用上下文且 g 对象包含 config_session 时才使用它
            if g and hasattr(g, 'config_session') and g.config_session.is_active:
                session = g.config_session
                logger.debug("log_sync_error: Using existing session from g object.")
            else:
                raise RuntimeError("No active session in g")  # 跳到 except 块
        except (RuntimeError, AttributeError):
            # 如果发生 RuntimeError (Working outside...) 或 g 不存在/没有 config_session
            logger.debug("log_sync_error: Not in request context or no session in g, creating new session.")
            session = ConfigSession()
            session_created = True

        # 1. 准备日志数据
        error_message = f"{extra_info}\n{str(error)}" if extra_info and error else (
            str(error) if error else extra_info or "Unknown error")
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
            sync_type=sync_type,  # 记录同步类型
            app_id=app_id,
            entry_id=entry_id,
            table_name=table_name,
            department_id=department_id,
            department_name=department_name or "N/A",
            error_message=error_message,
            traceback=traceback_str,  # 使用格式化后的字符串
            payload=payload_str,
            timestamp=datetime.now(TZ_UTC_8)
        )
        session.add(new_log)
        session.commit()
        logger.info(f"Error log successfully written to database: (Task {task_id or 'N/A'})")

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
        logger.error(f"!!! Critical error occurred while writing error log to database: {db_err}")
        # 确保 error_message 已定义
        if 'error_message' not in locals():
            error_message = str(error) if error else "Unknown original error"
        logger.error(f"Original error message: {error_message}")
        # 如果连日志会话都有问题，尝试回滚并打印更详细的错误
        if session:
            try:
                session.rollback()
            except Exception as rb_err:
                logger.error(f"!!! Failed to rollback error log session: {rb_err}")
        # 打印数据库错误的堆栈信息
        logger.error("--- Database Error Traceback ---")
        logger.error(traceback.format_exc())
        logger.error("-------------------------------")

    finally:
        # 仅当此函数自己创建了会话时才关闭它
        if session and session_created:
            logger.debug("log_sync_error: Closing independently created session.")
            session.close()


# --- 名称与类型转换 ---

def convert_to_pinyin(name: str) -> str | None:
    """将包含中文的名称转换为全小写的拼音下划线风格，便于用作表名"""
    if not name:
        return ""
    # 检查名称中是否包含中文字符
    if re.search(r'[\u4e00-\u9fa5]', name):
        # 如果有，先将中文转换为拼音
        pinyin_list = pinyin(name, style=Style.NORMAL)
        # 将拼音列表连接成一个字符串
        name = '_'.join(item[0] for item in pinyin_list if item and item[0])  # 添加检查防止空item

    # 对转换后（或原始的英文）字符串进行清理
    # 1. 替换所有非字母、数字、下划线的字符为空字符串
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    # 2. 将一个或多个连续的下划线合并为单个下划线
    name = re.sub(r'_+', '_', name)
    # 3. 确保不以下划线开头或结尾
    name = name.strip('_')
    # 4. 转换为小写
    name = name.lower()
    # 5. 如果名称为空或变为纯下划线，提供默认名称
    if not name or name == '_':
        return None
    # 6. 确保不以数字开头 (如果数据库有此限制)
    if name and name[0].isdigit():
        name = '_' + name

    return name


# 获取数据库连接字符串
def get_connection_url(db_type, db_host, db_port, db_name, db_user, db_password, db_args):
    if not db_type or not db_type.strip():
        raise ValueError(f"Unsupported database type: {db_type}")

    elif db_type.upper() == 'MYSQL' or db_type.upper() == 'MYSQL+PYMYSQL':
        db_url = f"mysql+pymysql://{db_user}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
        if db_args:
            db_url += f"?{db_args}"
    elif db_type.upper() == 'SQL SERVER' or db_type.upper() == 'MSSQL+PYMSSQL':
        db_url = f"mssql+pymssql://{db_user}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
        if db_args:
            db_url += f"?{db_args}"
    elif db_type.upper() == 'POSTGRESQL' or db_type.upper() == 'POSTGRESQL+PSYCOPG2':
        db_url = f"postgresql+psycopg2://{db_user}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
        if db_args:
            db_url += f"?{db_args}"
    elif db_type.upper() == 'ORACLE' or db_type.upper() == 'ORACLE+CX_ORACLE':
        db_url = f"oracle+cx_oracle://{db_user}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
        if db_args:
            db_url += f"?{db_args}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    return db_url


# 获取数据库驱动
def get_db_driver(db_type: str) -> str:
    driver_map = {
        'MySQL': 'mysql+pymysql',
        'PostgreSQL': 'postgresql+psycopg2',
        'SQL Server': '"mssql+pymssql',
        'Oracle': 'oracle+cx_oracle',
    }

    return driver_map.get(db_type)


# --- “测试连接”功能函数 ---
def test_db_connection(db_info: Dict[str, Any]) -> (bool, str):
    """
    尝试连接到数据库并执行一个简单查询。

    :param db_info: 包含连接参数的字典 (来自前端)
    :return: (bool: 是否成功, str: 消息)
    """
    try:
        db_type = db_info.get('db_type')
        db_user = db_info.get('db_user')
        db_password = db_info.get('db_password', '')  # 密码可能为空
        db_host = db_info.get('db_host')
        db_port = db_info.get('db_port')
        db_name = db_info.get('db_name')
        db_args = db_info.get('db_args', '')

        if not all([db_type, db_user, db_host, db_port, db_name]):
            return False, "数据库类型、主机、端口、库名和用户名均不能为空"

        # --- 1. 映射数据库类型到 SQLAlchemy 驱动 ---
        driver = get_db_driver(db_type)
        if not driver:
            return False, ValueError(f"Unsupported database type: {db_type}")

        # --- 2. 构建连接 URL ---
        db_url = get_connection_url(db_type, db_host, db_port, db_name, db_user, db_password, db_args)

        # --- 3. 尝试连接 ---
        engine = create_engine(db_url, connect_args={'connect_timeout': 5}, pool_recycle=3600)

        with engine.connect() as connection:
            # 执行一个简单的查询
            connection.execute(text("SELECT 1"))

        return True, "数据库连接成功！"

    except (OperationalError, DBAPIError) as e:
        logger.warning(f"数据库连接测试失败: {e}")
        # 返回一个更友好的错误信息
        error_msg = str(e).split('\n')[0]
        return False, f"连接失败: {error_msg}"
    except ImportError as e:
        logger.error(f"Database driver not installed: {e}")
        return False, f"连接失败: 缺少数据库驱动 {e}。 (例如: 'mysql' 需要 'pymysql')"
    except Exception as e:
        logger.error(f"Unknown error occurred during database connection test: {e}")
        return False, f"发生未知错误: {e}"


# --- Webhook 签名验证 ---
def get_signature(nonce: str, payload: str, secret: str, timestamp: str) -> str:
    """
    根据简道云指南计算 SHA1 签名。
    """
    content = f'{nonce}:{payload}:{secret}:{timestamp}'.encode('utf-8')
    m = hashlib.sha1()
    m.update(content)
    return m.hexdigest()


def validate_signature(nonce: str, payload_str: str, secret: str, timestamp: str, signature_from_header: str) -> bool:
    """
    验证传入的 Webhook 签名是否有效。
    :param nonce: URL 参数 'nonce'
    :param payload_str: 原始请求体 (字符串)
    :param secret: JdyKeyInfo.api_secret
    :param timestamp: URL 参数 'timestamp'
    :param signature_from_header: 'X-JDY-Signature'
    :return: (bool) 是否验证通过
    """
    if not all([nonce, payload_str is not None, secret, timestamp, signature_from_header]):
        logger.warning(
            f"[Webhook Auth] Signature verification failed: Missing required parameters. Nonce: {nonce}, TS: {timestamp}, Secret: {bool(secret)}, Header: {signature_from_header}")
        return False

    try:
        calculated_signature = get_signature(nonce, payload_str, secret, timestamp)

        if calculated_signature == signature_from_header:
            logger.info("[Webhook Auth] Signature verification successful.")
            return True
        else:
            logger.warning(f"[Webhook Auth] Signature verification failed: Signature mismatch.")
            logger.debug(f"Nonce: {nonce}, TS: {timestamp}")
            logger.debug(f"Payload (first 100): {payload_str[:100]}...")
            logger.debug(f"Expected: {signature_from_header}")
            logger.debug(f"Calculated: {calculated_signature}")
            return False

    except Exception as e:
        logger.error(f"[Webhook Auth] Error occurred while calculating signature: {e}", exc_info=True)
        return False
