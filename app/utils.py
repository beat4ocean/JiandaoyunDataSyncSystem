import datetime
import json
import logging
import re
import time
import time as time_module
import traceback
from datetime import datetime, date, time, timedelta, timezone
from datetime import time as time_obj
from decimal import Decimal
from functools import wraps
from typing import Dict, Any
from urllib.parse import quote_plus, urlunparse

import requests
from pypinyin import pinyin, Style
from sqlalchemy import (String, Float, DateTime, Text, JSON, Time, Boolean, BigInteger, Integer)
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.exc import DBAPIError
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

    # 如果是 Decimal 对象，将其转换为 float
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
    department_id = None
    department_name = None

    if task_config:
        # 延迟导入 SyncTask 以进行类型检查
        from app.models import SyncTask
        if isinstance(task_config, SyncTask):
            task_id = task_config.id
            app_id = task_config.app_id
            entry_id = task_config.entry_id
            table_name = task_config.table_name
            department_id = task_config.department.id
            department_name = task_config.department.department_name

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
            department_id=department_id,
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


# --- 简道云同步数据库 增加内容开始 ---
# --- 名称与类型转换 ---

def convert_to_pinyin(name: str) -> str:
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
        return "invalid_name"
    # 6. 确保不以数字开头 (如果数据库有此限制)
    if name[0].isdigit():
        name = '_' + name

    return name


def get_table_name(payload: dict) -> (str, str):
    """从数据中生成表名，并将中文转换为拼音，确保表名合法。"""
    data = payload.get('data')
    op = payload.get('op')

    # 优先从 data 中获取 name 或 formName
    form_name = None
    if isinstance(data, dict):
        # form_update 事件使用 'name'
        if op == 'form_update':
            form_name = data.get('name')
        # 其他事件通常使用 'formName'
        if not form_name:
            form_name = data.get('formName')

    # 如果 data 中没有，尝试从 payload 顶层获取（某些旧事件可能如此）
    if not form_name:
        form_name = payload.get('formName')

    # 如果都没有，提供一个基于 entryId 的默认值
    if not form_name:
        entry_id_from_data = data.get('entryId') if isinstance(data, dict) else None
        entry_id_from_payload = payload.get('entryId')
        entry_id = entry_id_from_data or entry_id_from_payload
        form_name = f"unknown_form_{entry_id}" if entry_id else "unknown_form"
        print(f"警告: 无法从 payload 中确定表单名称，使用默认值: {form_name}")

    pinyin_name = convert_to_pinyin(form_name)
    # 返回原始名称和转换后的名称
    return pinyin_name, form_name


def get_column_name(widget: dict, use_label_pinyin: bool) -> str:
    """
    根据动态配置获取数据库列名（已重构）。
    1. 字段别名 (name)，如果不是默认的 _widget_ 开头
    2. 字段标题 (label)，根据配置决定是否转拼音
    3. 字段ID (widgetName) 作为备用
    :param widget: 简道云的字段对象
    :param use_label_pinyin: 是否将 Label 转为拼音
    :return: 数据库列名
    """
    alias = widget.get('name')
    label = widget.get('label')
    widget_name = widget.get('widgetName')  # 这是字段 ID，通常是 _widget_ 开头

    final_name = None

    # 1. 优先使用别名 (如果已设置且非默认)
    if alias and isinstance(alias, str) and not alias.startswith('_widget_'):
        # 别名通常是用户自定义的，可能包含非法字符，需要清理
        final_name = alias
        # print(f"使用别名: {alias} -> {final_name}")

    # 2. 其次使用 label
    if not final_name and label and isinstance(label, str):
        if use_label_pinyin:
            final_name = convert_to_pinyin(label)
            # print(f"使用标签 (拼音): {label} -> {final_name}")
        else:
            final_name = label
            # print(f"使用标签 (原文清理): {label} -> {final_name}")

    # 3. 最后备用 widgetName (字段 ID)
    if not final_name and widget_name and isinstance(widget_name, str):
        # widgetName 通常是 _widget_xxx
        final_name = widget_name
        # print(f"使用 Widget Name: {widget_name} -> {final_name}")

    # 添加一个最终的非空检查
    if not final_name:
        print(f"警告：无法为 widget {widget} 生成有效的列名，将使用 'invalid_column'")
        final_name = 'invalid_column'

    return final_name


def get_sql_type(jdy_type: str, data_value: any):
    """
    根据简道云的字段类型（优先）或值的Python类型推断出合适的 SQLAlchemy 数据类型。
    :param jdy_type: 从 `form_fields_mapping` 表中获取的简道云字段类型字符串。
    :param data_value: 字段的实际值，用于备用推断。
    :return: SQLAlchemy 类型实例 (e.g., Text(), Float(), JSON())。
    """
    # 简道云字段类型到SQLAlchemy类型的映射 (返回类型类)
    JDY_TYPE_TO_SQLALCHEMY_CLASS = {
        'text': Text, 'textarea': Text, 'serial_number': String,  # Use String for serial
        'radiogroup': String, 'combo': String, 'calculation': Text,  # Calculation might be long
        'number': Float, 'money': Float,  # Add money type
        'datetime': DateTime, 'date': DateTime, 'time': Time,  # Add date and time
        'address': JSON, 'location': JSON, 'signature': JSON,
        'user': JSON, 'dept': JSON, 'phone': JSON, 'member': JSON,  # member is alias for user/dept
        'lookup': JSON, 'linkdata': JSON, 'formula': JSON,  # Treat formula results as potentially complex
        'checkboxgroup': JSON, 'combocheck': JSON, 'image': JSON,
        'upload': JSON, 'subform': JSON, 'widget_relation': JSON,  # Relation widget
        'usergroup': JSON, 'deptgroup': JSON,
        'cascader': JSON,  # Add cascader
        'rate': Float,  # Add rate
        'progress': Integer,  # Add progress
        'autonumber': BigInteger,  # Add autonumber (treat as big int)
        'flowstate': BigInteger,  # Keep as BigInteger
        'boolean': Boolean  # Add boolean explicitly
    }

    sql_type_class = None

    # 1. 优先根据简道云的字段类型进行映射
    if jdy_type and jdy_type in JDY_TYPE_TO_SQLALCHEMY_CLASS:
        sql_type_class = JDY_TYPE_TO_SQLALCHEMY_CLASS[jdy_type]
        # print(f"JDY Type '{jdy_type}' mapped to {sql_type_class.__name__}")

    # 2. 如果类型映射成功，但需要根据值调整 (例如 String vs Text)
    if sql_type_class:
        if sql_type_class in (Text, String) and isinstance(data_value, str):
            if len(data_value) > 65535:
                # print(f"Value length > 65535, promoting to LONGTEXT")
                return LONGTEXT()
            elif len(data_value) > 1024:
                # print(f"Value length > 1024, promoting to TEXT")
                return Text()
            else:
                # print(f"Value fits in String(1024)")
                # For serial_number, radiogroup, combo, allow longer String if needed, e.g., String(255)
                # Adjust based on expected max length for these types
                return String(1024)
        elif sql_type_class is Float and isinstance(data_value, int):
            # Allow integers to be stored in Float columns
            # print("Integer value for Float type, allowed.")
            return Float()
        elif sql_type_class is BigInteger and isinstance(data_value, (int, str)):
            # Allow potential string representation of big integers
            try:
                int(data_value)  # Check if convertible
                # print("String/Int value for BigInteger type, allowed.")
                return BigInteger()
            except (ValueError, TypeError):
                print(f"警告：值 '{data_value}' 无法转换为 BigInteger，将使用 Text。")
                return Text()  # Fallback if value cannot be converted
        # If type is mapped and doesn't need value adjustment, return instance
        # print(f"Returning instance: {sql_type_class.__name__}()")
        return sql_type_class

    # 3. 如果没有提供简道云类型或类型未知，则回退到基于值的推断
    # print(f"No JDY type or unknown type '{jdy_type}', inferring from value type: {type(data_value).__name__}")
    if isinstance(data_value, bool):
        return Boolean()
    if isinstance(data_value, int):
        # Consider magnitude for Int vs BigInt if necessary
        return BigInteger()  # Default to BigInteger for safety
    if isinstance(data_value, float):
        return Float()
    if isinstance(data_value, (dict, list)):
        return JSON()
    if isinstance(data_value, str):
        # 尝试检查是否为日期时间格式
        try:
            # Add more robust date/time checking if needed
            if len(data_value) >= 10:  # Basic check
                # Try parsing common formats
                datetime.fromisoformat(data_value.replace('Z', '+00:00'))
                return DateTime()
        except (ValueError, TypeError):
            pass  # Not a standard ISO datetime

        # 检查是否像时间 "HH:MM:SS"
        if re.match(r'^\d{2}:\d{2}:\d{2}$', data_value):
            try:
                time.fromisoformat(data_value)
                return Time()
            except ValueError:
                pass  # Not a valid time string

        # 根据长度决定使用 String/TEXT/LONGTEXT
        if len(data_value) > 65535:
            return LONGTEXT()
        elif len(data_value) > 1024:
            return Text()
        return String(1024)  # Default string length

    # Default fallback for unknown types
    # print("Unknown value type, falling back to Text()")
    return Text()


# --- 简道云同步数据库 增加内容结束 ---

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
        #    (e.g., pymysql, psycopg2-binary, pyodbc)
        driver_map = {
            'mysql+pymysql': 'mysql+pymysql',
            'postgresql+psycopg2': 'postgresql+psycopg2',
            # 'mssql+pyodbc': 'mssql+pyodbc',
            'oracle+cx_oracle': 'oracle+cx_oracle',
        }

        driver = driver_map.get(db_type)
        if not driver:
            return False, f"不支持的数据库类型: {db_type}。支持的类型: {list(driver_map.keys())}"

        # --- 2. 构建连接 URL (确保密码被正确编码) ---
        # 密码中的特殊字符 (如 @, :, /) 需要 URL 编码
        encoded_password = quote_plus(db_password)

        netloc = f"{db_user}:{encoded_password}@{db_host}:{db_port}"

        # # SQL Server (mssql) 可能需要特殊的 DSN 或 驱动参数
        # if db_type == 'mssql+pyodbc' and not db_args:
        #     # 如果用户没有提供 db_args，我们提供一个合理的默认值
        #     # 注意：这需要安装 'ODBC Driver 17 for SQL Server'
        #     db_args = "driver=ODBC+Driver+17+for+SQL+Server"

        # --- 3. 构造最终的 URL ---
        # 使用 urlunparse 来正确组合
        db_url = urlunparse((
            driver,  # scheme
            netloc,  # netloc
            f"/{db_name}",  # path
            "",  # params
            db_args,  # query
            ""  # fragment
        ))

        # # 移除 mssql 在 path 上的 /
        # if db_type == 'mssql+pyodbc':
        #     db_url = db_url.replace(f"///{db_name}", f"/{db_name}", 1)

        # --- 4. 尝试连接 ---
        # connect_args={'connect_timeout': 5} 设置5秒超时
        engine = create_engine(db_url, connect_args={'connect_timeout': 5}, pool_recycle=3600)

        with engine.connect() as connection:
            # 执行一个简单的查询
            connection.execute(text("SELECT 1"))

        return True, "数据库连接成功！"

    except (OperationalError, DBAPIError) as e:
        logging.warning(f"数据库连接测试失败: {e}")
        # 返回一个更友好的错误信息
        error_msg = str(e).split('\n')[0]
        return False, f"连接失败: {error_msg}"
    except ImportError as e:
        logging.error(f"数据库驱动未安装: {e}")
        return False, f"连接失败: 缺少数据库驱动 {e}。 (例如: 'mysql' 需要 'pymysql')"
    except Exception as e:
        logging.error(f"数据库连接测试发生未知错误: {e}")
        return False, f"发生未知错误: {e}"
