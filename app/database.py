# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager
from datetime import timedelta, timezone
from typing import Any, Generator, Dict
from urllib.parse import quote_plus

from sqlalchemy import MetaData, Table, text
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DBAPIError
from sqlalchemy.orm import sessionmaker

from app import ConfigSession
from app.config import DB_CONNECT_ARGS
from app.models import Database, SyncTask

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TZ_UTC_8 = timezone(timedelta(hours=8))

# --- 动态引擎缓存 ---
# 缓存 {database_info_id: (engine, SessionLocal)}
dynamic_engine_cache = {}

# --- 动态元数据缓存 ---
# 用于缓存已检查过的表结构 { engine_url: { table_name: Table } }
inspected_tables_cache: dict[str, dict[str, Table]] = {}
# 用于缓存动态引擎的 MetaData 对象 { engine_url: MetaData }
dynamic_metadata_cache: dict[str, MetaData] = {}


# --- 获取动态引擎 ---
@contextmanager
def get_dynamic_session(task: SyncTask) -> Generator[Any, Any, None]:
    """
    一个上下文管理器，用于根据任务动态获取源数据库会话。
    它会缓存引擎以提高性能。
    """

    if not task.database:
        with ConfigSession() as config_session:
            db_info = config_session.query(Database).get(task.database_id)
            if not db_info:
                raise ValueError(f"task_id:[{task.id}] Source Database (ID: {task.database_id}) not found.")
    else:
        db_info = task.database

    if not db_info.is_active:
        raise ValueError(f"task_id:[{task.id}] Source Database '{db_info.db_show_name}' is not active.")

    db_id = db_info.id

    # 检查缓存
    if db_id not in dynamic_engine_cache:
        logger.info(
            f"task_id:[{task.id}] Creating new dynamic engine for source DB: {db_info.db_show_name} (ID: {db_id})")

        # --- 根据 db_type 构建 URL ---
        try:
            db_url = get_connection_url(
                db_type=db_info.db_type,
                db_host=db_info.db_host,
                db_port=db_info.db_port,
                db_name=db_info.db_name,
                db_user=db_info.db_user,
                db_password=db_info.db_password,
                db_args=db_info.db_args
            )
        except ValueError as e:
            logger.error(f"task_id:[{task.id}] Failed to build connection URL: {e}")
            raise e
        except ImportError as e:
            logger.error(f"task_id:[{task.id}] Missing database driver: {e}")
            raise e

        engine = create_engine(db_url, pool_recycle=3600, connect_args=DB_CONNECT_ARGS)
        session_local = sessionmaker(bind=engine)

        # 缓存引擎和会话工厂
        dynamic_engine_cache[db_id] = (engine, session_local)

    # 从缓存中获取会话工厂
    _, session_local = dynamic_engine_cache[db_id]

    session = session_local()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_dynamic_engine(task: SyncTask):
    """获取动态引擎"""
    # 确保引擎在缓存中
    with get_dynamic_session(task):
        pass
    # 从缓存返回引擎
    database_id = task.database_id
    if database_id not in dynamic_engine_cache:
        raise RuntimeError(f"Dynamic engine for DB {database_id} not found in cache after get_dynamic_session.")
    return dynamic_engine_cache[database_id][0]


def get_dynamic_metadata(engine) -> MetaData:
    """获取或创建与动态引擎关联的 MetaData 对象"""
    engine_url = str(engine.url)
    if engine_url not in dynamic_metadata_cache:
        dynamic_metadata_cache[engine_url] = MetaData()
    return dynamic_metadata_cache[engine_url]


# def get_business_keys(task: SyncTask):
#     auto_detected_keys = None
#
#     table_name = task.table_name
#     business_keys = task.business_keys
#
#     # 仅在 提供了表名、且用户未手动提供 business_keys 时
#     if table_name and not business_keys:
#         try:
#             # 1. 获取引擎和检查器
#             engine = get_dynamic_engine(task)
#             inspector = inspect(engine)
#
#             # 2. 检查主键
#             pk_constraint = inspector.get_pk_constraint(table_name)
#             primary_keys = pk_constraint.get('constrained_columns', [])
#
#             if primary_keys:
#                 auto_detected_keys = primary_keys
#                 logger.info(f"[add_task] Auto-detected primary keys for {table_name}: {primary_keys}")
#             else:
#                 # 3. 如果没有主键，检查唯一约束
#                 unique_constraints = inspector.get_unique_constraints(table_name)
#                 if unique_constraints:
#                     # 获取第一个唯一约束的列
#                     first_unique_constraint = unique_constraints[0]
#                     unique_keys = first_unique_constraint.get('column_names', [])
#                     if unique_keys:
#                         auto_detected_keys = unique_keys
#                         logger.info(f"[add_task] Auto-detected unique keys for {table_name}: {unique_keys}")
#
#             task.business_keys = auto_detected_keys
#
#         except NoSuchTableError:
#             logger.warning(
#                 f"[add_task] Table '{table_name}' not found during key inspection. Skipping auto-detection.")
#             # 表可能还不存在，或者名称错误，允许继续创建任务，但不自动填充
#         except Exception as e:
#             # 捕获其他数据库检查错误 (例如连接失败)
#             logger.error(f"[add_task] Error during key inspection for {table_name}: {e}")
#             log_sync_error(task_config=task, error=e, extra_info="Failed to auto-detect business keys.")


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

        # 确保 db_port 是 int
        try:
            db_port = int(db_port)
        except (ValueError, TypeError):
            return False, f"端口必须是数字: {db_port}"

        # --- 1. 构建连接 URL (使用 get_connection_url) ---
        db_url = get_connection_url(
            db_type=db_type,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,  # 传入原始密码
            db_args=db_args
        )

        # --- 2. 尝试连接 ---
        # 增加 Oracle 的连接参数
        connect_args = {'connect_timeout': 5}
        # if db_type == 'Oracle':
        #     connect_args['encoding'] = 'UTF-8' # oracledb 驱动通常自动处理

        engine = create_engine(db_url, connect_args=connect_args, pool_recycle=3600)

        dialect_name = engine.dialect.name.lower()

        with engine.connect() as connection:
            # 执行一个简单的查询
            query = "SELECT 1"
            # Oracle 需要 `FROM dual`
            if dialect_name == 'oracle':
                query = "SELECT 1 FROM dual"

            connection.execute(text(query))

        return True, "数据库连接成功！"

    except (OperationalError, DBAPIError) as e:
        logger.warning(f"数据库连接测试失败: {e}")
        # 返回一个更友好的错误信息
        # 提取更清晰的错误
        error_msg = getattr(e, 'orig', e)
        # 适配不同驱动的错误提取
        if hasattr(error_msg, 'args'):
            # (code, message) or (message,)
            error_msg_str = error_msg.args[0]
            if isinstance(error_msg_str, (bytes)):
                try:
                    error_msg_str = error_msg_str.decode('utf-8')
                except:
                    error_msg_str = str(error_msg.args)
            else:
                error_msg_str = str(error_msg.args)
        else:
            error_msg_str = str(error_msg)

        error_msg_str = error_msg_str.split('\n')[0]
        return False, f"连接失败: {error_msg_str}"

    except ImportError as e:
        logger.error(f"Database driver not installed: {e}")
        # 提示用户安装
        driver_name = str(e).split("'")[-2]  # e.g. 'pymysql'
        install_cmd = ""
        if driver_name == 'pymysql':
            install_cmd = " (请在 requirements.txt 中添加 'pymysql')"
        elif driver_name == 'psycopg2':
            install_cmd = " (请在 requirements.txt 中添加 'psycopg2-binary')"
        elif driver_name == 'oracledb':
            install_cmd = " (请在 requirements.txt 中添加 'oracledb')"
        elif driver_name == 'cx_Oracle':
            install_cmd = " (请在 requirements.txt 中添加 'cx_Oracle')"
        return False, f"连接失败: 缺少数据库驱动 {e}。{install_cmd}"
    except Exception as e:
        logger.error(f"Unknown error occurred during database connection test: {e}")
        return False, f"发生未知错误: {e}"


# 获取数据库连接字符串
def get_connection_url(db_type, db_host, db_port, db_name, db_user, db_password, db_args):
    """
    根据 db_type 动态构建 SQLAlchemy 连接 URL。
    """
    if not db_type or not db_type.strip():
        raise ValueError(f"Unsupported database type: {db_type}")

    # 密码需要 URL 编码
    safe_password = quote_plus(db_password)

    # 统一使用 get_db_driver 获取驱动
    driver_dialect = get_db_driver(db_type)
    if not driver_dialect:
        raise ValueError(f"Unsupported database type: {db_type}")

    # 构建 URL
    db_url = f"{driver_dialect}://{db_user}:{safe_password}@{db_host}:{db_port}/{db_name}"

    # 特殊处理 Oracle 的 DSN 模式 (如果 db_name 包含 /)
    # oracledb 驱动支持 easy connect (host:port/service_name)
    # cx_Oracle 可能需要 DSN
    if driver_dialect.startswith('oracle') and '/' in db_name:
        # 假设 db_name 可能是 service_name, Oracle 驱动更喜欢这种格式
        # oracledb 支持 host:port/service_name
        db_url = f"{driver_dialect}://{db_user}:{safe_password}@{db_host}:{db_port}/{db_name}"

    # cx_Oracle DSN (如果需要)
    # if driver_dialect == 'oracle+cx_oracle':
    #     dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={db_host})(PORT={db_port}))(CONNECT_DATA=(SERVICE_NAME={db_name})))"
    #     db_url = f"oracle+cx_oracle://{db_user}:{safe_password}@{dsn}"

    if db_args:
        db_url += f"?{db_args}"

    return db_url


# 获取数据库驱动
def get_db_driver(db_type: str) -> str:
    """
    将用户友好的 db_type 映射到 SQLAlchemy 驱动字符串。
    """
    if not db_type:
        return None

    db_type_upper = db_type.upper()

    # 映射字典
    # 使用 requirements.txt 中指定的驱动
    driver_map = {
        'STARROCKS': 'mysql+pymysql',
        'MYSQL': 'mysql+pymysql',
        'POSTGRESQL': 'postgresql+psycopg2',
        'ORACLE': 'oracle+oracledb',  # 优先使用 oracledb
        # 'ORACLE': 'oracle+cx_oracle', # 如果 oracledb 有问题，切换到 cx_oracle
    }

    # 允许用户输入 'MySQL' 或 'mysql+pymysql'
    if '+' in db_type:
        # 假设用户输入了完整的驱动
        return db_type.lower()

    return driver_map.get(db_type_upper)
