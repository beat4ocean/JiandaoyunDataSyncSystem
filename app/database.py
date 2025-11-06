# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager
from datetime import timedelta, timezone
from typing import Any, Generator
from urllib.parse import quote_plus

from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import ConfigSession
from app.config import DB_CONNECT_ARGS
from app.models import Database, SyncTask
from app.utils import get_db_driver

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
        logger.error(
            f"task_id:[{task.id}] Creating new dynamic engine for source DB: {db_info.db_show_name} (ID: {db_id})")

        # --- 根据 db_type 构建 URL ---
        driver = get_db_driver(db_info.db_type)
        if not driver:
            raise ValueError(f"Unsupported database type: {db_info.db_type}")

        # 构建连接字符串
        db_url = (
            f"{driver}://{db_info.db_user}:{quote_plus(db_info.db_password)}@"
            f"{db_info.db_host}:{db_info.db_port}/{db_info.db_name}"
        )

        if db_info.db_args:
            db_url += f"?{db_info.db_args}"

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
