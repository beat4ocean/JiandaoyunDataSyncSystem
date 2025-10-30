from datetime import datetime

from sqlalchemy import Index, Boolean, Time
from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
                        DateTime, Text, UniqueConstraint)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import CONFIG_DB_URL, SOURCE_DB_URL, DB_CONNECT_ARGS
from app.utils import TZ_UTC_8

# --- 数据库引擎和会话 ---

# 配置数据库 (存储租户、任务配置、日志)
config_engine = create_engine(CONFIG_DB_URL, pool_recycle=3600, connect_args=DB_CONNECT_ARGS)
ConfigSession = sessionmaker(bind=config_engine)
config_metadata = MetaData()

# 源数据库 (存储需要被同步的业务数据)
source_engine = create_engine(SOURCE_DB_URL, pool_recycle=3600, connect_args=DB_CONNECT_ARGS)
SourceSession = sessionmaker(bind=source_engine)
source_metadata = MetaData()


# --- 声明式基类 ---
class ConfigBase(DeclarativeBase):
    metadata = config_metadata


class SourceBase(DeclarativeBase):
    metadata = source_metadata


# --- 配置数据库模型 ---

# 2. 将 Table() 定义重写为 ORM 模型类

class SyncTask(ConfigBase):
    """
    同步任务配置表
    """
    __tablename__ = 'sync_tasks'

    task_id = Column(Integer, primary_key=True, autoincrement=True, comment="任务ID")
    task_name = Column(String(255), nullable=True, comment="任务名称")

    # 源数据库配置
    source_table = Column(String(255), nullable=False, comment="源数据库表名")
    # 业务主键
    pk_field_names = Column(String(255), nullable=False, comment="源表主键字段名 (复合主键用英文逗号分隔)")

    # 简道云配置
    jdy_app_id = Column(String(100), nullable=False, comment="简道云应用ID")
    jdy_entry_id = Column(String(100), nullable=False, comment="简道云表单ID")
    jdy_api_key = Column(String(255), nullable=False, comment="简道云 API Key")

    # 同步模式
    sync_mode = Column(String(50), nullable=False, default='INCREMENTAL',
                       comment="同步模式 (FULL_REPLACE, INCREMENTAL, BINLOG)")
    # 增量同步依赖的时间字段
    incremental_field = Column(String(100), comment="增量同步依赖的时间字段 (仅 INCREMENTAL 模式)")
    # 增量同步间隔 (分钟)
    incremental_interval = Column(Integer, comment="增量同步间隔 (分钟) (仅 INCREMENTAL 模式)")
    # 全量同步定时时间
    full_replace_time = Column(Time, nullable=True, comment="全量同步时间 (仅 FULL_REPLACE 模式)")
    # 源数据库过滤
    source_filter_sql = Column(Text, nullable=True, comment="源数据库过滤 SQL (用于 INCREMENTAL 和 FULL_REPLACE模式)")
    # 状态与日志
    status = Column(String(20), default='idle', comment="任务状态 (idle, running, error, disabled)")
    # 上次同步时间
    last_sync_time = Column(DateTime, comment="上次同步时间 (用于 INCREMENTAL 和 FULL_REPLACE模式)")
    last_binlog_file = Column(String(255), comment="上次同步的 binlog 文件 (用于 BINLOG)")
    last_binlog_pos = Column(Integer, comment="上次同步的 binlog 位置 (用于 BINLOG)")
    # 是否首次同步执行全量同步
    is_full_sync_first = Column(Boolean, default=True,
                                comment="是否首次同步执行全量同步 (用于 INCREMENTAL 和 BINLOG 模式)")
    is_active = Column(Boolean, default=True, comment="任务是否启用")
    send_error_log_to_wecom = Column(Boolean, default=False, nullable=False, comment="是否发送错误日志到企微")
    wecom_robot_webhook_url = Column(String(500), nullable=True, comment="企业微信机器人 Webhook URL")

    created_at = Column(DateTime, default=datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now(TZ_UTC_8), onupdate=datetime.now(TZ_UTC_8),
                        comment="更新时间")

    __table_args__ = (
        Index('idx_mode_status', 'sync_mode', 'status'),
    )


class FormFieldMapping(ConfigBase):
    """
    表单字段映射缓存表
    """
    __tablename__ = 'form_fields_mapping'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, nullable=False, comment="关联的任务ID (替换了 app_id 和 entry_id)")

    form_name = Column(String(255), nullable=True, comment="简道云表单名")
    widget_name = Column(String(255), nullable=False, comment="字段ID (e.g., _widget_xxx_, 用于 API 提交)")
    widget_alias = Column(String(255), nullable=False, comment="字段后端别名 (name, 用于 API 查询/匹配)")
    label = Column(String(255), nullable=False, comment="字段前端别名 (label)")
    type = Column(String(255), nullable=False, comment="字段类型 (type)")

    # 存储表单数据的最新修改时间
    data_modify_time = Column(DateTime, nullable=True, comment="表单数据修改时间")

    last_updated = Column(DateTime, default=datetime.now(TZ_UTC_8), onupdate=datetime.now(TZ_UTC_8))

    __table_args__ = (
        # 确保每个任务中，widget_alias (MySQL 的列名) 是唯一的
        UniqueConstraint('task_id', 'widget_alias', name='uq_task_widget_alias'),
        # 确保 widget_name 也是唯一的
        UniqueConstraint('task_id', 'widget_name', name='uq_task_widget_name'),
        Index('idx_task_id', 'task_id'),
    )


class SyncErrLog(ConfigBase):
    """
    存储同步过程中的*错误*日志
    """
    __tablename__ = 'sync_err_log'
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 允许 task_id 为空, 以防 task_config 未传入
    task_id = Column(Integer, nullable=True, comment="关联的任务ID")
    app_id = Column(String(100), nullable=True)
    entry_id = Column(String(100), nullable=True)
    table_name = Column(String(255), nullable=True)

    error_message = Column(Text, nullable=False)
    traceback = Column(Text, nullable=True)
    payload = Column(LONGTEXT, nullable=True)
    timestamp = Column(DateTime, default=datetime.now(TZ_UTC_8), comment="发生错误时间")

    __table_args__ = (
        Index('idx_task_time', 'task_id', 'timestamp'),
    )
