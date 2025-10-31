from datetime import datetime

from sqlalchemy import (Index, Boolean, Time, create_engine, MetaData, Column, Integer, String,
                        DateTime, Text, UniqueConstraint, ForeignKey)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import sessionmaker, DeclarativeBase, relationship
from passlib.hash import pbkdf2_sha256 as sha256

from app.config import CONFIG_DB_URL, SOURCE_DB_URL, DB_CONNECT_ARGS
from app.utils import TZ_UTC_8

# --- 数据库引擎和会话 ---

# 配置数据库 (存储任务、日志、用户)
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


# --- 新增模型 ---

class User(ConfigBase):
    """存储用户信息"""
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(120), unique=True, nullable=False, comment="用户名")
    password_hash = Column(String(128), comment="密码哈希")  # 存储密码哈希

    def set_password(self, password):
        """设置密码 (加密)"""
        self.password_hash = sha256.hash(password)

    def check_password(self, password):
        """校验密码"""
        return sha256.verify(password, self.password_hash)


# --- 密钥/部门模型 ---

class JdyKeyInfo(ConfigBase):
    """
    存储部门（租户）与简道云项目API Key的映射关系
    """
    __tablename__ = 'jdy_key_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    department_name = Column(String(100), nullable=False, unique=True, comment="部门英文简称, e.g., 'dpt_a'")
    api_key = Column(String(255), nullable=False, comment="该项目专属的 API Key")

    # 与任务的关联
    tasks = relationship("SyncTask", back_populates="department")


# --- 更新的现有模型 ---

class SyncTask(ConfigBase):
    """
    存储同步任务的配置信息
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

    # 删除 jdy_api_key，替换为与部门的关联
    department_name = Column(String(100), ForeignKey('jdy_key_info.department_name'), nullable=False,
                             comment="关联的部门 (用于获取 API Key)")
    department = relationship("JdyKeyInfo", back_populates="tasks")

    # 同步模式
    sync_mode = Column(String(50), nullable=False, default='INCREMENTAL',
                       comment="同步模式 (FULL_REPLACE, INCREMENTAL, BINLOG)")
    # 增量同步依赖的时间字段
    incremental_field = Column(String(100), comment="增量同步依赖的时间字段 (仅 INCREMENTAL 模式)")
    # 增量同步间隔 (分钟)
    incremental_interval = Column(Integer, comment="增量同步间隔 (分钟) (仅 INCREMENTAL 模式)")
    # 全量同步定时时间
    full_replace_time = Column(Time, nullable=True, comment="全量同步时间 (仅 FULL_REPLACE 模式)")
    # 源数据 SQL 过滤器
    source_filter_sql = Column(Text, nullable=True, comment="源数据库过滤 SQL (用于 INCREMENTAL 和 FULL_REPLACE模式)")

    # 状态和日志
    status = Column(String(20), default='idle', comment="任务状态 (idle, running, error, disabled)")
    # 上次同步时间
    last_sync_time = Column(DateTime, comment="上次同步时间 (用于 INCREMENTAL 和 FULL_REPLACE模式)")
    last_binlog_file = Column(String(255), comment="上次同步的 binlog 文件 (用于 BINLOG)")
    last_binlog_pos = Column(Integer, comment="上次同步的 binlog 位置 (用于 BINLOG)")
    # 是否首次同步执行全量覆盖同步
    is_full_replace_first = Column(Boolean, default=True,
                                   comment="是否首次同步执行全量覆盖同步 (用于 INCREMENTAL 和 BINLOG 模式)")
    is_active = Column(Boolean, default=True, comment="任务是否启用")
    send_error_log_to_wecom = Column(Boolean, default=False, nullable=False, comment="是否发送错误日志到企微")
    wecom_robot_webhook_url = Column(String(500), nullable=True, comment="企业微信机器人 Webhook URL")

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    __table_args__ = (
        Index('idx_mode_status', 'sync_mode', 'status'),
        UniqueConstraint('jdy_app_id', 'jdy_entry_id', name='uq_app_entry'),
    )


class FormFieldMapping(ConfigBase):
    """
    表单字段映射缓存表
    """
    __tablename__ = 'form_fields_mapping'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, nullable=False, comment="关联的任务ID")  # 已经使用 task_id

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


# --- 错误日志模型 ---

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

    # 为符合 UI 要求添加
    department_name = Column(String(100), nullable=True, comment="关联的部门")

    error_message = Column(Text, nullable=False)
    traceback = Column(Text, nullable=True)
    payload = Column(LONGTEXT, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="发生错误时间")

    __table_args__ = (
        Index('idx_task_time', 'task_id', 'timestamp'),
    )
