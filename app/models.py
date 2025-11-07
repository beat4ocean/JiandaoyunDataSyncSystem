# -*- coding: utf-8 -*-
from datetime import datetime

from passlib.hash import pbkdf2_sha256 as sha256
from sqlalchemy import (Index, Boolean, Time, create_engine, MetaData, Column, Integer, String,
                        DateTime, Text, UniqueConstraint, ForeignKey)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import sessionmaker, DeclarativeBase, relationship

from app.config import CONFIG_DB_URL, DB_CONNECT_ARGS
from app.utils import TZ_UTC_8

# --- 数据库引擎和会话 ---

# 配置数据库 (存储任务、日志、用户、数据库配置)
config_engine = create_engine(CONFIG_DB_URL, pool_recycle=3600, connect_args=DB_CONNECT_ARGS)
ConfigSession = sessionmaker(bind=config_engine)
config_metadata = MetaData()


# --- 声明式基类 ---
class ConfigBase(DeclarativeBase):
    metadata = config_metadata


# --- 部门模型 ---

class Department(ConfigBase):
    """
    存储租户信息 (部门即租户)
    这是所有多租户模型的““““根””””
    """
    __tablename__ = 'department'
    id = Column(Integer, primary_key=True, autoincrement=True, comment="关联的租户部门外部ID")
    # 用于前端展示
    department_name = Column(String(100), nullable=False, unique=True,
                             comment="关联的租户部门简称(英文), e.g., 'sft_dept'")
    department_full_name = Column(String(255), nullable=False, unique=True, comment="部门全称, e.g., '软件部门'")

    is_active = Column(Boolean, default=True, comment="是否激活")

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    # --- 租户拥有的资源 (Relationships) ---
    # 1. 租户下的用户
    users = relationship("User", back_populates="department", cascade="all, delete-orphan")
    # 2. 租户的数据库配置 (1:N 关系)
    databases = relationship("Database", back_populates="department", cascade="all, delete-orphan")
    # 3. 租户的简道云 Key (1:1 关系)
    jdy_key_info = relationship("JdyKeyInfo", back_populates="department", uselist=False, cascade="all, delete-orphan")
    # 4. 租户的同步任务
    sync_tasks = relationship("SyncTask", back_populates="department", cascade="all, delete-orphan")
    # 5. 租户的错误日志
    error_logs = relationship("SyncErrLog", back_populates="department", cascade="all, delete-orphan")


# --- 用户模型 ---

class User(ConfigBase):
    """存储用户信息，并关联到 *一个* 租户"""
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(120), unique=True, nullable=False, comment="用户名")
    password = Column(String(128), comment="密码")  # 存储密码哈希

    # 直接使用外键关联到 Department.id
    department_id = Column(Integer, ForeignKey('department.id'), nullable=False, comment="关联的租户ID")

    is_superuser = Column(Boolean, default=False, comment="是否是超级用户")
    is_active = Column(Boolean, default=True, comment="是否激活")

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    # 与 Department 的关系
    department = relationship("Department", back_populates="users")

    def set_password(self, password):
        """设置密码 (加密)"""
        self.password = sha256.hash(password)

    def check_password(self, password):
        """校验密码"""
        return sha256.verify(password, self.password)

    def set_is_superuser(self, is_superuser):
        """是否超级管理员"""
        return is_superuser


# --- 数据库模型 ---

class Database(ConfigBase):
    """
    存储 *源* 数据库连接配置，并关联到 *一个* 租户
    """
    __tablename__ = 'database'
    id = Column(Integer, primary_key=True, autoincrement=True)

    sync_type = Column(String(50), nullable=False, comment="同步类型: db2jdy (数据库->简道云), jdy2db (简道云->数据库)")

    db_show_name = Column(String(50), nullable=False, comment="数据库显示名称 (e.g., 质量部门专用数据库)")

    db_type = Column(String(50), nullable=False, comment="数据库类型(e.g. MySQL, PostgreSQL, Oracle, SQL Server)")
    db_host = Column(String(255), nullable=False, comment="数据库主机")
    db_port = Column(Integer, nullable=False, comment="数据库端口")
    db_name = Column(String(50), nullable=False, comment="数据库名称")
    db_args = Column(String(255), nullable=True, comment="数据库连接参数(e.g. charset=utf8mb4)")
    db_user = Column(String(50), nullable=False, comment="数据库用户名")
    db_password = Column(String(255), nullable=False, comment="数据库密码")  # 增加长度
    is_active = Column(Boolean, default=True, comment="是否激活")

    # 直接使用外键关联到 Department.id
    department_id = Column(Integer, ForeignKey('department.id'), nullable=False, comment="关联的租户ID")

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    # 关系应指向 Department
    department = relationship("Department", back_populates="databases")

    # 此数据库配置被哪些同步任务使用
    sync_tasks = relationship("SyncTask", back_populates="database")

    __table_args__ = (
        # 确保同一个租户下的显示名称是唯一的
        UniqueConstraint('department_id', 'db_show_name', name='uq_dept_db_show_name'),
        # 确保连接信息是唯一的
        UniqueConstraint('department_id', 'db_show_name', 'db_type', 'db_host', 'db_port', 'db_name', 'db_user',
                         name='uq_dept_db_connection_info'),
    )


# --- 简道云密钥模型 ---

class JdyKeyInfo(ConfigBase):
    """
    存储部门（租户）与简道云项目API Key的映射关系 (1:1)
    """
    __tablename__ = 'jdy_key_info'
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 直接使用外键，并添加 unique=True 实现 1:1 关系
    department_id = Column(Integer, ForeignKey('department.id'), nullable=False, unique=True, comment="关联的租户ID")

    api_key = Column(String(255), nullable=False, comment="该项目专属的 API Key")
    # api_secret = Column(String(255), nullable=True, comment="该项目专属的 API Secret")

    description = Column(String(255), nullable=True, comment="该项目描述")

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    # 关系指向 Department
    department = relationship("Department", back_populates="jdy_key_info")


# --- 数据同步配置信息模型 ---

class SyncTask(ConfigBase):
    """
    存储数据同步配置信息 (双向)，并关联到 *一个* 租户
    """
    __tablename__ = 'sync_tasks'

    id = Column(Integer, primary_key=True, autoincrement=True, comment="任务ID")

    sync_type = Column(String(10), nullable=False, server_default='db2jdy', comment="任务同步类型: db2jdy, jdy2db")

    # 任务名称
    task_name = Column(String(255), nullable=True, comment="任务名称")

    # 1. 关联到 Database
    database_id = Column(Integer, ForeignKey('database.id'), nullable=False, comment="数据库ID")

    # 2. 源表名 (db2jdy) 或 目标表名 (jdy2db)
    table_name = Column(String(255), nullable=True, comment="数据库表名 (源或目标)")

    # 业务主键 (db2jdy)
    business_keys = Column(String(255), nullable=True, comment="数据表主键字段名 (复合主键用英文逗号分隔)")

    # 简道云配置
    app_id = Column(String(100), nullable=True, comment="简道云应用ID (jdy2db 模式下可由 webhook 自动填充)")
    entry_id = Column(String(100), nullable=True, comment="简道云表单ID (jdy2db 模式下可由 webhook 自动填充)")

    # 直接使用外键关联到 Department.id
    department_id = Column(Integer, ForeignKey('department.id'), nullable=False, comment="关联的租户ID")

    # --- db2jdy (数据库 -> 简道云) 专属配置 ---
    sync_mode = Column(String(50), nullable=True, default='INCREMENTAL',
                       comment="同步模式 (FULL_SYNC, INCREMENTAL, BINLOG)")
    # 增量同步依赖的时间字段
    incremental_field = Column(String(100), comment="增量同步依赖的时间字段 (仅 INCREMENTAL 模式)")
    # 增量同步间隔 (分钟)
    incremental_interval = Column(Integer, comment="增量同步间隔 (分钟) (仅 INCREMENTAL 模式)")
    # 全量同步定时时间
    full_sync_time = Column(Time, nullable=True, comment="全量同步时间 (仅 FULL_SYNC 模式)")
    # 源数据 SQL 过滤器
    source_filter_sql = Column(Text, nullable=True, comment="源数据库过滤 SQL (用于 INCREMENTAL 和 FULL_SYNC模式)")

    last_binlog_file = Column(String(255), comment="上次同步的 binlog 文件 (用于 BINLOG)")
    last_binlog_pos = Column(Integer, comment="上次同步的 binlog 位置 (用于 BINLOG)")

    # --- 状态和日志 (通用) ---
    # 状态和日志
    sync_status = Column(String(20), default='idle', comment="任务状态 (idle, running, error, disabled)")
    # 上次同步时间
    last_sync_time = Column(DateTime, comment="上次同步时间 (用于 INCREMENTAL 和 FULL_SYNC模式)")

    # 是否首次同步执行全量同步
    is_full_sync_first = Column(Boolean, default=False,
                                comment="是否首次同步执行全量同步 (用于 INCREMENTAL, BINLOG, jdy2db)")
    # 是否首次同步清空目标数据 即：简道云到数据库：清空数据库，数据库到简道云：清空简道云表
    is_delete_first = Column(Boolean, default=False, nullable=False, comment="全量同步是否清空目标数据")

    is_active = Column(Boolean, default=True, comment="任务是否启用")
    send_error_log_to_wecom = Column(Boolean, default=False, nullable=False, comment="是否发送错误日志到企微")
    wecom_robot_webhook_url = Column(String(500), nullable=True, comment="企业微信机器人 Webhook URL")

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    # --- jdy2db (简道云 -> 数据库) 专属配置 ---
    # 添加 api_secret
    api_secret = Column(String(255), nullable=True, comment="简道云 Webhook API Secret (jdy2db 专属)")

    daily_sync_time = Column(Time, nullable=True, comment="简道云同步数据库的每日全量同步时间")
    daily_sync_type = Column(String(20), nullable=True, default='DAILY', comment="DAILY, ONCE")
    json_as_string = Column(Boolean, default=False, nullable=False, comment="是否将JSON存储为字符串")
    label_to_pinyin = Column(Boolean, default=False, nullable=False, comment="是否将label转换为拼音作为列名")
    webhook_url = Column(String(500), nullable=True, comment="简道云同步数据库的WebHook URL (后端自动生成)")

    # 关系1: 指向 Department
    department = relationship("Department", back_populates="sync_tasks")

    # 关系2: 指向源/目标数据库 ---
    database = relationship("Database", back_populates="sync_tasks")

    # 与 FormFieldMapping 的 1:N 关系
    field_mappings = relationship("FormFieldMapping", back_populates="task", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_mode_status', 'sync_mode', 'sync_status'),
        # UniqueConstraint('department_id', 'database_id', 'table_name', 'sync_type', name='uq_dept_db_table_synctype'),
    )


# --- 表单字段映射关系模型 ---

class FormFieldMapping(ConfigBase):
    """
    储存表单字段映射缓存表。
    租户属性通过 task_id 间接关联。
    """
    __tablename__ = 'form_fields_mapping'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 明确指定 ForeignKey
    task_id = Column(Integer, ForeignKey('sync_tasks.id'), nullable=False, comment="关联的任务ID")

    form_name = Column(String(255), nullable=True, comment="简道云表单名")
    widget_name = Column(String(255), nullable=False, comment="字段ID (e.g., _widget_xxx, 用于 API 提交)")
    widget_alias = Column(String(255), nullable=False, comment="字段后端别名 (name, 用于 API 查询/匹配)")
    label = Column(String(255), nullable=False, comment="字段前端别名 (label)")
    type = Column(String(255), nullable=False, comment="字段类型 (type)")

    # 存储表单数据的最新修改时间
    data_modify_time = Column(DateTime, nullable=True, comment="表单数据修改时间")

    last_updated = Column(DateTime, default=datetime.now(TZ_UTC_8), onupdate=datetime.now(TZ_UTC_8))

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    # 与 SyncTask 的关系
    task = relationship("SyncTask", back_populates="field_mappings")

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
    存储同步过程中的*错误*日志，并关联到 *一个* 租户
    """
    __tablename__ = 'sync_err_log'
    id = Column(Integer, primary_key=True, autoincrement=True)

    sync_type = Column(String(10), nullable=False, server_default='db2jdy', comment="任务同步类型: db2jdy, jdy2db")

    # 允许 task_id 为空, 以防 task_config 未传入
    task_id = Column(Integer, nullable=True, comment="关联的任务ID")
    app_id = Column(String(100), nullable=True)
    entry_id = Column(String(100), nullable=True)
    table_name = Column(String(255), nullable=True)

    # 直接使用外键关联到 Department.id
    department_id = Column(Integer, ForeignKey('department.id'), nullable=False, comment="关联的租户ID")
    department_name = Column(String(255), nullable=False, comment="关联的租户部门英文简称, e.g., 'dpt_a'")

    error_message = Column(Text, nullable=False)
    traceback = Column(Text, nullable=True)
    payload = Column(LONGTEXT, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="发生错误时间")

    created_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), comment="创建时间")
    updated_at = Column(DateTime, default=lambda: datetime.now(TZ_UTC_8), onupdate=lambda: datetime.now(TZ_UTC_8),
                        comment="更新时间")

    # 与 Department 的关系
    department = relationship("Department", back_populates="error_logs")

    __table_args__ = (
        Index('idx_task_time', 'task_id', 'timestamp'),
    )
