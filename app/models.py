from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, DateTime, Text, Index, UniqueConstraint
import datetime

# 1. 实例化 SQLAlchemy
db = SQLAlchemy()


# 2. 将 Table() 定义重写为 ORM 模型类
class SyncTask(db.Model):
    """
    同步任务配置表
    """
    __tablename__ = 'sync_tasks'

    task_id = Column(Integer, primary_key=True, autoincrement=True, comment="任务ID")
    task_name = Column(String(255), nullable=False, comment="任务名称")
    sync_mode = Column(String(50), nullable=False, comment="同步模式 (FULL_REPLACE, INCREMENTAL, BINLOG)")

    # 源数据库配置
    source_table = Column(String(255), nullable=False, comment="源数据库表名")
    pk_field_name = Column(String(255), nullable=False, comment="源表主键字段名")
    incremental_field = Column(String(100), comment="增量同步依赖的时间字段 (仅 INCREMENTAL 模式)")

    # 简道云配置
    jdy_app_id = Column(String(100), nullable=False, comment="简道云应用ID")
    jdy_entry_id = Column(String(100), nullable=False, comment="简道云表单ID")

    # 状态与日志
    status = Column(String(20), default='idle', comment="任务状态 (idle, running, error, disabled)")
    last_sync_time = Column(DateTime, comment="上次同步时间 (用于 INCREMENTAL)")
    last_binlog_file = Column(String(255), comment="上次同步的 binlog 文件 (用于 BINLOG)")
    last_binlog_pos = Column(Integer, comment="上次同步的 binlog 位置 (用于 BINLOG)")
    last_error_message = Column(Text, comment="最新错误信息")

    created_at = Column(DateTime, default=datetime.datetime.utcnow, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow,
                        comment="更新时间")

    __table_args__ = (
        Index('idx_mode_status', 'sync_mode', 'status'),
    )


class FormFieldMapping(db.Model):
    """
    表单字段映射缓存表
    """
    __tablename__ = 'form_fields_mapping'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, nullable=False, comment="关联的任务ID")
    source_field = Column(String(255), nullable=False, comment="源表字段名")
    jdy_field = Column(String(255), nullable=False, comment="简道云字段名 (API 字段名)")
    jdy_field_type = Column(String(100), comment="简道云字段类型 (如 text, number, datetime)")

    __table_args__ = (
        UniqueConstraint('task_id', 'source_field', name='uq_task_source'),
        Index('idx_task_id', 'task_id'),
    )


class SyncErrLog(db.Model):
    """
    同步错误日志表
    """
    __tablename__ = 'sync_err_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, nullable=False, comment="关联的任务ID")
    error_time = Column(DateTime, default=datetime.datetime.utcnow, comment="错误发生时间")
    error_message = Column(Text, nullable=False, comment="错误详情")
    data_content = Column(Text, comment="导致错误的原始数据 (JSON)")

    __table_args__ = (
        Index('idx_task_time', 'task_id', 'error_time'),
    )
