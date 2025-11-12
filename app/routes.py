# -*- coding: utf-8 -*-
import json
import logging
import traceback
from datetime import time, datetime, date
from functools import wraps
from urllib.parse import quote

from flask import Blueprint, jsonify, request, g
from flask_jwt_extended import jwt_required, current_user
from sqlalchemy import select, desc, inspect
from sqlalchemy.exc import IntegrityError, OperationalError, NoSuchTableError
from sqlalchemy.orm import joinedload

from app.config import Config
from app.database import test_db_connection, get_dynamic_engine
from app.jdy2db_services import Jdy2DbSyncService
from app.jdy_api import FormApi
from app.models import (JdyKeyInfo, SyncTask, SyncErrLog, FormFieldMapping, Department, Database, ConfigSession, User)
from app.scheduler import add_or_update_task_in_scheduler, remove_task_from_scheduler
from app.utils import log_sync_error, validate_signature

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

api_bp = Blueprint('api', __name__, url_prefix='/api')


# 辅助函数：将 SQLAlchemy 对象（包括关联对象）转换为字典
def row_to_dict(row, include_relations=None):
    """
    将 SQLAlchemy ORM 对象转换为字典，可选择性地包含已加载的关系。
    include_relations: 一个字典 e.g., {'department': ['department_name']}
    """
    d = {}
    if not row:
        return None

    # 确保 'password' (用户密码), 'db_password' (数据库密码), 'api_key' (简道云key), 和 'api_secret' (任务密钥) 不会被序列化
    excluded_fields = {'password', 'db_password', 'api_key', 'api_secret'}

    # 1. 转换主表字段
    for column in row.__table__.columns:

        # 不暴露密码哈希或任何敏感字段
        if column.name in excluded_fields:
            continue

        value = getattr(row, column.name)

        # # *** 统一处理 business_keys 和 incremental_fields (DB -> JSON) ***
        # if isinstance(row, SyncTask) and column.name in ('business_keys', 'incremental_fields'):
        #     if row.sync_type == 'db2jdy':
        #         if not value:
        #             d[column.name] = []  # 确保返回空数组
        #         else:
        #             try:
        #                 # 1. 尝试按新格式 (JSON 字符串) 解析: "[\"pk1\", \"pk2\"]"
        #                 parsed = json.loads(value)
        #                 # 确保是列表 (兼容旧的 "pk1" 字符串)
        #                 d[column.name] = parsed if isinstance(parsed, list) else [parsed]
        #             except (json.JSONDecodeError, TypeError):
        #                 # 2. 降级: 按旧格式 (逗号分隔) 解析: "pk1,pk2"
        #                 d[column.name] = [k for k in str(value).split(',') if k]
        #         continue  # 已处理，跳到下一个字段

        if isinstance(value, (datetime, date)):
            d[column.name] = value.isoformat()
        elif isinstance(value, time):
            # 将 time 对象格式化为 HH:MM:SS 字符串
            d[column.name] = value.strftime('%H:%M:%S')
        else:
            # value 可能是 str, int, bool, None,
            # 或来自 JSON 列的 list/dict
            d[column.name] = value

    # 2. (可选) 转换关联字段
    if include_relations:
        for relation_name, fields in include_relations.items():
            related_obj = getattr(row, relation_name, None)
            if related_obj:
                if isinstance(related_obj, list):
                    # (暂不支持一对多关系的序列化)
                    pass
                else:
                    # 处理一对一或多对一
                    for field in fields:
                        # e.g., d['department_name'] = related_obj.department_name
                        d[field] = getattr(related_obj, field, None)
    return d


# --- 超级管理员装饰器 ---
def superuser_required(fn):
    """
    一个装饰器，用于限制路由只能由超级管理员访问。
    """

    @jwt_required()
    @wraps(fn)  # 保持函数元信息
    def wrapper(*args, **kwargs):
        if not current_user or not current_user.is_superuser:
            return jsonify({"error": "权限不足：需要超级管理员权限"}), 403
        return fn(*args, **kwargs)

    return wrapper


# --- 1. 数据库管理 ---

@api_bp.route('/databases', methods=['GET'])
@jwt_required()
def get_databases():
    """
    获取数据库配置列表。
    支持按 sync_type 过滤。
    """
    session = g.config_session
    try:
        # 3.2 - 增加 sync_type 过滤器
        sync_type_filter = request.args.get('sync_type')

        query = select(Database).options(
            joinedload(Database.department)  # 预加载部门信息
        )

        # 非超级管理员只能看到自己部门的
        if not current_user.is_superuser:
            query = query.where(Database.department_id == current_user.department_id)

        # 3.2 - 应用 sync_type 过滤器
        if sync_type_filter in ['db2jdy', 'jdy2db']:
            query = query.where(Database.sync_type == sync_type_filter)

        databases = session.scalars(query.order_by(Database.department_id, Database.id)).all()

        # 序列化并包含 department_name
        result = [row_to_dict(db, include_relations={'department': ['department_name']}) for db in databases]
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting Databases: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "获取数据库配置失败"}), 500


@api_bp.route('/databases', methods=['POST'])
@jwt_required()
def add_database():
    """
    添加新的数据库配置。
    3.1 - 增加 sync_type 字段
    """
    data = request.get_json()
    session = g.config_session
    try:
        # 租户ID处理
        if current_user.is_superuser:
            department_id = data.get('department_id')
            if not department_id:
                return jsonify({"error": "超级管理员必须指定部门ID"}), 400
        else:
            department_id = current_user.department_id

        # 3.1 - 获取 sync_type
        sync_type = data.get('sync_type')
        if not sync_type:
            return jsonify({"error": "必须提供 sync_type (db2jdy 或 jdy2db)"}), 400

        new_db = Database(
            department_id=department_id,
            sync_type=sync_type,  # 保存 sync_type
            db_show_name=data.get('db_show_name'),
            db_type=data.get('db_type'),
            db_host=data.get('db_host'),
            db_port=data.get('db_port'),
            db_name=data.get('db_name'),
            db_args=data.get('db_args'),
            db_user=data.get('db_user'),
            db_password=data.get('db_password'),  # 密码在模型中处理
            is_active=data.get('is_active', True)
        )
        session.add(new_db)
        session.commit()

        # 返回包含部门名称的新对象
        session.refresh(new_db, ['department'])
        return jsonify(row_to_dict(new_db, include_relations={'department': ['department_name']})), 201

    except IntegrityError as e:
        session.rollback()
        if 'uq_dept_db_show_name' in str(e):
            return jsonify({"error": "该部门下的显示名称已存在"}), 409
        if 'uq_db_connection_info' in str(e):
            return jsonify({"error": "该数据库连接信息 (主机/端口/库/用户) 已被其他租户使用"}), 409
        return jsonify({"error": f"数据库完整性错误: {e}"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding Database: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "添加数据库配置失败"}), 500


@api_bp.route('/databases/<int:db_id>', methods=['PUT'])
@jwt_required()
def update_database(db_id):
    """
    更新数据库配置。
    3.1 - 增加 sync_type 字段
    """
    data = request.get_json()
    session = g.config_session
    try:
        # 1. 查找数据库
        query = select(Database).where(Database.id == db_id)
        if not current_user.is_superuser:
            query = query.where(Database.department_id == current_user.department_id)

        db_to_update = session.scalar(query)
        if not db_to_update:
            return jsonify({"error": "数据库不存在或权限不足"}), 404

        # 2. 租户ID处理
        if current_user.is_superuser:
            db_to_update.department_id = data.get('department_id', db_to_update.department_id)
        # (普通用户不能修改 department_id)

        # 3. 更新字段
        # 3.1 - 更新 sync_type
        db_to_update.sync_type = data.get('sync_type', db_to_update.sync_type)
        db_to_update.db_show_name = data.get('db_show_name', db_to_update.db_show_name)
        db_to_update.db_type = data.get('db_type', db_to_update.db_type)
        db_to_update.db_host = data.get('db_host', db_to_update.db_host)
        db_to_update.db_port = data.get('db_port', db_to_update.db_port)
        db_to_update.db_name = data.get('db_name', db_to_update.db_name)
        db_to_update.db_args = data.get('db_args', db_to_update.db_args)
        db_to_update.db_user = data.get('db_user', db_to_update.db_user)
        db_to_update.is_active = data.get('is_active', db_to_update.is_active)

        # 密码处理：仅在提供了新密码时更新
        if data.get('db_password'):
            db_to_update.db_password = data['db_password']

        session.commit()

        # 返回包含部门名称的更新后对象
        session.refresh(db_to_update, ['department'])
        return jsonify(row_to_dict(db_to_update, include_relations={'department': ['department_name']}))

    except IntegrityError:
        session.rollback()
        return jsonify({"error": "显示名称或连接信息已存在"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating Database {db_id}: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "更新数据库配置失败"}), 500


@api_bp.route('/databases/<int:db_id>', methods=['DELETE'])
@jwt_required()
def delete_database(db_id):
    """删除数据库配置"""
    session = g.config_session
    try:
        # 1. 查找数据库
        query = select(Database).where(Database.id == db_id)
        if not current_user.is_superuser:
            query = query.where(Database.department_id == current_user.department_id)

        db_to_delete = session.scalar(query)
        if not db_to_delete:
            return jsonify({"error": "数据库不存在或权限不足"}), 404

        # 2. 执行删除
        session.delete(db_to_delete)
        session.commit()
        return jsonify({"message": "数据库配置删除成功"})

    except IntegrityError:
        session.rollback()
        # 捕获外键约束（如果任务正在使用此数据库）
        return jsonify({"error": "无法删除：此数据库配置可能正被一个或多个同步任务使用。"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"Error deleting Database {db_id}: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "删除数据库配置失败"}), 500


@api_bp.route('/databases/test', methods=['POST'])
@jwt_required()
def test_database_connection():
    """测试数据库连接"""
    data = request.get_json()
    try:
        # 权限检查：确保用户只能测试他们有权访问的数据库
        # 1. 如果是编辑 (data 包含 id)
        if data.get('id'):
            query = select(Database).where(Database.id == data['id'])
            if not current_user.is_superuser:
                query = query.where(Database.department_id == current_user.department_id)
            db = g.config_session.scalar(query)
            if not db:
                return jsonify({"msg": "无权测试此数据库"}), 403
            # (如果密码未更改，从数据库加载)
            if not data.get('db_password'):
                data['db_password'] = db.db_password
        # # 2. 如果是新建 (data 不包含 id)
        # else:
        #     if not current_user.is_superuser:
        #         # 确保他们正在为自己的部门创建
        #         pass  # (暂时允许测试，因为 department_id 可能还未设置)
        #     pass

        # 执行测试
        success, message = test_db_connection(data)

        if success:
            return jsonify({"msg": message}), 200
        else:
            return jsonify({"msg": message}), 400  # 400 Bad Request 表示连接参数错误

    except Exception as e:
        return jsonify({"msg": f"测试失败: {e}"}), 500


# --- 2. 密钥管理 (JdyKeyInfo) ---

@api_bp.route('/jdy-keys', methods=['GET'])
@jwt_required()
def get_jdy_keys():
    """获取密钥列表"""
    session = g.config_session
    try:
        query = select(JdyKeyInfo).options(joinedload(JdyKeyInfo.department))
        if not current_user.is_superuser:
            query = query.where(JdyKeyInfo.department_id == current_user.department_id)

        keys = session.scalars(query.order_by(JdyKeyInfo.department_id)).all()
        result = [row_to_dict(key, include_relations={'department': ['department_name']}) for key in keys]
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "获取密钥列表失败"}), 500


@api_bp.route('/jdy-keys', methods=['POST'])
@jwt_required()
def add_jdy_key():
    """添加密钥"""
    data = request.get_json()
    session = g.config_session
    try:
        if current_user.is_superuser:
            department_id = data.get('department_id')
            if not department_id:
                return jsonify({"error": "超级管理员必须指定部门ID"}), 400
        else:
            department_id = current_user.department_id

        new_key = JdyKeyInfo(
            department_id=department_id,
            api_key=data.get('api_key'),
            # api_secret=data.get('api_secret'),
            description=data.get('description'),
        )
        session.add(new_key)
        session.commit()

        session.refresh(new_key, ['department'])
        return jsonify(row_to_dict(new_key, include_relations={'department': ['department_name']})), 201

    except IntegrityError:
        session.rollback()
        return jsonify({"error": "一个部门只能绑定一个密钥 (department_id 必须唯一)"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "添加密钥失败"}), 500


@api_bp.route('/jdy-keys/<int:key_id>', methods=['PUT'])
@jwt_required()
def update_jdy_key(key_id):
    """更新密钥"""
    data = request.get_json()
    session = g.config_session
    try:
        query = select(JdyKeyInfo).where(JdyKeyInfo.id == key_id)
        if not current_user.is_superuser:
            query = query.where(JdyKeyInfo.department_id == current_user.department_id)

        key_to_update = session.scalar(query)
        if not key_to_update:
            return jsonify({"error": "密钥不存在或权限不足"}), 404

        if current_user.is_superuser:
            key_to_update.department_id = data.get('department_id', key_to_update.department_id)

        key_to_update.api_key = data.get('api_key', key_to_update.api_key)
        # key_to_update.api_secret = data.get('api_secret', key_to_update.api_secret)
        key_to_update.description = data.get('description', key_to_update.description)

        session.commit()

        session.refresh(key_to_update, ['department'])
        return jsonify(row_to_dict(key_to_update, include_relations={'department': ['department_name']}))
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "部门ID冲突"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "更新密钥失败"}), 500


@api_bp.route('/jdy-keys/<int:key_id>', methods=['DELETE'])
@jwt_required()
def delete_jdy_key(key_id):
    """删除密钥"""
    session = g.config_session
    try:
        query = select(JdyKeyInfo).where(JdyKeyInfo.id == key_id)
        if not current_user.is_superuser:
            query = query.where(JdyKeyInfo.department_id == current_user.department_id)

        key_to_delete = session.scalar(query)
        if not key_to_delete:
            return jsonify({"error": "密钥不存在或权限不足"}), 404

        session.delete(key_to_delete)
        session.commit()
        return jsonify({"message": "密钥删除成功"})
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "无法删除：此密钥可能正被一个或多个同步任务的部门使用。"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"Error deleting JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "删除密钥失败"}), 500


# --- 3. 任务管理 (SyncTask) ---

def _parse_time_string(time_str: str | None) -> time | None:
    """辅助函数：从 ISO 字符串 (HH:MM:SS) 或 (YYYY-MM-DDTHH:MM:SS) 中解析时间对象"""
    if not time_str:
        return None
    try:
        # 兼容 HH:MM, HH:MM:SS, 或完整的 ISO datetime 字符串
        time_part = time_str.split('T')[-1].split('.')[0]
        return time.fromisoformat(time_part)
    except (ValueError, TypeError):
        return None


@api_bp.route('/sync-tasks', methods=['GET'])
@jwt_required()
def get_sync_tasks():
    """
    获取同步任务列表。
    3.2 - 增加 sync_type 过滤器
    """
    session = g.config_session
    try:
        # 3.2 - 获取 sync_type 参数
        sync_type_filter = request.args.get('sync_type')
        if not sync_type_filter:
            return jsonify({"error": "必须提供 数据同步类型 (简道云到数据库 或 数据库到简道云) 查询参数"}), 400

        query = select(SyncTask).options(
            joinedload(SyncTask.department),
            joinedload(SyncTask.database)
        ).where(SyncTask.sync_type == sync_type_filter)  # 严格过滤

        if not current_user.is_superuser:
            query = query.where(SyncTask.department_id == current_user.department_id)

        tasks = session.scalars(query.order_by(SyncTask.id)).all()

        # 序列化并包含 department_name 和 db_show_name
        result = [row_to_dict(task, include_relations={
            'department': ['department_name'],
            'database': ['db_show_name']
        }) for task in tasks]

        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting SyncTasks: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "获取同步任务列表失败"}), 500


@api_bp.route('/sync-tasks', methods=['POST'])
@jwt_required()
def add_sync_task():
    """
    添加新任务 (db2jdy 或 jdy2db)。
    3.2 - 处理 jdy2db 的 Webhook URL 生成
    """
    data = request.get_json()
    session = g.config_session
    try:
        # --- 1. 确定租户 ID ---
        if current_user.is_superuser:
            department_id = data.get('department_id')
            if not department_id:
                return jsonify({"error": "超级管理员必须指定部门ID"}), 400
            department = session.get(Department, department_id)
            if not department:
                return jsonify({"error": "指定的部门不存在"}), 404
        else:
            department_id = current_user.department_id
            department = current_user.department  # 已从 current_user 加载

        # --- 2. 确定数据库 ID ---
        database_id = data.get('database_id')
        if not database_id:
            return jsonify({"error": "必须提供 database_id"}), 400
        # 验证数据库是否属于该租户 (或超管权限)
        db_query = select(Database).where(Database.id == database_id)
        if not current_user.is_superuser:
            db_query = db_query.where(Database.department_id == department_id)
        source_db = session.scalar(db_query)
        if not source_db:
            return jsonify({"error": "数据库不存在或权限不足"}), 400

        # --- 3. 解析通用字段 ---
        sync_type = data.get('sync_type')
        if not sync_type:
            return jsonify({"error": "必须提供 sync_type (db2jdy 或 jdy2db)"}), 400

        # 确保 sync_type 匹配数据库的 sync_type
        if source_db.sync_type != sync_type:
            return jsonify({"error": f"任务类型 '{sync_type}' 与所选数据库的类型 '{source_db.sync_type}' 不匹配。"}), 400

        table_name = data.get('table_name')
        # table_name 可根据规则生成
        # if not table_name:
        #     return jsonify({"error": "必须提供表名"}), 400

        # --- 4. 初始化任务对象 ---
        new_task = SyncTask(
            task_name=data.get('task_name'),
            department_id=department_id,
            database_id=database_id,
            table_name=table_name,
            sync_type=sync_type,
            is_active=data.get('is_active', True),
            sync_status='idle'
        )

        # --- 5. 根据 sync_type 处理特定字段 ---
        if sync_type == 'db2jdy':
            # (db2jdy 专属字段)
            new_task.app_id = data.get('app_id')  # db2jdy 必须
            new_task.entry_id = data.get('entry_id')  # db2jdy 必须

            # # *** 统一处理 business_keys (JSON -> DB) ***
            # raw_keys = data.get('business_keys')  # 期望是 ['pk1', 'pk2']
            # if isinstance(raw_keys, list):
            #     new_task.business_keys = json.dumps(raw_keys)
            # else:
            #     new_task.business_keys = raw_keys  # 降级
            #
            # # *** 修复: 统一处理 incremental_fields (JSON -> DB) ***
            # raw_inc_field = data.get('incremental_fields')  # 期望是 ['field1']
            # if isinstance(raw_inc_field, list):
            #     new_task.incremental_fields = json.dumps(raw_inc_field)
            # else:
            #     new_task.incremental_fields = raw_inc_field  # 降级

            # *** 移除 json.dumps, 直接赋值数组 ***
            new_task.business_keys = data.get('business_keys')  # 期望是 ['pk1', 'pk2']
            new_task.incremental_fields = data.get('incremental_fields')  # 期望是 ['field1']

            new_task.sync_mode = data.get('sync_mode', 'INCREMENTAL')
            new_task.incremental_interval = data.get('incremental_interval')
            new_task.full_sync_time = _parse_time_string(data.get('full_sync_time'))
            new_task.source_filter_sql = data.get('source_filter_sql')

        elif sync_type == 'jdy2db':
            # (jdy2db 专属字段)
            # app_id 和 entry_id 在 jdy2db 中是 nullable=True, 由 webhook 自动填充
            new_task.app_id = data.get('app_id')  # 允许前端传入
            new_task.entry_id = data.get('entry_id')  # 允许前端传入
            new_task.daily_sync_time = _parse_time_string(data.get('daily_sync_time'))
            new_task.daily_sync_type = data.get('daily_sync_type', 'ONCE')
            new_task.json_as_string = data.get('json_as_string', False)
            new_task.label_to_pinyin = data.get('label_to_pinyin', False)
            new_task.api_secret = data.get('api_secret')

            # 第一次commit时没有生成task_id
            # # 3.2 - 动态生成 Webhook URL
            # # 格式: http://<host>/api/jdy/webhook?dpt=<dept_name>&db_id=<db_id>&table=<table_name>
            # # 使用 db_id (数字) 而不是 db_show_name (可能变化)
            # # 使用 dpt (部门简称)
            # host_url = Config.WEB_HOOK_BASE_URL or request.host_url
            # if host_url.endswith('/'):
            #     host_url = host_url.rstrip('/')
            # query_params = f"dpt={quote(department.department_name)}&db_id={database_id}&task_id={new_task.id}&table={quote(table_name)}"
            # new_task.webhook_url = f"{host_url}/api/jdy/webhook?{query_params}"

        # --- 6. 通用通知字段 ---
        new_task.is_full_sync_first = data.get('is_full_sync_first', False)
        new_task.is_delete_first = data.get('is_delete_first', False)
        new_task.send_error_log_to_wecom = data.get('send_error_log_to_wecom', False)
        new_task.wecom_robot_webhook_url = data.get('wecom_robot_webhook_url')

        session.add(new_task)
        session.commit()  # 提交以获取 task_id

        # 3.2 - 动态生成 Webhook URL (在第一次 commit 之后)
        # 此时 new_task.id 已经有值
        if new_task.sync_type == 'jdy2db':
            host_url = Config.WEB_HOOK_BASE_URL or request.host_url
            if host_url.endswith('/'):
                host_url = host_url.rstrip('/')

            # 使用 new_task.id (现在已填充)
            query_params = f"dpt={quote(department.department_name)}&db_id={database_id}&task_id={new_task.id}&table={quote(table_name)}"
            new_task.webhook_url = f"{host_url}/api/jdy/webhook?{query_params}"

            # 再次提交以保存 webhook_url
            session.commit()

        # --- 7. 通知调度器 ---
        # 重新查询更新后的任务 (包含 department 和 database)
        final_task = session.query(SyncTask).options(
            joinedload(SyncTask.department),
            joinedload(SyncTask.database)
        ).get(new_task.id)

        if final_task:
            add_or_update_task_in_scheduler(final_task)

        return jsonify(row_to_dict(final_task, include_relations={
            'department': ['department_name'],
            'database': ['db_show_name']
        })), 201

    except IntegrityError as e:
        session.rollback()
        # (models.py 中有一个 uq_dept_db_table_synctype 约束被注释掉了，如果启用，这里需要处理)
        return jsonify({"error": f"数据库完整性错误: {e}"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding SyncTask: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"添加同步任务失败: {e}"}), 500


@api_bp.route('/sync-tasks/<int:task_id>', methods=['PUT'])
@jwt_required()
def update_sync_task(task_id):
    """
    更新任务 (db2jdy 或 jdy2db)。
    3.2 - 更新 jdy2db 的 Webhook URL
    """
    data = request.get_json()
    session = g.config_session
    try:
        # --- 1. 查找任务 ---
        query = select(SyncTask).where(SyncTask.id == task_id)
        if not current_user.is_superuser:
            query = query.where(SyncTask.department_id == current_user.department_id)

        task_to_update = session.scalar(query)
        if not task_to_update:
            return jsonify({"error": "同步任务不存在或权限不足"}), 404

        # --- 2. 确定租户 ID 和对象 ---
        department_id = task_to_update.department_id
        if current_user.is_superuser:
            department_id = data.get('department_id', task_to_update.department_id)

        department = session.get(Department, department_id)
        if not department:
            return jsonify({"error": "部门不存在"}), 404

        task_to_update.department_id = department_id

        # --- 3. 确定数据库 ID ---
        database_id = data.get('database_id', task_to_update.database_id)
        # 验证数据库
        db_query = select(Database).where(Database.id == database_id)
        if not current_user.is_superuser:
            db_query = db_query.where(Database.department_id == department_id)
        source_db = session.scalar(db_query)
        if not source_db:
            return jsonify({"error": "数据库不存在或该部门权限不足"}), 400

        # (不允许修改 sync_type)
        sync_type = task_to_update.sync_type

        # 确保 sync_type 匹配数据库的 sync_type
        if source_db.sync_type != sync_type:
            return jsonify({"error": f"任务类型 '{sync_type}' 与所选数据库的类型 '{source_db.sync_type}' 不匹配。"}), 400

        task_to_update.database_id = database_id

        # --- 4. 更新通用字段 ---
        table_name = data.get('table_name', task_to_update.table_name)  # 获取新或旧的 table_name
        task_to_update.table_name = table_name
        task_to_update.task_name = data.get('task_name', task_to_update.task_name)
        task_to_update.is_active = data.get('is_active', task_to_update.is_active)

        # --- 5. 根据 sync_type 更新特定字段 ---
        if sync_type == 'db2jdy':
            task_to_update.app_id = data.get('app_id', task_to_update.app_id)
            task_to_update.entry_id = data.get('entry_id', task_to_update.entry_id)

            # # *** 统一处理 business_keys (JSON -> DB) ***
            # if 'business_keys' in data:
            #     raw_keys = data.get('business_keys')
            #     if isinstance(raw_keys, list):
            #         task_to_update.business_keys = json.dumps(raw_keys)
            #     else:
            #         task_to_update.business_keys = raw_keys  # 降级
            #
            # # *** 统一处理 incremental_fields (JSON -> DB) ***
            # if 'incremental_fields' in data:
            #     raw_inc_field = data.get('incremental_fields')
            #     if isinstance(raw_inc_field, list):
            #         task_to_update.incremental_fields = json.dumps(raw_inc_field)
            #     else:
            #         task_to_update.incremental_fields = raw_inc_field  # 降级

            # # *** 移除 json.dumps, 直接赋值数组 ***
            # if 'business_keys' in data:
            #     task_to_update.business_keys = data.get('business_keys')  # 期望是 ['pk1']
            #
            # if 'incremental_fields' in data:
            #     task_to_update.incremental_fields = data.get('incremental_fields')  # 期望是 ['field1']

            task_to_update.business_keys = data.get('business_keys', task_to_update.business_keys)
            task_to_update.sync_mode = data.get('sync_mode', task_to_update.sync_mode)
            task_to_update.incremental_fields = data.get('incremental_fields', task_to_update.incremental_fields)
            task_to_update.incremental_interval = data.get('incremental_interval', task_to_update.incremental_interval)
            # 修正 _parse_time_string 对 null 的处理
            full_sync_time_val = data.get('full_sync_time', task_to_update.full_sync_time)
            task_to_update.full_sync_time = _parse_time_string(full_sync_time_val) if full_sync_time_val else None

            task_to_update.source_filter_sql = data.get('source_filter_sql', task_to_update.source_filter_sql)

        elif sync_type == 'jdy2db':
            # app_id 和 entry_id 由 webhook 自动填充，但如果前端传了（例如手动指定），也允许
            if data.get('app_id') is not None:
                task_to_update.app_id = data.get('app_id')
            if data.get('entry_id') is not None:
                task_to_update.entry_id = data.get('entry_id')

            # 更新 api_secret
            if 'api_secret' in data:
                task_to_update.api_secret = data.get('api_secret')

            # 修正 _parse_time_string 对 null 的处理
            daily_sync_time_val = data.get('daily_sync_time', task_to_update.daily_sync_time)
            task_to_update.daily_sync_time = _parse_time_string(daily_sync_time_val) if daily_sync_time_val else None

            task_to_update.daily_sync_type = data.get('daily_sync_type', task_to_update.daily_sync_type)
            task_to_update.json_as_string = data.get('json_as_string', task_to_update.json_as_string)
            task_to_update.label_to_pinyin = data.get('label_to_pinyin', task_to_update.label_to_pinyin)

            # 3.2 - 重新生成 Webhook URL
            host_url = Config.WEB_HOOK_BASE_URL or request.host_url
            if host_url.endswith('/'):
                host_url = host_url.rstrip('/')
            query_params = f"dpt={quote(department.department_name)}&db_id={database_id}&task_id={task_id}&table={quote(table_name)}"
            task_to_update.webhook_url = f"{host_url}/api/jdy/webhook?{query_params}"

        # --- 6. 通用通知字段 ---
        task_to_update.is_full_sync_first = data.get('is_full_sync_first', task_to_update.is_full_sync_first)
        task_to_update.is_delete_first = data.get('is_delete_first', task_to_update.is_delete_first)
        task_to_update.send_error_log_to_wecom = data.get('send_error_log_to_wecom',
                                                          task_to_update.send_error_log_to_wecom)
        task_to_update.wecom_robot_webhook_url = data.get('wecom_robot_webhook_url',
                                                          task_to_update.wecom_robot_webhook_url)

        session.commit()

        # --- 7. 通知调度器 ---
        # 重新查询更新后的任务 (包含 department 和 database)
        updated_task = session.query(SyncTask).options(
            joinedload(SyncTask.department),
            joinedload(SyncTask.database)
        ).get(task_id)

        if updated_task:
            add_or_update_task_in_scheduler(updated_task)

        return jsonify(row_to_dict(updated_task, include_relations={
            'department': ['department_name'],
            'database': ['db_show_name']
        }))

    except IntegrityError as e:
        session.rollback()
        return jsonify({"error": f"数据库完整性错误: {e}"}), 409
    except Exception as e:
        session.rollback()
        logger.error(f"task_id:[{task_id}] Error updating SyncTask: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"更新同步任务失败: {e}"}), 500


@api_bp.route('/sync-tasks/<int:task_id>', methods=['DELETE'])
@jwt_required()
def delete_sync_task(task_id):
    """删除任务"""
    session = g.config_session
    try:
        # 1. 查找任务
        query = select(SyncTask).where(SyncTask.id == task_id)
        if not current_user.is_superuser:
            query = query.where(SyncTask.department_id == current_user.department_id)

        task_to_delete = session.scalar(query)
        if not task_to_delete:
            return jsonify({"error": "同步任务不存在或权限不足"}), 404

        # 2. 通知调度器 (在删除数据库记录之前)
        remove_task_from_scheduler(task_id)

        # 3. 执行删除
        session.delete(task_to_delete)
        session.commit()

        return jsonify({"message": "同步任务删除成功"})
    except Exception as e:
        session.rollback()
        logger.error(f"task_id:[{task_id}] Error deleting SyncTask: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "删除同步任务失败"}), 500


# --- 4. 数据库检查 (DB Inspector) ---

def _get_db_or_404(db_id, session):
    """辅助函数：检查用户是否有权访问 db_id"""
    query = select(Database).where(Database.id == db_id, Database.sync_type == 'db2jdy')
    if not current_user.is_superuser:
        query = query.where(Database.department_id == current_user.department_id, Database.sync_type == 'db2jdy')

    db_config = session.scalar(query)
    if not db_config:
        raise PermissionError("数据库不存在或权限不足")
    return db_config


@api_bp.route('/database/inspector/<int:db_id>/tables', methods=['GET'])
@jwt_required()
def get_db_tables(db_id):
    """
    获取指定数据库的所有表名和视图名
    """
    session = g.config_session
    try:
        # 1. 权限检查
        db_config = _get_db_or_404(db_id, session)

        # 2. 创建一个临时的 SyncTask 对象以传递给 get_dynamic_engine
        #    get_dynamic_engine 会使用 db_config (task.database)
        dummy_task = SyncTask(database_id=db_config.id)
        dummy_task.database = db_config

        # 3. 获取动态引擎并检查
        engine = get_dynamic_engine(dummy_task)
        inspector = inspect(engine)

        table_names = inspector.get_table_names()
        view_names = inspector.get_view_names()

        all_names = sorted(list(set(table_names + view_names)))

        return jsonify({"tables": all_names})

    except PermissionError as e:
        return jsonify({"error": str(e)}), 403
    except OperationalError as e:
        logger.error(f"[Inspector] 数据库连接失败 (DB_ID: {db_id}): {e}")
        return jsonify({"error": f"数据库连接失败: {e.orig}"}), 500
    except Exception as e:
        logger.error(f"[Inspector] 获取表列表失败 (DB_ID: {db_id}): {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"检查数据库失败: {e}"}), 500


@api_bp.route('/database/inspector/<int:db_id>/<string:table_name>/schema', methods=['GET'])
@jwt_required()
def get_table_schema(db_id, table_name):
    """
    获取指定表的所有列信息
    """
    session = g.config_session
    try:
        # 1. 权限检查
        db_config = _get_db_or_404(db_id, session)

        # 2. 创建临时任务
        dummy_task = SyncTask(database_id=db_config.id)
        dummy_task.database = db_config

        # 3. 获取动态引擎并检查
        engine = get_dynamic_engine(dummy_task)
        inspector = inspect(engine)

        columns = inspector.get_columns(table_name)

        schema_list = [
            {"name": col["name"], "type": str(col["type"])}
            for col in columns
        ]

        return jsonify({"schema": schema_list})

    except PermissionError as e:
        return jsonify({"error": str(e)}), 403
    except NoSuchTableError:
        logger.warning(f"[Inspector] 表不存在 (DB_ID: {db_id}, Table: {table_name})")
        return jsonify({"error": f"表 '{table_name}' 不存在"}), 404
    except OperationalError as e:
        logger.error(f"[Inspector] 数据库连接失败 (DB_ID: {db_id}): {e}")
        return jsonify({"error": f"数据库连接失败: {e.orig}"}), 500
    except Exception as e:
        logger.error(f"[Inspector] 获取表结构失败 (DB_ID: {db_id}, Table: {table_name}): {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"检查表结构失败: {e}"}), 500


# --- 5. 日志管理 (SyncErrLog) ---

@api_bp.route('/sync-logs', methods=['GET'])
@jwt_required()
def get_sync_logs():
    """获取错误日志"""
    session = g.config_session
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        sync_type_filter = request.args.get('sync_type')  # 前端将使用此参数

        query = select(SyncErrLog)

        # 权限过滤
        if not current_user.is_superuser:
            query = query.where(SyncErrLog.department_id == current_user.department_id)

        # 过滤器
        if sync_type_filter in ['db2jdy', 'jdy2db']:
            query = query.where(SyncErrLog.sync_type == sync_type_filter)
        else:
            return jsonify({"error": "必须提供 sync_type (db2jdy 或 jdy2db) 查询参数"}), 400

        logs = session.scalars(
            query
            .order_by(desc(SyncErrLog.timestamp))
            .limit(limit)
            .offset(offset)
        ).all()
        return jsonify([row_to_dict(log) for log in logs])
    except Exception as e:
        logger.error(f"Error getting SyncErrLogs: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "获取同步错误日志失败"}), 500


# --- 6. 字段映射 (FormFieldMapping) ---

@api_bp.route('/field-mappings', methods=['GET'])
@jwt_required()
def get_field_mappings():
    """获取字段映射 (按 task_id)"""
    session = g.config_session
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({"error": "task_id 为必填项"}), 400

        query = select(FormFieldMapping).where(FormFieldMapping.task_id == task_id)

        # 权限检查：确保用户有权访问此 task_id
        if not current_user.is_superuser:
            task_owner_id = session.scalar(select(SyncTask.department_id).where(SyncTask.id == task_id))
            if task_owner_id != current_user.department_id:
                return jsonify({"error": "无权访问此任务"}), 403

        mappings = session.scalars(query).all()
        return jsonify([row_to_dict(m) for m in mappings])
    except Exception as e:
        logger.error(f"Error getting FormFieldMappings: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "获取字段映射失败"}), 500


# --- 7. 简道云 Webhook 接收端点 ---

@api_bp.route('/jdy/webhook', methods=['POST'])
def handle_jdy_webhook():
    """
    接收简道云数据推送 (jdy2db)。
    URL 格式: /api/jdy/webhook?dpt=<dept_name>&db_id=<db_id>&task_id=<task_id>&table=<table_name>&nonce=...&timestamp=...
    """

    # --- 1. 解析 URL 参数 ---
    task_id_str = request.args.get('task_id')
    dpt_name = request.args.get('dpt')
    db_id_str = request.args.get('db_id')
    table_name = request.args.get('table')

    # 来自 ?table= 的 "" 正确匹配数据库中的 NULL
    if table_name and table_name.strip() == "":
        table_name = None

    # --- [鉴权] 获取签名参数 ---
    nonce = request.args.get('nonce')
    timestamp = request.args.get('timestamp')
    signature_from_header = request.headers.get('X-JDY-Signature')

    # table_name 可不传
    # if not all([dpt_name, db_id_str, table_name]):
    #     logger.error(f"[Webhook] 400: URL 参数不完整 (dpt, db_id, table)")
    #     return jsonify({"error": "Webhook URL 已失效: URL (dpt, db_id, table)"}), 410  # 410 GONE 表示配置已失效
    if not all([dpt_name, db_id_str, task_id_str]):
        logger.error(f"[Webhook] 400: Incomplete URL parameters (dpt, db_id, task_id)")
        return jsonify({"error": "Webhook URL 已失效: URL (dpt, db_id, task_id)"}), 410  # 410 GONE 表示配置已失效

    try:
        task_id = int(task_id_str)
        db_id = int(db_id_str)
    except ValueError:
        logger.error(f"[Webhook] 400: task_id or db_id must be an integer")
        return jsonify({"error": "Webhook URL 已失效: task_id 或 db_id 必须是整数"}), 410

    # --- 2. [鉴权] 获取 Webhook 原始负载 ---
    try:
        raw_payload_str = request.get_data(as_text=True)
        if not raw_payload_str:
            logger.error(f"[Webhook] 400: Payload is empty")
            return jsonify({"error": "负载为空"}), 400
    except Exception as e:
        logger.error(f"[Webhook] 400: Unable to read request body: {e}")
        return jsonify({"error": "读取请求体失败"}), 400

    # 使用独立的 ConfigSession 处理请求
    session = ConfigSession()
    task_config = None  # 初始化 task_config

    try:
        # --- 步骤 3. 查找任务配置 (SyncTask) ---
        # 必须先找到任务，才能获取该任务的 api_secret
        task_config = session.scalar(
            select(SyncTask)
            .where(
                SyncTask.id == task_id,
                SyncTask.sync_type == 'jdy2db',
                SyncTask.database_id == db_id,
                SyncTask.is_active == True
            )
            # 预加载所需的关系
            .options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                joinedload(SyncTask.database)
            )
        )

        if not task_config:
            logger.error(
                f"[Webhook] 404 No active jdy2db task found (TaskID: {task_id}, DB_ID: {db_id})")
            log_sync_error(
                task_config=None,  # 没有 task 对象
                error=Exception("Webhook 404"),
                payload={"raw_payload": raw_payload_str[:500]},
                extra_info=f"找不到激活的 jdy2db 任务 (TaskID: {task_id}, DB_ID: {db_id})"
            )
            return jsonify({"error": "Webhook 已失效: 未找到匹配的激活任务"}), 410

        # --- 4. [鉴权] 获取 Webhook 原始负载 ---
        try:
            raw_payload_str = request.get_data(as_text=True)
            if not raw_payload_str:
                logger.error(f"[Webhook] 400: Payload is empty (Task ID: {task_id})")
                return jsonify({"error": "负载为空"}), 400
        except Exception as e:
            logger.error(f"[Webhook] 400: Unable to read request body (Task ID: {task_id}): {e}")
            return jsonify({"error": "读取请求体失败"}), 400

        # --- 5. [鉴权] 验证签名 ---
        api_secret = task_config.api_secret

        # 仅在配置了 api_secret 时才执行验证
        if api_secret:
            if not all([nonce, timestamp, signature_from_header]):
                logger.error(
                    f"[Webhook Auth] 400: Request rejected (Task ID: {task_id}). Secret is configured but request lacks signature parameters (nonce/timestamp/header).")
                return jsonify({"error": "缺少签名参数"}), 400

            is_valid = validate_signature(
                nonce=nonce,
                payload_str=raw_payload_str,
                secret=task_config.api_secret,
                timestamp=timestamp,
                signature_from_header=signature_from_header
            )

            if not is_valid:
                logger.error(f"[Webhook Auth] 403: Invalid signature (Dept: {dpt_name}, TaskID: {task_id}).")
                # 记录日志 (但不使用 task_config)
                log_sync_error(
                    task_config=task_config,
                    error=Exception("无效签名"),
                    payload={"raw_payload": raw_payload_str[:500]},  # 避免 payload 过大
                    extra_info=f"Webhook 签名验证失败 (Dept: {dpt_name}, TaskID: {task_id}, DB: {db_id})"
                )
                return jsonify({"error": "签名无效"}), 403

        # (鉴权通过或未配置Task Secret)

        # --- 5. 解析 JSON 负载 ---
        try:
            payload = json.loads(raw_payload_str)
        except json.JSONDecodeError:
            logger.error(f"[Webhook] 400: Payload is not valid JSON (Task ID: {task_id}).")
            return jsonify({"error": "无效的 JSON 负载"}), 400

        if not payload or not payload.get('data') or not payload.get('op'):
            logger.error(f"[Webhook] 400: Invalid payload structure (op/data) (Task ID: {task_id})")
            return jsonify({"error": "负载结构无效"}), 400

        op = payload.get('op')
        data = payload.get('data')
        app_id = data.get('appId') or payload.get('app_id')
        entry_id = data.get('entryId') or payload.get('entry_id')

        # --- 7. 查找 JdyKeyInfo (用于 API Key) ---
        key_info = task_config.department.jdy_key_info
        if not key_info or not key_info.api_key:
            logger.error(f"[Webhook] 404: No JdyKeyInfo (api_key) found for department (Task ID: {task_id})")
            log_sync_error(
                task_config=task_config,
                error=Exception("部门 API Key 未配置"),
                payload=payload,
                extra_info="Webhook 处理器: 找不到部门的 JdyKeyInfo (api_key)"
            )
            return jsonify({"error": "Webhook 已失效: 部门 API Key 未配置"}), 410

        # --- 8. 实例化 API 客户端 (用于自动创建映射) ---
        api_client = FormApi(
            api_key=key_info.api_key,
            host=Config.JDY_API_BASE_URL
        )

        # --- 9. 调用核心服务处理数据 ---
        sync_service = Jdy2DbSyncService()
        sync_service.handle_webhook_data(
            config_session=session,  # 传入当前会话用于更新任务状态
            payload=payload,
            task_config=task_config,
            api_client=api_client,
            table_param=table_name
        )

        # 根据开发指南，返回 2xx 状态码
        return jsonify({"code": 0, "msg": "成功"}), 200

    except Exception as e:
        session.rollback()
        # 尝试从 payload 中获取 app_id 和 entry_id (如果解析成功)
        app_id_log = locals().get('payload', {}).get('app_id', 'N/A')
        entry_id_log = locals().get('payload', {}).get('entry_id', 'N/A')

        logger.error(
            f"[Webhook] 500: (Task ID: {task_id}, App: {app_id_log}, Entry: {entry_id_log}) processing failed: {e}\n{traceback.format_exc()}")
        # 尝试记录错误
        log_sync_error(
            task_config=task_config,  # task_config 可能为 None，但 log_sync_error 已处理
            error=e,
            payload=locals().get('payload', {"raw_payload": locals().get('raw_payload_str', 'N/A')}),
            extra_info="Webhook 处理器发生意外错误"
        )
        # 即使发生错误，也可能需要返回 2xx 以避免简道云重试
        return jsonify({"error": "服务器内部错误"}), 500
        # # 根据开发指南，"直接响应成功，不要响应为失败"
        # # 即使发生错误，也返回 2xx 状态码
        # return jsonify({"code": 0, "msg": "成功 (内部错误已记录)"}), 200
    finally:
        session.close()


# --- 8. 部门管理 (Department) ---

@api_bp.route('/departments', methods=['GET'])
@superuser_required
def get_departments():
    """获取所有部门列表 (仅超级管理员)"""
    session = g.config_session
    try:
        query = select(Department).order_by(Department.id)
        departments = session.scalars(query).all()
        return jsonify([row_to_dict(dept) for dept in departments])
    except Exception as e:
        logger.error(f"Error getting Departments: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "获取部门列表失败"}), 500


@api_bp.route('/departments', methods=['POST'])
@superuser_required
def add_department():
    """添加新部门 (仅超级管理员)"""
    data = request.get_json()
    session = g.config_session
    if not data or not data.get('department_name'):
        return jsonify({"error": "department_name 为必填项"}), 400

    try:
        new_dept = Department(
            department_name=data.get('department_name'),
            department_full_name=data.get('department_full_name'),
            is_active=data.get('is_active', True)
        )
        session.add(new_dept)
        session.commit()
        return jsonify(row_to_dict(new_dept)), 201
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "部门名称已存在"}), 409
    except Exception as e:
        session.rollback()
        return jsonify({"error": f"添加部门失败: {e}"}), 500


@api_bp.route('/departments/<int:dept_id>', methods=['PUT'])
@superuser_required
def update_department(dept_id):
    """更新部门信息 (仅超级管理员)"""
    data = request.get_json()
    session = g.config_session
    try:
        dept = session.get(Department, dept_id)
        if not dept:
            return jsonify({"error": "部门不存在"}), 404

        dept.department_name = data.get('department_name', dept.department_name)
        dept.department_full_name = data.get('department_full_name', dept.department_full_name)
        dept.is_active = data.get('is_active', dept.is_active)

        session.commit()
        return jsonify(row_to_dict(dept))
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "部门名称已存在"}), 409
    except Exception as e:
        session.rollback()
        return jsonify({"error": f"更新部门失败: {e}"}), 500


@api_bp.route('/departments/<int:dept_id>', methods=['DELETE'])
@superuser_required
def delete_department(dept_id):
    """删除部门 (仅超级管理员)"""
    session = g.config_session
    try:
        dept = session.get(Department, dept_id)
        if not dept:
            return jsonify({"error": "部门不存在"}), 404

        # 检查是否有用户关联
        user_count = session.query(User).filter(User.department_id == dept_id).count()
        if user_count > 0:
            return jsonify({"error": "无法删除部门：仍有用户与此部门关联"}), 409

        session.delete(dept)
        session.commit()
        return jsonify({"message": "部门删除成功"})
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "无法删除：部门正在使用中（例如，被任务或密钥使用）"}), 409
    except Exception as e:
        session.rollback()
        return jsonify({"error": f"删除部门失败: {e}"}), 500


# --- 9. 用户管理 (User) ---

@api_bp.route('/users', methods=['GET'])
@jwt_required()
def get_users():
    """
    获取用户列表。
    超级管理员获取所有用户；普通用户仅获取自己。
    """
    session = g.config_session
    try:
        query = select(User).options(joinedload(User.department))

        if not current_user.is_superuser:
            query = query.where(User.id == current_user.id)

        users = session.scalars(query.order_by(User.id)).all()

        # 使用本文件中的 row_to_dict 辅助函数
        result = [row_to_dict(user, include_relations={'department': ['department_name']}) for user in users]
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting Users: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "获取用户列表失败"}), 500


@api_bp.route('/users', methods=['POST'])
@superuser_required
def add_user():
    """添加新用户 (仅超级管理员)"""
    data = request.get_json()
    session = g.config_session

    if not data or not data.get('username') or not data.get('password') or not data.get('department_id'):
        return jsonify({"error": "用户名、密码和 部门ID 为必填项"}), 400

    try:
        # 检查部门是否存在
        dept = session.get(Department, data.get('department_id'))
        if not dept:
            return jsonify({"error": "指定的 部门ID 不存在"}), 404

        new_user = User(
            username=data.get('username'),
            department_id=data.get('department_id'),
            is_superuser=data.get('is_superuser', False),
            is_active=data.get('is_active', True)
        )
        new_user.set_password(data.get('password'))  # 设置密码

        session.add(new_user)
        session.commit()

        session.refresh(new_user, ['department'])
        return jsonify(row_to_dict(new_user, include_relations={'department': ['department_name']})), 201

    except IntegrityError:
        session.rollback()
        return jsonify({"error": "用户名已存在"}), 409
    except Exception as e:
        session.rollback()
        return jsonify({"error": f"添加用户失败: {e}"}), 500


@api_bp.route('/users/<int:user_id>', methods=['PUT'])
@superuser_required
def update_user(user_id):
    """更新用户信息 (仅超级管理员)"""
    data = request.get_json()
    session = g.config_session
    try:
        user_to_update = session.get(User, user_id)
        if not user_to_update:
            return jsonify({"error": "用户不存在"}), 404

        # 检查部门
        if 'department_id' in data:
            dept = session.get(Department, data['department_id'])
            if not dept:
                return jsonify({"error": "指定的 department_id 不存在"}), 404
            user_to_update.department_id = data['department_id']

        user_to_update.username = data.get('username', user_to_update.username)
        user_to_update.is_superuser = data.get('is_superuser', user_to_update.is_superuser)
        user_to_update.is_active = data.get('is_active', user_to_update.is_active)

        session.commit()

        session.refresh(user_to_update, ['department'])
        return jsonify(row_to_dict(user_to_update, include_relations={'department': ['department_name']}))

    except IntegrityError:
        session.rollback()
        return jsonify({"error": "用户名已存在"}), 409
    except Exception as e:
        session.rollback()
        return jsonify({"error": f"更新用户失败: {e}"}), 500


@api_bp.route('/users/<int:user_id>', methods=['DELETE'])
@superuser_required
def delete_user(user_id):
    """删除用户 (仅超级管理员)"""
    session = g.config_session

    if user_id == current_user.id:
        return jsonify({"error": "不能删除自己"}), 400

    try:
        user_to_delete = session.get(User, user_id)
        if not user_to_delete:
            return jsonify({"error": "用户不存在"}), 404

        session.delete(user_to_delete)
        session.commit()
        return jsonify({"message": "用户删除成功"})
    except Exception as e:
        session.rollback()
        return jsonify({"error": f"删除用户失败: {e}"}), 500


# --- 10. 重置密码路由 ---

@api_bp.route('/users/<int:user_id>/reset-password', methods=['PATCH'])
@jwt_required()
def reset_user_password(user_id):
    """
    重置用户密码
    """

    # 允许超级管理员, 或用户自己操作 (user_id 匹配 current_user.id)
    if not current_user.is_superuser and user_id != current_user.id:
        return jsonify({"error": "权限不足：只能重置自己的密码。"}), 403

    data = request.get_json()
    old_password = data.get('old_password')
    new_password = data.get('new_password')

    if not old_password:
        return jsonify({"error": "旧密码 为必填项"}), 400

    if not new_password:
        return jsonify({"error": "新密码 为必填项"}), 400

    session = g.config_session
    try:
        user_to_update = session.get(User, user_id)
        if not user_to_update:
            return jsonify({"error": "用户不存在"}), 404

        # 额外的权限检查
        if not current_user.is_superuser and user_to_update.id != current_user.id:
            return jsonify({"error": "权限不足。"}), 403

        # 检查旧密码
        if not user_to_update.check_password(old_password):
            return jsonify({"error": "旧密码错误。"}), 400

        user_to_update.set_password(new_password)
        session.commit()

        return jsonify({"message": f"用户 {user_to_update.username} 的密码已重置成功"})

    except Exception as e:
        session.rollback()
        logger.error(f"Error resetting password for user {user_id}: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "重置密码失败"}), 500
