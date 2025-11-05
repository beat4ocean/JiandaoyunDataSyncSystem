# -*- coding: utf-8 -*-
import traceback
from datetime import time, datetime

from flask import Blueprint, jsonify, request, g, current_app
from flask_jwt_extended import jwt_required, get_current_user
from sqlalchemy import select, update, delete, desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload

from app.models import JdyKeyInfo, SyncTask, SyncErrLog, FormFieldMapping, Department, Database
from app.scheduler import add_or_update_task_in_scheduler, remove_task_from_scheduler
from app import jdy2db_services
from app.config import Config
from app.utils import test_db_connection

api_bp = Blueprint('api', __name__, url_prefix='/api')


# Helper to convert SQLAlchemy objects to dict
def row_to_dict(row):
    d = {}
    if not row:
        return None
    for column in row.__table__.columns:
        value = getattr(row, column.name)
        if isinstance(value, (datetime, time)):
            d[column.name] = value.isoformat()
        else:
            d[column.name] = value
    return d


# [REFACTOR] 新增辅助函数，用于返回包含关联名称的任务
def task_to_dict(task: SyncTask):
    """将 SyncTask 对象（已 eager load）转换为字典"""
    d = row_to_dict(task)
    if not d:
        return None

    # 添加关联数据
    if task.department:
        d['department_name'] = task.department.department_name
    if task.database:
        d['db_show_name'] = task.database.db_show_name

    # (重要) 确保 Jdy2Db 任务返回 webhook_url
    if task.sync_type == 'jdy2db' and not task.webhook_url:
        # 如果 URL 为空（例如旧数据），尝试动态生成
        try:
            d['webhook_url'] = (
                f"{Config.BASE_URL}/api/jdy/webhook?"
                f"dpt={task.department.department_name}&"
                f"db={task.database.db_show_name}&"
                f"table={task.table_name}"
            )
        except Exception:
            d['webhook_url'] = "Error: Missing department or db link."

    return d


# --- JdyKeyInfo CRUD (密钥管理) ---

@api_bp.route('/jdy-keys', methods=['GET'])
@jwt_required()
def get_jdy_keys():
    session = g.config_session
    user = get_current_user()
    try:
        query = select(JdyKeyInfo).options(joinedload(JdyKeyInfo.department))
        if not user.is_superuser:
            query = query.where(JdyKeyInfo.department_id == user.department_id)

        keys = session.scalars(query.order_by(JdyKeyInfo.department_id)).all()

        # 转换为 dict 并添加 department_name
        def key_to_dict(key):
            d = row_to_dict(key)
            if key.department:
                d['department_name'] = key.department.department_name
            return d

        return jsonify([key_to_dict(key) for key in keys])
    except Exception as e:
        print(f"Error getting JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve JdyKeyInfo"}), 500


@api_bp.route('/jdy-keys', methods=['POST'])
@jwt_required()
def add_jdy_key():
    data = request.get_json()
    session = g.config_session
    user = get_current_user()
    try:
        department_id = data.get('department_id')
        if not user.is_superuser:
            department_id = user.department_id  # 非超管只能为自己部门添加

        if not department_id:
            return jsonify({"error": "Department ID is required."}), 400

        new_key = JdyKeyInfo(
            department_id=department_id,
            api_key=data.get('api_key'),
            api_secret=data.get('api_secret')
        )
        session.add(new_key)
        session.commit()

        # 重新加载以获取 department_name
        session.refresh(new_key, ['department'])
        d = row_to_dict(new_key)
        d['department_name'] = new_key.department.department_name
        return jsonify(d), 201

    except IntegrityError:
        session.rollback()
        return jsonify({"error": "Department already has a key (1-to-1 relationship)."}), 409
    except Exception as e:
        session.rollback()
        print(f"Error adding JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to add JdyKeyInfo"}), 500


@api_bp.route('/jdy-keys/<int:key_id>', methods=['PUT'])
@jwt_required()
def update_jdy_key(key_id):
    data = request.get_json()
    session = g.config_session
    user = get_current_user()

    try:
        # 检查权限
        key_to_update = session.get(JdyKeyInfo, key_id)
        if not key_to_update:
            return jsonify({"error": "JdyKeyInfo not found"}), 404
        if not user.is_superuser and key_to_update.department_id != user.department_id:
            return jsonify({"error": "Forbidden"}), 403

        department_id = data.get('department_id')
        if not user.is_superuser:
            department_id = user.department_id  # 非超管不能修改部门

        if not department_id:
            return jsonify({"error": "Department ID is required."}), 400

        stmt = update(JdyKeyInfo).where(JdyKeyInfo.id == key_id).values(
            department_id=department_id,
            api_key=data.get('api_key'),
            api_secret=data.get('api_secret')
        )
        result = session.execute(stmt)
        session.commit()

        updated_key = session.query(JdyKeyInfo).options(joinedload(JdyKeyInfo.department)).get(key_id)
        d = row_to_dict(updated_key)
        d['department_name'] = updated_key.department.department_name
        return jsonify(d)

    except IntegrityError:
        session.rollback()
        return jsonify({"error": "Department already has a key (1-to-1 relationship)."}), 409
    except Exception as e:
        session.rollback()
        print(f"Error updating JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to update JdyKeyInfo"}), 500


@api_bp.route('/jdy-keys/<int:key_id>', methods=['DELETE'])
@jwt_required()
def delete_jdy_key(key_id):
    session = g.config_session
    user = get_current_user()
    try:
        # 检查权限
        key_to_delete = session.get(JdyKeyInfo, key_id)
        if not key_to_delete:
            return jsonify({"error": "JdyKeyInfo not found"}), 404
        if not user.is_superuser and key_to_delete.department_id != user.department_id:
            return jsonify({"error": "Forbidden"}), 403

        stmt = delete(JdyKeyInfo).where(JdyKeyInfo.id == key_id)
        result = session.execute(stmt)
        session.commit()

        return jsonify({"message": "JdyKeyInfo deleted successfully"})
    except IntegrityError as e:
        session.rollback()
        # 检查是否有任务在引用
        return jsonify({"error": "Cannot delete Key: It may be referenced by active Sync Tasks."}), 409
    except Exception as e:
        session.rollback()
        print(f"Error deleting JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to delete JdyKeyInfo"}), 500


# --- [REFACTOR] Database CRUD (数据库管理) ---

# [REFACTOR] 路由修改
@api_bp.route('/databases', methods=['GET'])
@jwt_required()
def get_databases():
    session = g.config_session
    user = get_current_user()
    try:
        # [REFACTOR] 模型修改
        query = select(Database).options(joinedload(Database.department))
        if not user.is_superuser:
            query = query.where(Database.department_id == user.department_id)

        databases = session.scalars(query.order_by(Database.id)).all()

        def db_to_dict(db):
            d = row_to_dict(db)
            if db.department:
                d['department_name'] = db.department.department_name
            return d

        return jsonify([db_to_dict(db) for db in databases])
    except Exception as e:
        print(f"Error getting Databases: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve Databases"}), 500


# [REFACTOR] 路由修改
@api_bp.route('/databases', methods=['POST'])
@jwt_required()
def add_database():
    data = request.get_json()
    session = g.config_session
    user = get_current_user()
    try:
        department_id = data.get('department_id')
        if not user.is_superuser:
            department_id = user.department_id

        if not department_id:
            return jsonify({"error": "Department ID is required."}), 400

        # [REFACTOR] 增加 sync_type
        new_db = Database(
            department_id=department_id,
            sync_type=data.get('sync_type'),  # 新增
            db_show_name=data.get('db_show_name'),
            db_type=data.get('db_type'),
            db_host=data.get('db_host'),
            db_port=data.get('db_port'),
            db_name=data.get('db_name'),
            db_args=data.get('db_args'),
            db_user=data.get('db_user'),
            db_password=data.get('db_password'),
            is_active=data.get('is_active', True)
        )
        session.add(new_db)
        session.commit()

        session.refresh(new_db, ['department'])
        d = row_to_dict(new_db)
        d['department_name'] = new_db.department.department_name
        return jsonify(d), 201

    except IntegrityError as e:
        session.rollback()
        if 'uq_dept_db_show_name' in str(e):
            return jsonify({"error": "This 'Show Name' already exists in this department."}), 409
        if 'uq_db_connection_info' in str(e):
            return jsonify({"error": "This database connection info is already registered."}), 409
        return jsonify({"error": f"Database integrity error: {e}"}), 409
    except Exception as e:
        session.rollback()
        print(f"Error adding Database: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to add Database"}), 500


# [REFACTOR] 路由修改
@api_bp.route('/databases/<int:db_id>', methods=['PUT'])
@jwt_required()
def update_database(db_id):
    data = request.get_json()
    session = g.config_session
    user = get_current_user()
    try:
        # [REFACTOR] 权限检查
        db_to_update = session.get(Database, db_id)
        if not db_to_update:
            return jsonify({"error": "Database not found"}), 404
        if not user.is_superuser and db_to_update.department_id != user.department_id:
            return jsonify({"error": "Forbidden"}), 403

        department_id = data.get('department_id')
        if not user.is_superuser:
            department_id = user.department_id

        if not department_id:
            return jsonify({"error": "Department ID is required."}), 400

        # [REFACTOR] 增加 sync_type
        update_values = {
            'department_id': department_id,
            'sync_type': data.get('sync_type'),  # 新增
            'db_show_name': data.get('db_show_name'),
            'db_type': data.get('db_type'),
            'db_host': data.get('db_host'),
            'db_port': data.get('db_port'),
            'db_name': data.get('db_name'),
            'db_args': data.get('db_args'),
            'db_user': data.get('db_user'),
            'is_active': data.get('is_active')
        }
        # 只有在提供了新密码时才更新
        if data.get('db_password'):
            update_values['db_password'] = data.get('db_password')

        stmt = update(Database).where(Database.id == db_id).values(**update_values)
        result = session.execute(stmt)
        session.commit()

        updated_db = session.query(Database).options(joinedload(Database.department)).get(db_id)
        d = row_to_dict(updated_db)
        d['department_name'] = updated_db.department.department_name
        return jsonify(d)

    except IntegrityError:
        session.rollback()
        return jsonify({"error": "Integrity error. Show Name or Connection Info might be duplicated."}), 409
    except Exception as e:
        session.rollback()
        print(f"Error updating Database: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to update Database"}), 500


# [REFACTOR] 路由修改
@api_bp.route('/databases/<int:db_id>', methods=['DELETE'])
@jwt_required()
def delete_database(db_id):
    session = g.config_session
    user = get_current_user()
    try:
        # [REFACTOR] 权限检查
        db_to_delete = session.get(Database, db_id)
        if not db_to_delete:
            return jsonify({"error": "Database not found"}), 404
        if not user.is_superuser and db_to_delete.department_id != user.department_id:
            return jsonify({"error": "Forbidden"}), 403

        stmt = delete(Database).where(Database.id == db_id)
        result = session.execute(stmt)
        session.commit()

        return jsonify({"message": "Database deleted successfully"})
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "Cannot delete: This database is referenced by one or more Sync Tasks."}), 409
    except Exception as e:
        session.rollback()
        print(f"Error deleting Database: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to delete Database"}), 500


# [REFACTOR] 新增：数据库连接测试
@api_bp.route('/databases/test', methods=['POST'])
@jwt_required()
def test_database_connection():
    data = request.get_json()
    # 移除 department_id, is_active 等非连接字段
    connection_info = {
        'db_type': data.get('db_type'),
        'db_host': data.get('db_host'),
        'db_port': data.get('db_port'),
        'db_name': data.get('db_name'),
        'db_user': data.get('db_user'),
        'db_password': data.get('db_password'),
        'db_args': data.get('db_args')
    }

    # 权限检查：确保用户有权测试 (虽然只是测试，但最好还是检查)
    user = get_current_user()
    if not user.is_superuser:
        if data.get('department_id') != user.department_id:
            # 如果是租户用户在尝试测试一个不属于他们的部门ID (虽然测试不依赖ID，但前端会传来)
            # 或者在创建新库时，他们没有 department_id，但我们从 user 对象中知道
            pass  # 暂时允许租户测试他们自己的连接

    try:
        success, message = test_db_connection(connection_info)
        if success:
            return jsonify({"msg": message}), 200
        else:
            return jsonify({"error": message}), 400
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


# --- SyncTask CRUD (同步任务) ---

@api_bp.route('/sync-tasks', methods=['GET'])
@jwt_required()
def get_sync_tasks():
    session = g.config_session
    user = get_current_user()
    try:
        # [REFACTOR] 增加 sync_type 过滤
        sync_type_filter = request.args.get('sync_type')

        query = select(SyncTask).options(
            joinedload(SyncTask.department),
            joinedload(SyncTask.database)  # [REFACTOR]
        )

        if not user.is_superuser:
            query = query.where(SyncTask.department_id == user.department_id)

        if sync_type_filter in ['db2jdy', 'jdy2db']:
            query = query.where(SyncTask.sync_type == sync_type_filter)

        tasks = session.scalars(query.order_by(SyncTask.id)).all()

        # [REFACTOR] 使用新的 task_to_dict 辅助函数
        return jsonify([task_to_dict(task) for task in tasks])
    except Exception as e:
        print(f"Error getting SyncTasks: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve Sync Tasks"}), 500


def _parse_time_string(time_str: str | None) -> time | None:
    """辅助函数：从 ISO 字符串 (HH:MM:SS) 或 (YYYY-MM-DDTHH:MM:SS) 中解析时间对象"""
    if not time_str:
        return None
    try:
        #  handles HH:MM:SS or HH:MM formats from ISO string
        # 增加对 HH:mm 的支持
        if len(time_str) == 5:
            time_str += ":00"

        time_part = time_str.split('T')[-1].split('.')[0]
        return time.fromisoformat(time_part)
    except (ValueError, TypeError):
        return None


@api_bp.route('/sync-tasks', methods=['POST'])
@jwt_required()
def add_sync_task():
    data = request.get_json()
    session = g.config_session
    user = get_current_user()

    sync_type = data.get('sync_type')
    if not sync_type:
        return jsonify({"error": "sync_type (db2jdy or jdy2db) is required."}), 400

    try:
        department_id = data.get('department_id')
        if not user.is_superuser:
            department_id = user.department_id

        if not department_id:
            return jsonify({"error": "Department ID is required."}), 400

        database_id = data.get('database_id')
        if not database_id:
            return jsonify({"error": "Database ID is required."}), 400

        # --- 检查 Database 是否存在且匹配 ---
        db_manage = session.get(Database, database_id)
        if not db_manage:
            return jsonify({"error": f"Database (ID: {database_id}) not found."}), 404
        if not user.is_superuser and db_manage.department_id != user.department_id:
            return jsonify({"error": "Database does not belong to your department."}), 403
        if db_manage.sync_type != sync_type:
            return jsonify({
                               "error": f"Database sync_type ('{db_manage.sync_type}') does not match task sync_type ('{sync_type}')."}), 400

        new_task = None

        # --- 分支：创建 jdy2db 任务 ---
        if sync_type == 'jdy2db':
            table_name = data.get('table_name')
            if not table_name:
                return jsonify({"error": "Table Name is required for jdy2db task."}), 400

            # 获取部门名称和数据库名称以生成 URL
            department = session.get(Department, department_id)

            # 生成 Webhook URL
            webhook_url = (
                f"{Config.BASE_URL}/api/jdy/webhook?"
                f"dpt={department.department_name}&"
                f"db={db_manage.db_show_name}&"
                f"table={table_name}"
            )

            new_task = SyncTask(
                sync_type='jdy2db',
                task_name=data.get('task_name'),
                department_id=department_id,
                database_id=database_id,
                table_name=table_name,
                webhook_url=webhook_url,  # 自动生成

                # jdy2db 特定字段
                daily_sync_time=_parse_time_string(data.get('daily_sync_time')),
                daily_sync_type=data.get('daily_sync_type', 'ONCE'),
                # 修正字段名
                is_full_replace_first=data.get('is_full_replace_first', True),
                json_as_string=data.get('json_as_string', False),
                label_to_pinyin=data.get('label_to_pinyin', False),

                # db2jdy 字段设为 Null
                app_id=None,
                entry_id=None,
                sync_mode=None,

                # 通用
                is_active=data.get('is_active', True),
                sync_status='idle',
                send_error_log_to_wecom=data.get('send_error_log_to_wecom', False),
                wecom_robot_webhook_url=data.get('wecom_robot_webhook_url')
            )

        # --- 分支：创建 db2jdy 任务 ---
        elif sync_type == 'db2jdy':
            if not data.get('app_id') or not data.get('entry_id'):
                return jsonify({"error": "App ID and Entry ID are required for db2jdy task."}), 400

            new_task = SyncTask(
                sync_type='db2jdy',
                task_name=data.get('task_name'),
                department_id=department_id,
                database_id=database_id,
                table_name=data.get('table_name'),

                # db2jdy 特定字段
                business_keys=data.get('business_keys'),
                app_id=data.get('app_id'),
                entry_id=data.get('entry_id'),
                sync_mode=data.get('sync_mode', 'INCREMENTAL'),
                incremental_field=data.get('incremental_field'),
                incremental_interval=data.get('incremental_interval'),
                full_replace_time=_parse_time_string(data.get('full_replace_time')),
                source_filter_sql=data.get('source_filter_sql'),
                is_full_replace_first=data.get('is_full_replace_first', True),

                # jdy2db 字段设为 Null
                daily_sync_time=None,
                daily_sync_type=None,

                # 通用
                is_active=data.get('is_active', True),
                sync_status='idle',
                send_error_log_to_wecom=data.get('send_error_log_to_wecom', False),
                wecom_robot_webhook_url=data.get('wecom_robot_webhook_url')
            )

        else:
            return jsonify({"error": f"Invalid sync_type: {sync_type}"}), 400

        session.add(new_task)
        session.commit()  # 提交以获取 task_id

        # --- 通知调度器 ---
        # 重新查询更新后的任务，以确保所有关联字段 (department, database) 都是最新的
        final_task = session.query(SyncTask).options(
            joinedload(SyncTask.department),
            joinedload(SyncTask.database)  # [REFACTOR]
        ).get(new_task.id)

        if final_task:
            # (解决问题3) 确保调度器被调用
            add_or_update_task_in_scheduler(final_task)

        return jsonify(task_to_dict(final_task)), 201

    except IntegrityError as e:
        session.rollback()
        # if "uq_app_entry" in str(e).lower():
        #     return jsonify({"error": "App ID and Entry ID combination already exists."}), 409
        if "foreign key constraint" in str(e).lower():
            return jsonify({"error": "Invalid Department or Database ID."}), 400
        return jsonify({"error": f"Database integrity error: {e}"}), 409
    except Exception as e:
        session.rollback()
        print(f"Error adding SyncTask: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"Failed to add SyncTask: {e}"}), 500


@api_bp.route('/sync-tasks/<int:task_id>', methods=['PUT'])
@jwt_required()
def update_sync_task(task_id):
    data = request.get_json()
    session = g.config_session
    user = get_current_user()

    try:
        # --- 权限检查 ---
        task_to_update = session.get(SyncTask, task_id)
        if not task_to_update:
            return jsonify({"error": "SyncTask not found"}), 404
        if not user.is_superuser and task_to_update.department_id != user.department_id:
            return jsonify({"error": "Forbidden"}), 403

        # 获取任务类型 (不能更改)
        sync_type = task_to_update.sync_type

        department_id = data.get('department_id')
        if not user.is_superuser:
            department_id = user.department_id

        database_id = data.get('database_id')

        # --- 检查 Database ---
        db_manage = session.get(Database, database_id)
        if not db_manage:
            return jsonify({"error": f"Database (ID: {database_id}) not found."}), 404
        if not user.is_superuser and db_manage.department_id != user.department_id:
            return jsonify({"error": "Database does not belong to your department."}), 403
        if db_manage.sync_type != sync_type:
            return jsonify({
                               "error": f"Database sync_type ('{db_manage.sync_type}') does not match task sync_type ('{sync_type}')."}), 400

        update_values = {
            'task_name': data.get('task_name'),
            'department_id': department_id,
            'database_id': database_id,
            'table_name': data.get('table_name'),
            'is_active': data.get('is_active'),
            'send_error_log_to_wecom': data.get('send_error_log_to_wecom'),
            'wecom_robot_webhook_url': data.get('wecom_robot_webhook_url'),
        }

        # --- 分支：更新 jdy2db 任务 ---
        if sync_type == 'jdy2db':
            department = session.get(Department, department_id)
            # 重新生成 Webhook URL
            update_values['webhook_url'] = (
                f"{Config.BASE_URL}/api/jdy/webhook?"
                f"dpt={department.department_name}&"
                f"db={db_manage.db_show_name}&"
                f"table={data.get('table_name')}"
            )
            update_values.update({
                'daily_sync_time': _parse_time_string(data.get('daily_sync_time')),
                'daily_sync_type': data.get('daily_sync_type'),
                'is_full_replace_first': data.get('is_full_replace_first'),
                'json_as_string': data.get('json_as_string'),
                'label_to_pinyin': data.get('label_to_pinyin'),
            })

        # --- 分支：更新 db2jdy 任务 ---
        elif sync_type == 'db2jdy':
            update_values.update({
                'business_keys': data.get('business_keys'),
                'app_id': data.get('app_id'),
                'entry_id': data.get('entry_id'),
                'sync_mode': data.get('sync_mode'),
                'incremental_field': data.get('incremental_field'),
                'incremental_interval': data.get('incremental_interval'),
                'full_replace_time': _parse_time_string(data.get('full_replace_time')),
                'source_filter_sql': data.get('source_filter_sql'),
                'is_full_replace_first': data.get('is_full_replace_first'),
            })

        stmt = update(SyncTask).where(SyncTask.id == task_id).values(**update_values)
        result = session.execute(stmt)
        session.commit()

        # --- 通知调度器 ---
        # 重新查询更新后的任务，以确保所有字段都是最新的
        updated_task = session.query(SyncTask).options(
            joinedload(SyncTask.department),
            joinedload(SyncTask.database)  # [REFACTOR]
        ).get(task_id)

        if updated_task:
            # (解决问题3) 确保调度器被调用
            add_or_update_task_in_scheduler(updated_task)

        return jsonify(task_to_dict(updated_task))

    except IntegrityError as e:
        session.rollback()
        # if "uq_app_entry" in str(e).lower():
        #     return jsonify({"error": "App ID and Entry ID combination already exists."}), 409
        if "foreign key constraint" in str(e).lower():
            return jsonify({"error": "Invalid Department or Database ID."}), 400
        return jsonify({"error": f"Database integrity error: {e}"}), 409
    except Exception as e:
        session.rollback()
        print(f"Error updating SyncTask: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"Failed to update SyncTask: {e}"}), 500


@api_bp.route('/sync-tasks/<int:task_id>', methods=['DELETE'])
@jwt_required()
def delete_sync_task(task_id):
    session = g.config_session
    user = get_current_user()
    try:
        # --- 权限检查 ---
        task_to_delete = session.get(SyncTask, task_id)
        if not task_to_delete:
            return jsonify({"error": "SyncTask not found"}), 404
        if not user.is_superuser and task_to_delete.department_id != user.department_id:
            return jsonify({"error": "Forbidden"}), 403

        # --- 通知调度器 ---
        # (解决问题3) 在删除数据库记录之前，先从调度器中移除
        remove_task_from_scheduler(task_id)

        # 使用 task_id
        stmt = delete(SyncTask).where(SyncTask.id == task_id)
        result = session.execute(stmt)
        session.commit()
        if result.rowcount == 0:
            return jsonify({"error": "SyncTask not found"}), 404
        return jsonify({"message": "SyncTask deleted successfully"})
    except Exception as e:
        session.rollback()
        print(f"Error deleting SyncTask: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to delete SyncTask"}), 500


# --- SyncErrLog Read ---

@api_bp.route('/sync-logs', methods=['GET'])
@jwt_required()
def get_sync_logs():
    session = g.config_session
    user = get_current_user()
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)

        # [REFACTOR] 过滤 sync_type (原文件已有)
        sync_type_filter = request.args.get('sync_type')
        query = select(SyncErrLog)

        if not user.is_superuser:
            query = query.where(SyncErrLog.department_id == user.department_id)

        if sync_type_filter in ['db2jdy', 'jdy2db']:
            query = query.where(SyncErrLog.sync_type == sync_type_filter)

        logs = session.scalars(
            query
            .order_by(desc(SyncErrLog.timestamp))
            .limit(limit)
            .offset(offset)
        ).all()
        return jsonify([row_to_dict(log) for log in logs])
    except Exception as e:
        print(f"Error getting SyncErrLogs: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve Sync Error Logs"}), 500


# --- FormFieldMapping Read ---

@api_bp.route('/field-mappings', methods=['GET'])
@jwt_required()
def get_field_mappings():
    session = g.config_session
    user = get_current_user()
    try:
        # 使用 task_id 过滤
        task_id = request.args.get('task_id', type=int)
        if not task_id:
            return jsonify({"error": "task_id is required."}), 400

        query = select(FormFieldMapping).where(FormFieldMapping.task_id == task_id)

        # [REFACTOR] 权限检查
        if not user.is_superuser:
            # 检查这个 task_id 是否属于该用户
            task_dept_id = session.scalar(select(SyncTask.department_id).where(SyncTask.id == task_id))
            if task_dept_id != user.department_id:
                return jsonify({"error": "Forbidden"}), 403

        mappings = session.scalars(query).all()
        return jsonify([row_to_dict(m) for m in mappings])
    except Exception as e:
        print(f"Error getting FormFieldMappings: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve Field Mappings"}), 500


# --- [REFACTOR] 新增：Webhook 接收器 ---
# (从 app/routes.py 迁移并重构)

@api_bp.route('/jdy/webhook', methods=['POST'])
def jdy_webhook():
    """
    简道云 Webhook 接收器。
    通过 URL 参数定位任务，然后处理推送的数据。
    URL 样例: /api/jdy/webhook?dpt={task.department_name}&db={task.db_show_name}&table={task.table_name}
    """
    # 1. (关键) 从 URL 获取任务定位参数
    url_params = {
        "dpt": request.args.get('dpt'),
        "db": request.args.get('db'),
        "table": request.args.get('table')
    }

    # 2. 获取 JSON 负载
    payload = request.get_json()

    if not all(url_params.values()) or not payload:
        current_app.logger.error(
            f"[Webhook] 缺少参数。Params: {url_params}, HasPayload: {bool(payload)}"
        )
        return jsonify({"error": "Missing required URL parameters or payload."}), 400

    config_session = None
    try:
        # 3. (关键) 使用 g.config_session
        # Webhook 调用不在 @jwt_required() 保护下，也没有 before_request
        # 我们必须手动创建和关闭会话。
        # [FIX] 改为使用 app context 来获取 g
        with current_app.app_context():
            config_session = g.config_session

            # 4. 调用核心处理逻辑
            # process_webhook 会处理数据库查询、API客户端实例化和数据处理
            jdy2db_services.process_webhook(
                config_session=config_session,
                url_params=url_params,
                payload=payload
            )

            # 5. 提交事务
            # process_webhook 内部不提交，由 API 层统一提交
            config_session.commit()

            return jsonify({"status": "success"}), 200

    except Exception as e:
        # 捕获 process_webhook 中可能抛出的所有异常
        current_app.logger.error(
            f"[Webhook] 处理失败。Params: {url_params}, Error: {e}\n{traceback.format_exc()}"
        )
        if config_session:
            config_session.rollback()  # 回滚

        # 即使失败，也可能需要返回 200，以防止简道云重试
        # 但返回 500 更能反映真实情况
        return jsonify({"error": f"Internal server error: {e}"}), 500

    # [FIX] g.config_session 的管理由 @teardown_request 自动处理
    # finally:
    #     if config_session:
    #         config_session_scoped.remove() # 确保会话被移除
