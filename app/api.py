# -*- coding: utf-8 -*-
import traceback
from datetime import time, datetime

from flask import Blueprint, jsonify, request, g
from flask_jwt_extended import jwt_required
from sqlalchemy import select, update, delete, desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload

from app.models import JdyKeyInfo, SyncTask, SyncErrLog, FormFieldMapping, Department, Database
from app.scheduler import add_or_update_task_in_scheduler, remove_task_from_scheduler

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


# --- JdyKeyInfo CRUD (密钥管理) ---

@api_bp.route('/jdy-keys', methods=['GET'])
@jwt_required()
def get_jdy_keys():
    session = g.config_session
    try:
        keys = session.scalars(select(JdyKeyInfo).order_by(JdyKeyInfo.department_id)).all()
        return jsonify([row_to_dict(key) for key in keys])
    except Exception as e:
        print(f"Error getting JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve JdyKeyInfo"}), 500


@api_bp.route('/jdy-keys', methods=['POST'])
@jwt_required()
def add_jdy_key():
    data = request.get_json()
    session = g.config_session
    try:
        # --- 查询 department_id ---
        department_name = data.get('department_name')
        department = session.scalar(select(Department).where(Department.department_name == department_name))
        if not department:
            return jsonify({"error": f"Department '{department_name}' not found."}), 400

        new_key = JdyKeyInfo(
            department_id=department.id,
            api_key=data.get('api_key'),
            app_secret=data.get('api_secret')
        )
        session.add(new_key)
        session.commit()
        return jsonify(row_to_dict(new_key)), 201
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "Department name already exists."}), 409
    except Exception as e:
        session.rollback()
        print(f"Error adding JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to add JdyKeyInfo"}), 500


@api_bp.route('/jdy-keys/<int:key_id>', methods=['PUT'])
@jwt_required()
def update_jdy_key(key_id):
    data = request.get_json()
    session = g.config_session
    try:
        # --- 查询 department_id ---
        department_name = data.get('department_name')
        department = session.scalar(select(Department).where(Department.department_name == department_name))
        if not department:
            return jsonify({"error": f"Department '{department_name}' not found."}), 400

        stmt = update(JdyKeyInfo).where(JdyKeyInfo.id == key_id).values(
            department_id=department.id,
            api_key=data.get('api_key'),
            app_secret=data.get('api_secret')
        )
        result = session.execute(stmt)
        session.commit()
        if result.rowcount == 0:
            return jsonify({"error": "JdyKeyInfo not found"}), 404

        updated_key = session.get(JdyKeyInfo, key_id)
        return jsonify(row_to_dict(updated_key))
    except IntegrityError:
        session.rollback()
        return jsonify({"error": "Department name already exists."}), 409
    except Exception as e:
        session.rollback()
        print(f"Error updating JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to update JdyKeyInfo"}), 500


@api_bp.route('/jdy-keys/<int:key_id>', methods=['DELETE'])
@jwt_required()
def delete_jdy_key(key_id):
    session = g.config_session
    try:
        stmt = delete(JdyKeyInfo).where(JdyKeyInfo.id == key_id)
        result = session.execute(stmt)
        session.commit()
        if result.rowcount == 0:
            return jsonify({"error": "JdyKeyInfo not found"}), 404
        return jsonify({"message": "JdyKeyInfo deleted successfully"})
    except IntegrityError as e:
        session.rollback()
        if "foreign key constraint" in str(e).lower():
            return jsonify({"error": "Cannot delete Key: It is referenced by one or more Sync Tasks."}), 409
        return jsonify({"error": "Integrity error."}), 500
    except Exception as e:
        session.rollback()
        print(f"Error deleting JdyKeyInfo: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to delete JdyKeyInfo"}), 500


# --- SyncTask CRUD (同步任务) ---

@api_bp.route('/sync-tasks', methods=['GET'])
@jwt_required()
def get_sync_tasks():
    session = g.config_session
    try:
        tasks = session.scalars(select(SyncTask).order_by(SyncTask.id)).all()
        return jsonify([row_to_dict(task) for task in tasks])
    except Exception as e:
        print(f"Error getting SyncTasks: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve Sync Tasks"}), 500


def _parse_time_string(time_str: str | None) -> time | None:
    """辅助函数：从 ISO 字符串 (HH:MM:SS) 或 (YYYY-MM-DDTHH:MM:SS) 中解析时间对象"""
    if not time_str:
        return None
    try:
        #  handles HH:MM:SS or HH:MM formats from ISO string
        time_part = time_str.split('T')[-1].split('.')[0]
        return time.fromisoformat(time_part)
    except (ValueError, TypeError):
        return None


@api_bp.route('/sync-tasks', methods=['POST'])
@jwt_required()
def add_sync_task():
    data = request.get_json()
    session = g.config_session
    try:
        full_replace_time_obj = _parse_time_string(data.get('full_replace_time'))

        # --- 查询 department_id ---
        department_id = data.get('department_id')
        if not department_id:
            department_name = data.get('department_name')
            if department_name:
                department = session.scalar(select(Department).where(Department.department_name == department_name))
                if department:
                    department_id = department.id

            if not department_id:
                return jsonify({"error": f"Department '{department_name}' not found."}), 400

        # --- 查询 database_id ---
        database_id = data.get('database_id')
        if not database_id:
            db_show_name = data.get('db_show_name')
            if db_show_name:
                source_db = session.scalar(select(Database).where(Database.db_show_name == db_show_name))
                if source_db:
                    database_id = source_db.id
                    if not source_db.is_active:
                        return jsonify({"error": f"Source DB '{db_show_name}' is not active."}), 400

            if not database_id:
                return jsonify({"error": f"Database '{database_id}' not found."}), 400

        new_task = SyncTask(
            task_name=data.get('task_name'),
            database_id=database_id,
            department_id=department_id,
            table_name=data.get('table_name'),
            business_keys=data.get('business_keys'),
            app_id=data.get('app_id'),
            entry_id=data.get('entry_id'),
            sync_mode=data.get('sync_mode', 'INCREMENTAL'),
            incremental_field=data.get('incremental_field'),
            incremental_interval=data.get('incremental_interval'),
            full_replace_time=full_replace_time_obj,
            source_filter_sql=data.get('source_filter_sql'),
            is_full_replace_first=data.get('is_full_replace_first', True),
            is_active=data.get('is_active', True),
            send_error_log_to_wecom=data.get('send_error_log_to_wecom', False),
            wecom_robot_webhook_url=data.get('wecom_robot_webhook_url'),
            sync_status='idle'  # Initial status
        )
        session.add(new_task)
        session.commit()  # 提交以获取 task_id

        # --- 通知调度器 ---
        # 重新查询更新后的任务，以确保所有字段 (包括 .department) 都是最新的
        final_task = session.query(SyncTask).options(
            joinedload(SyncTask.department)
        ).get(new_task.id)

        if final_task:
            add_or_update_task_in_scheduler(final_task)

        return jsonify(row_to_dict(final_task)), 201
    except IntegrityError as e:
        session.rollback()
        if "uq_app_entry" in str(e).lower():
            return jsonify({"error": "App ID and Entry ID combination already exists."}), 409
        if "foreign key constraint" in str(e).lower():
            return jsonify({"error": "Invalid department name."}), 400
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
    try:
        full_replace_time_obj = _parse_time_string(data.get('full_replace_time'))

        # --- 查询 department_id ---
        department_id = data.get('department_id')
        if not department_id:
            department_name = data.get('department_name')
            if department_name:
                department = session.scalar(select(Department).where(Department.department_name == department_name))
                if department:
                    department_id = department.id

            if not department_id:
                return jsonify({"error": f"Department '{department_name}' not found."}), 400

        # --- 查询 database_id ---
        database_id = data.get('database_id')
        if not database_id:
            db_show_name = data.get('db_show_name')
            if db_show_name:
                source_db = session.scalar(select(Database).where(Database.db_show_name == db_show_name))
                if source_db:
                    database_id = source_db.id
                    if not source_db.is_active:
                        return jsonify({"error": f"Source DB '{db_show_name}' is not active."}), 400

            if not database_id:
                return jsonify({"error": f"Database '{database_id}' not found."}), 400

        update_values = {
            'task_name': data.get('task_name'),
            'database_id': database_id,
            'table_name': data.get('table_name'),
            'business_keys': data.get('business_keys'),
            'app_id': data.get('app_id'),
            'entry_id': data.get('entry_id'),
            'department_id': department_id,
            'sync_mode': data.get('sync_mode'),
            'incremental_field': data.get('incremental_field'),
            'incremental_interval': data.get('incremental_interval'),
            'full_replace_time': full_replace_time_obj,
            'source_filter_sql': data.get('source_filter_sql'),
            'is_full_replace_first': data.get('is_full_replace_first'),
            'is_active': data.get('is_active'),
            'send_error_log_to_wecom': data.get('send_error_log_to_wecom'),
            'wecom_robot_webhook_url': data.get('wecom_robot_webhook_url'),
        }

        stmt = update(SyncTask).where(SyncTask.id == task_id).values(**update_values)
        result = session.execute(stmt)
        session.commit()

        if result.rowcount == 0:
            return jsonify({"error": "SyncTask not found"}), 404

        # --- 通知调度器 ---
        # 重新查询更新后的任务，以确保所有字段 (包括 .department) 都是最新的
        updated_task = session.query(SyncTask).options(
            joinedload(SyncTask.department),
            joinedload(SyncTask.database)
        ).get(task_id)

        if updated_task:
            add_or_update_task_in_scheduler(updated_task)

        return jsonify(row_to_dict(updated_task))
    except IntegrityError as e:
        session.rollback()
        if "uq_app_entry" in str(e).lower():
            return jsonify({"error": "App ID and Entry ID combination already exists."}), 409
        if "foreign key constraint" in str(e).lower():
            return jsonify({"error": "Invalid department name."}), 400
        return jsonify({"error": f"Database integrity error: {e}"}), 409
    except Exception as e:
        session.rollback()
        print(f"Error updating SyncTask: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"Failed to update SyncTask: {e}"}), 500


@api_bp.route('/sync-tasks/<int:task_id>', methods=['DELETE'])
@jwt_required()
def delete_sync_task(task_id):
    session = g.config_session
    try:
        # --- 通知调度器 ---
        # 在删除数据库记录之前，先从调度器中移除
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
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)

        # --- REFACTOR: 19. 过滤 sync_type ---
        sync_type_filter = request.args.get('sync_type')
        query = select(SyncErrLog)
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
    try:
        # 使用 task_id 过滤
        task_id = request.args.get('task_id')

        query = select(FormFieldMapping)
        if task_id:
            query = query.where(FormFieldMapping.task_id == task_id)

        mappings = session.scalars(query).all()
        return jsonify([row_to_dict(m) for m in mappings])
    except Exception as e:
        print(f"Error getting FormFieldMappings: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to retrieve Field Mappings"}), 500
