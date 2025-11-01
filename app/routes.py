# -*- coding: utf-8 -*-
import logging
from flask import Blueprint, request, jsonify, g
from flask_jwt_extended import jwt_required, get_jwt
from sqlalchemy.orm import joinedload
from sqlalchemy.exc import IntegrityError
from app.models import ( Department, User, DatabaseInfo, JdyKeyInfo, SyncTask, SyncErrLog)
from app.auth import superuser_required

# 创建 API 蓝图
api_bp = Blueprint('api_bp', __name__)


# --- 辅助函数 ---

def get_current_user_claims():
    """从 JWT 获取当前用户的声明"""
    return get_jwt()


def to_dict(model_instance):
    """简单的 SQLAlchemy 模型转字典"""
    if not model_instance:
        return None
    d = {}
    for column in model_instance.__table__.columns:
        # 不暴露密码哈希
        if column.name == 'password':
            continue
        d[column.name] = getattr(model_instance, column.name)

    # --- 自动添加 department_name (如果存在) ---
    if hasattr(model_instance, 'department') and model_instance.department:
        d['department_name'] = model_instance.department.department_name

    # --- 添加关联数据库显示名称 ---
    if isinstance(model_instance, SyncTask):
        if model_instance.source_database:
            d['source_db_name'] = model_instance.source_database.db_show_name
        else:
            d['source_db_name'] = 'N/A'

    return d


# --- Department (租户) API ---
# 只有超级管理员可以管理

@api_bp.route('/api/departments', methods=['GET'])
@superuser_required
def get_departments():
    session = g.config_session
    try:
        departments = session.query(Department).all()
        return jsonify([to_dict(d) for d in departments]), 200
    finally:
        pass


@api_bp.route('/api/departments', methods=['POST'])
@superuser_required
def create_department():
    data = request.get_json()
    session = g.config_session
    try:
        new_dept = Department(
            id=data.get('id'),
            department_name=data.get('department_name'),
            is_active=data.get('is_active', True)
        )
        session.add(new_dept)
        session.commit()
        return jsonify(to_dict(new_dept)), 201
    except IntegrityError as e:  # 捕获唯一约束
        session.rollback()
        logging.error(f"Create department error: {e}")
        return jsonify({"msg": "创建失败：部门ID或名称已存在。"}), 409
    except Exception as e:
        session.rollback()
        logging.error(f"Create department error: {e}")
        return jsonify({"msg": f"Error creating department: {e}"}), 500
    finally:
        pass


@api_bp.route('/api/departments/<int:id>', methods=['PUT'])
@superuser_required
def update_department(id):
    data = request.get_json()
    session = g.config_session
    try:
        dept = session.query(Department).get(id)
        if not dept:
            return jsonify({"msg": "Department not found"}), 404

        # 字段名应为 id
        dept.id = data.get('id', dept.id)
        dept.department_name = data.get('department_name', dept.department_name)
        dept.is_active = data.get('is_active', dept.is_active)
        session.commit()
        return jsonify(to_dict(dept)), 200
    except IntegrityError as e:  # 捕获唯一约束
        session.rollback()
        logging.error(f"Update department error: {e}")
        return jsonify({"msg": "更新失败：部门ID或名称已存在。"}), 409
    except Exception as e:
        session.rollback()
        logging.error(f"Update department error: {e}")
        return jsonify({"msg": f"Error updating department: {e}"}), 500
    finally:
        pass


@api_bp.route('/api/departments/<int:id>', methods=['DELETE'])
@superuser_required
def delete_department(id):
    session = g.config_session
    try:
        dept = session.query(Department).get(id)
        if not dept:
            return jsonify({"msg": "Department not found"}), 404

        session.delete(dept)
        session.commit()
        return jsonify({"msg": "Department deleted"}), 200
    except Exception as e:
        session.rollback()
        logging.error(f"Delete department error: {e}")
        # 捕获外键约束错误
        if "foreign key constraint fails" in str(e).lower():
            return jsonify(
                {"msg": "Cannot delete department: It is referenced by other resources (users, keys, etc.)"}), 409
        return jsonify({"msg": f"Error deleting department: {e}"}), 500
    finally:
        pass


# --- User (用户) API ---
# 只有超级管理员可以管理 (修改密码除外)

@api_bp.route('/api/users', methods=['GET'])
@superuser_required
def get_users():
    session = g.config_session
    try:
        # 预加载部门信息以显示名称 (虽然本视图不显示，但 to_dict 支持)
        users = session.query(User).options(joinedload(User.department)).all()
        return jsonify([to_dict(u) for u in users]), 200
    finally:
        pass


@api_bp.route('/api/users', methods=['POST'])
@superuser_required
def create_user():
    data = request.get_json()
    session = g.config_session
    try:
        new_user = User(
            username=data.get('username'),
            department_id=data.get('department_id'),
            is_superuser=data.get('is_superuser', False),
            is_active=data.get('is_active', True)
        )
        if not data.get('password'):
            return jsonify({"msg": "Password is required for new user"}), 400
        new_user.set_password(data.get('password'))
        session.add(new_user)
        session.commit()

        # 刷新以加载 department 关系
        session.refresh(new_user, ['department'])
        return jsonify(to_dict(new_user)), 201
    except IntegrityError as e:
        session.rollback()
        logging.error(f"Create user error: {e}")
        if "unique constraint" in str(e).lower() or "duplicate entry" in str(e).lower():
            return jsonify({"msg": "创建失败：用户名已存在。"}), 409
        return jsonify({"msg": f"数据库错误: {e}"}), 500
    except Exception as e:
        session.rollback()
        logging.error(f"Create user error: {e}")
        return jsonify({"msg": f"Error creating user: {e}"}), 500
    finally:
        pass


@api_bp.route('/api/users/<int:id>', methods=['PUT'])
@superuser_required
def update_user(id):
    data = request.get_json()
    session = g.config_session
    try:
        user = session.query(User).get(id)
        if not user:
            return jsonify({"msg": "User not found"}), 404

        user.username = data.get('username', user.username)
        user.department_id = data.get('department_id', user.department_id)
        user.is_superuser = data.get('is_superuser', user.is_superuser)
        user.is_active = data.get('is_active', user.is_active)
        session.commit()

        # 刷新以加载 department 关系
        session.refresh(user, ['department'])
        return jsonify(to_dict(user)), 200
    except IntegrityError as e:
        session.rollback()
        logging.error(f"Update user error: {e}")
        if "unique constraint" in str(e).lower() or "duplicate entry" in str(e).lower():
            return jsonify({"msg": "更新失败：用户名已存在。"}), 409
        return jsonify({"msg": f"数据库错误: {e}"}), 500
    except Exception as e:
        session.rollback()
        logging.error(f"Update user error: {e}")
        return jsonify({"msg": f"Error updating user: {e}"}), 500
    finally:
        pass


@api_bp.route('/api/users/<int:id>/reset-password', methods=['PATCH'])
@superuser_required
def reset_user_password(id):
    data = request.get_json()
    new_password = data.get('new_password')
    if not new_password:
        return jsonify({"msg": "Missing new_password"}), 400

    session = g.config_session
    try:
        user = session.query(User).get(id)
        if not user:
            return jsonify({"msg": "User not found"}), 404

        user.set_password(new_password)
        session.commit()
        return jsonify({"msg": f"Password for user {user.username} has been reset."}), 200
    except Exception as e:
        session.rollback()
        logging.error(f"Reset password error: {e}")
        return jsonify({"msg": f"Error resetting password: {e}"}), 500
    finally:
        pass


@api_bp.route('/api/users/<int:id>', methods=['DELETE'])
@superuser_required
def delete_user(id):
    session = g.config_session
    try:
        user = session.query(User).get(id)
        if not user:
            return jsonify({"msg": "User not found"}), 404

        session.delete(user)
        session.commit()
        return jsonify({"msg": "User deleted"}), 200
    except Exception as e:
        session.rollback()
        logging.error(f"Delete user error: {e}")
        return jsonify({"msg": f"Error deleting user: {e}"}), 500
    finally:
        pass


# --- API: 获取用于下拉框的数据库列表 ---

@api_bp.route('/api/tenant-databases', methods=['GET'])
@jwt_required()
def get_tenant_databases():
    """
    获取当前租户可用的数据库列表 (仅ID和显示名称)，用于填充表单下拉框。
    """
    claims = get_current_user_claims()
    session = g.config_session
    try:
        query = (session.query(DatabaseInfo)
                 .with_entities(DatabaseInfo.id, DatabaseInfo.db_show_name, DatabaseInfo.department_id))

        if claims.get('is_superuser'):
            items = query.all()
        else:
            department_id = claims.get('department_id')
            items = query.filter_by(department_id=department_id).all()

        return jsonify(
            [{"id": item.id, "name": item.db_show_name, "department_id": item.department_id} for item in items]), 200
    except Exception as e:
        logging.error(f"Get tenant-databases error: {e}")
        return jsonify({"msg": "Internal server error"}), 500
    finally:
        pass


# --- 通用资源 API (DatabaseInfo, JdyKeyInfo, SyncTask) ---

def create_resource_endpoints(bp, model_class, route_name):
    """
    辅助函数，为 DatabaseInfo, JdyKeyInfo, SyncTask 创建权限控制的 API
    """

    @bp.route(f'/api/{route_name}', methods=['GET'], endpoint=f'get_{route_name}')
    @jwt_required()
    def get_items():
        claims = get_current_user_claims()
        session = g.config_session
        try:
            query = session.query(model_class)

            # --- 预加载关联数据 ---
            if model_class == SyncTask:
                query = query.options(joinedload(SyncTask.source_database), joinedload(SyncTask.department))
            elif model_class == DatabaseInfo:
                query = query.options(joinedload(DatabaseInfo.department))
            elif model_class == JdyKeyInfo:
                query = query.options(joinedload(JdyKeyInfo.department))

            if claims.get('is_superuser'):
                items = query.all()
            else:
                department_id = claims.get('department_id')
                items = query.filter_by(department_id=department_id).all()

            return jsonify([to_dict(item) for item in items]), 200
        finally:
            pass

    @bp.route(f'/api/{route_name}', methods=['POST'], endpoint=f'create_{route_name}')
    @jwt_required()
    def create_item():
        claims = get_current_user_claims()
        data = request.get_json()
        session = g.config_session

        if not claims.get('is_superuser'):
            data['department_id'] = claims.get('department_id')

        if 'department_id' not in data or data['department_id'] is None:
            if model_class in [DatabaseInfo, SyncTask, JdyKeyInfo]:
                return jsonify({"msg": "department_id is required"}), 400

        # --- 移除 'created_at' 和 'updated_at' (如果它们存在) ---
        data.pop('created_at', None)
        data.pop('updated_at', None)

        try:
            new_item = model_class(**data)
            session.add(new_item)
            session.commit()

            # 刷新以加载关联数据
            if model_class == SyncTask:
                session.refresh(new_item, ['source_database', 'department'])
            elif hasattr(new_item, 'department'):
                session.refresh(new_item, ['department'])

            return jsonify(to_dict(new_item)), 201

        # --- 1-to-1 密钥约束 ---
        except IntegrityError as e:
            session.rollback()
            logging.error(f"Create {route_name} IntegrityError: {e}")
            error_info = str(e).lower()

            if model_class == JdyKeyInfo and (
                    'unique constraint' in error_info or 'duplicate entry' in error_info) and 'department_id' in error_info:
                return jsonify({"msg": "操作失败：该部门已存在密钥。一个部门只允许一个密钥。"}), 409

            if 'unique constraint' in error_info or 'duplicate entry' in error_info:
                return jsonify({"msg": "操作失败：违反了唯一约束（例如，名称或ID已存在）。"}), 409

            return jsonify({"msg": f"数据库完整性错误: {e}"}), 409

        except Exception as e:
            session.rollback()
            logging.error(f"Create {route_name} error: {e}")
            return jsonify({"msg": f"Error creating item: {e}"}), 500
        finally:
            pass

    @bp.route(f'/api/{route_name}/<int:id>', methods=['PUT'], endpoint=f'update_{route_name}')
    @jwt_required()
    def update_item(id):
        claims = get_current_user_claims()
        data = request.get_json()
        session = g.config_session
        try:
            pk_name = 'task_id' if model_class == SyncTask else 'id'
            item = session.query(model_class).filter(getattr(model_class, pk_name) == id).first()

            if not item:
                return jsonify({"msg": "Item not found"}), 404

            if not claims.get('is_superuser') and item.department_id != claims.get('department_id'):
                return jsonify({"msg": "Forbidden"}), 403

            # 定义不允许客户端更新的字段
            ignored_keys = {pk_name, 'id', 'created_at', 'updated_at'}

            # 动态更新字段
            for key, value in data.items():
                # 仅在字段存在于模型上且不在忽略列表时才更新
                if hasattr(item, key) and key not in ignored_keys:
                    setattr(item, key, value)

            session.commit()

            # 刷新以加载关联数据
            if model_class == SyncTask:
                session.refresh(item, ['source_database', 'department'])
            elif hasattr(item, 'department'):
                session.refresh(item, ['department'])

            return jsonify(to_dict(item)), 200
        except IntegrityError as e:  # 捕获唯一约束
            session.rollback()
            logging.error(f"Update {route_name} error: {e}")
            return jsonify({"msg": "更新失败：违反了唯一约束（例如，名称或ID已存在）。"}), 409
        except Exception as e:
            session.rollback()
            logging.error(f"Update {route_name} error: {e}")
            return jsonify({"msg": f"Error updating item: {e}"}), 500
        finally:
            pass

    @bp.route(f'/api/{route_name}/<int:id>', methods=['DELETE'], endpoint=f'delete_{route_name}')
    @jwt_required()
    def delete_item(id):
        claims = get_current_user_claims()
        session = g.config_session
        try:
            pk_name = 'task_id' if model_class == SyncTask else 'id'
            item = session.query(model_class).filter(getattr(model_class, pk_name) == id).first()

            if not item:
                return jsonify({"msg": "Item not found"}), 404

            if not claims.get('is_superuser') and item.department_id != claims.get('department_id'):
                return jsonify({"msg": "Forbidden"}), 403

            session.delete(item)
            session.commit()
            return jsonify({"msg": "Item deleted"}), 200
        except Exception as e:
            session.rollback()
            logging.error(f"Delete {route_name} error: {e}")
            if "foreign key constraint fails" in str(e).lower():
                if model_class == DatabaseInfo:
                    return jsonify({"msg": "Cannot delete: This database is being used by one or more SyncTasks."}), 409
                if model_class == Department:
                    return jsonify({
                        "msg": "Cannot delete: This department is being used by users, databases, or other resources."}), 409
            return jsonify({"msg": f"Error deleting item: {e}"}), 500
        finally:
            pass


# --- 注册通用资源 API ---
create_resource_endpoints(api_bp, DatabaseInfo, 'databases')
create_resource_endpoints(api_bp, JdyKeyInfo, 'jdy-keys')
create_resource_endpoints(api_bp, SyncTask, 'sync-tasks')


# --- SyncErrLog (错误日志) API ---
# 只有 GET 权限

@api_bp.route('/api/error-logs', methods=['GET'])
@jwt_required()
def get_error_logs():
    claims = get_current_user_claims()
    session = g.config_session
    try:
        query = session.query(SyncErrLog).options(joinedload(SyncErrLog.department))  # 预加载

        if claims.get('is_superuser'):
            logs = query.order_by(SyncErrLog.timestamp.desc()).all()
        else:
            department_id = claims.get('department_id')
            logs = query.filter_by(department_id=department_id).order_by(SyncErrLog.timestamp.desc()).all()

        return jsonify([to_dict(log) for log in logs]), 200
    finally:
        pass