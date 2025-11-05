# -*- coding: utf-8 -*-
import hashlib
import logging
import traceback
from datetime import time, date, datetime
from urllib.parse import quote

from flask import Blueprint, request, jsonify, g, current_app
from flask_jwt_extended import jwt_required, get_jwt, get_jwt_identity
from requests import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload, aliased

from app.auth import superuser_required
from app.jdy2db_services import handle_webhook_data
from app.jdy_api import FormApi
from app.models import (Department, User, Database, SyncErrLog)
from app.models import JdyKeyInfo, SyncTask
from app.utils import log_sync_error
from app.utils import test_db_connection

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

        # --- 2. 修复时间解析报错的bug ---
        value = getattr(model_instance, column.name)

        if isinstance(value, (datetime, date)):
            d[column.name] = value.isoformat()
        elif isinstance(value, time):
            # 将 time 对象格式化为 HH:MM:SS 字符串
            d[column.name] = value.strftime('%H:%M:%S')
        else:
            d[column.name] = value

    # --- 自动添加 department_name (如果存在) ---
    if hasattr(model_instance, 'department') and model_instance.department:
        d['department_name'] = model_instance.department.department_name

    # --- 添加关联数据库显示名称 ---
    if isinstance(model_instance, SyncTask):
        if model_instance.database:
            d['db_show_name'] = model_instance.database.db_show_name
        else:
            d['db_show_name'] = 'N/A'

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
            # id=data.get('id'),
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
        # dept.id = data.get('id', dept.id) # 不允许修改主键
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
@jwt_required()
def get_users():
    session = g.config_session
    claims = get_jwt()
    is_superuser = claims.get('is_superuser', False)

    try:
        query = session.query(User).options(joinedload(User.department))

        if is_superuser:
            # 管理员获取所有用户
            users = query.all()
        else:
            # 非管理员仅获取自己的信息
            user_id = get_jwt_identity()
            user = query.get(user_id)
            users = [user] if user else []

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
@jwt_required()
def reset_user_password(id):
    claims = get_jwt()
    is_superuser = claims.get('is_superuser', False)
    current_user_id_str = get_jwt_identity()

    # 安全检查：必须是超级管理员，或者是用户本人
    if not is_superuser and str(id) != current_user_id_str:
        return jsonify({"msg": "Forbidden: You can only reset your own password."}), 403

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

        # 根据上下文返回不同的成功消息
        if is_superuser and str(id) != current_user_id_str:
            msg = f"Password for user {user.username} has been reset."
        else:
            msg = "Your password has been successfully updated."

        return jsonify({"msg": msg}), 200
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
    --- REFACTOR: 6. 按 sync_type 过滤 ---
    """
    claims = get_current_user_claims()
    session = g.config_session

    # 获取 ?sync_type=db2jdy 或 ?sync_type=jdy2db
    sync_type_filter = request.args.get('sync_type')

    try:
        query = (session.query(Database)
                 .with_entities(Database.id, Database.db_show_name, Database.department_id, Database.sync_type))

        if claims.get('is_superuser'):
            # 超管不过滤部门
            pass
        else:
            # 普通过滤部门
            department_id = claims.get('department_id')
            query = query.filter_by(department_id=department_id)

        # 按 sync_type 过滤
        if sync_type_filter in ['db2jdy', 'jdy2db']:
            query = query.filter_by(sync_type=sync_type_filter)

        items = query.all()

        return jsonify(
            [{"id": item.id, "name": item.db_show_name, "department_id": item.department_id,
              "sync_type": item.sync_type} for item in items]), 200
    except Exception as e:
        logging.error(f"Get tenant-databases error: {e}")
        return jsonify({"msg": "Internal server error"}), 500
    finally:
        pass


# --- 通用资源 API (Database, JdyKeyInfo, SyncTask) ---

def _generate_webhook_url(task_data: dict, session: Session) -> str | None:
    """辅助函数：为 jdy2db 任务生成 webhook URL"""
    if task_data.get('sync_type') != 'jdy2db':
        return None

    try:
        # 1. 获取部门名称
        department_id = task_data.get('department_id')
        if not department_id:
            return None  # 部门ID是必须的
        department = session.get(Department, department_id)
        if not department:
            return None
        dpt_name = department.department_name

        # 2. 获取数据库显示名称
        database_id = task_data.get('database_id')
        if not database_id:
            return None
        database = session.get(Database, database_id)
        if not database:
            return None
        db_show_name = database.db_show_name

        # 3. 获取表名
        table_name = task_data.get('table_name')
        if not table_name:
            return None

        # 4. 获取服务器基地址 (从 Config 或硬编码)
        # 假设 Config.SERVER_BASE_URL 存在, e.g., "http://your_host:5000"
        # base_url = getattr(Config, 'SERVER_BASE_URL', '')
        # (使用相对路径更健壮)
        base_url = ""

        # 5. 组装 URL (使用 prompt 中要求的 'db' 参数)
        url = (f"{base_url}/webhook?"
               f"dpt={quote(dpt_name)}&"
               f"db={quote(db_show_name)}&"
               f"table={quote(table_name)}")

        return url

    except Exception as e:
        logging.error(f"Error generating webhook URL: {e}")
        return None


def create_resource_endpoints(bp, model_class, route_name):
    """
    辅助函数，为 Database, JdyKeyInfo, SyncTask 创建权限控制的 API
    """

    @bp.route(f'/api/{route_name}', methods=['GET'], endpoint=f'get_{route_name}')
    @jwt_required()
    def get_items():
        claims = get_current_user_claims()
        session = g.config_session

        # --- REFACTOR: 7. 按 sync_type 过滤 SyncTask ---
        sync_type_filter = request.args.get('sync_type')

        try:
            query = session.query(model_class)

            # --- 预加载关联数据 ---
            if model_class == SyncTask:
                query = query.options(joinedload(SyncTask.database), joinedload(SyncTask.department))
                if sync_type_filter in ['db2jdy', 'jdy2db']:
                    query = query.filter_by(sync_type=sync_type_filter)

            elif model_class == Database:
                query = query.options(joinedload(Database.department))
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
            if model_class in [Database, SyncTask, JdyKeyInfo]:
                return jsonify({"msg": "department_id is required"}), 400

        # --- 移除 'created_at' 和 'updated_at' (如果它们存在) ---
        data.pop('created_at', None)
        data.pop('updated_at', None)
        data.pop('db_show_name', None)  # 移除只读字段
        data.pop('department_name', None)  # 移除只读字段

        # --- REFACTOR: 8. Webhook URL 生成 (Create) ---
        if model_class == SyncTask and data.get('sync_type') == 'jdy2db':
            data['webhook_url'] = _generate_webhook_url(data, session)
            if not data['webhook_url']:
                logging.warning(f"Failed to generate webhook URL for new task, data missing.")

        try:
            new_item = model_class(**data)
            session.add(new_item)
            session.commit()

            # 刷新以加载关联数据
            if model_class == SyncTask:
                session.refresh(new_item, ['database', 'department'])
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
            item = session.query(model_class).filter(getattr(model_class, 'id') == id).first()

            if not item:
                return jsonify({"msg": "Item not found"}), 404

            if not claims.get('is_superuser') and item.department_id != claims.get('department_id'):
                return jsonify({"msg": "Forbidden"}), 403

            # 定义不允许客户端更新的字段
            ignored_keys = {'id', 'created_at', 'updated_at', 'db_show_name', 'department_name', 'webhook_url'}

            # 动态更新字段
            for key, value in data.items():
                # 仅在字段存在于模型上且不在忽略列表时才更新
                if hasattr(item, key) and key not in ignored_keys:
                    setattr(item, key, value)

            # --- REFACTOR: 9. Webhook URL 生成 (Update) ---
            if model_class == SyncTask and item.sync_type == 'jdy2db':
                # 需要 item 对象的字典视图来生成 URL
                item_data = to_dict(item)
                # 确保 data 中的更改已合并
                item_data.update(data)

                item.webhook_url = _generate_webhook_url(item_data, session)
                if not item.webhook_url:
                    logging.warning(f"Failed to generate webhook URL for updated task {id}, data missing.")

            session.commit()

            # 刷新以加载关联数据
            if model_class == SyncTask:
                session.refresh(item, ['database', 'department'])
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
            item = session.query(model_class).filter(getattr(model_class, 'id') == id).first()

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
                if model_class == Database:
                    return jsonify({"msg": "Cannot delete: This database is being used by one or more SyncTasks."}), 409
                if model_class == Department:
                    return jsonify({
                        "msg": "Cannot delete: This department is being used by users, databases, or other resources."}), 409
            return jsonify({"msg": f"Error deleting item: {e}"}), 500
        finally:
            pass


# --- 注册通用资源 API ---
create_resource_endpoints(api_bp, Database, 'databases')
create_resource_endpoints(api_bp, JdyKeyInfo, 'jdy-keys')
create_resource_endpoints(api_bp, SyncTask, 'sync-tasks')


# --- SyncErrLog (错误日志) API ---
# 只有 GET 权限

@api_bp.route('/api/error-logs', methods=['GET'])
@jwt_required()
def get_error_logs():
    claims = get_current_user_claims()
    session = g.config_session

    # --- REFACTOR: 10. 按 sync_type 过滤日志 ---
    sync_type_filter = request.args.get('sync_type')

    try:
        query = session.query(SyncErrLog).options(joinedload(SyncErrLog.department))  # 预加载

        if sync_type_filter in ['db2jdy', 'jdy2db']:
            query = query.filter_by(sync_type=sync_type_filter)

        if claims.get('is_superuser'):
            logs = query.order_by(SyncErrLog.timestamp.desc()).all()
        else:
            department_id = claims.get('department_id')
            logs = query.filter_by(department_id=department_id).order_by(SyncErrLog.timestamp.desc()).all()

        return jsonify([to_dict(log) for log in logs]), 200
    finally:
        pass


# --- 简道云同步数据库 增加内容开始 ---
# --- Webhook Signature Verification ---
@api_bp.before_request
def verify_signature():
    """
    在每个 Webhook 请求处理前验证签名（已重构）。
    使用 ?dpt= 参数动态获取 app_secret。
    """
    # 仅对 /webhook 路径进行验证
    if request.path != '/webhook':
        return

    # 1. 从 URL 获取部门（租户）参数
    department_name = request.args.get('dpt')
    if not department_name:
        print("签名验证失败：缺少 ?dpt= 参数。")
        return jsonify({"error": "缺少 'dpt' URL 参数"}), 400

    # 2. 从 g 对象获取配置数据库会话
    config_session = g.get('config_session')
    if not config_session:
        # If running outside request context or session wasn't opened, this can happen
        # Let's ensure session is open here if needed, though before_request in __init__ should handle it
        from app.models import ConfigSession
        g.config_session = ConfigSession()
        config_session = g.config_session
        # print("警告：在 verify_signature 中重新打开了 config_session。")
        # return jsonify({"error": "内部服务器错误 - session missing"}), 500

    # 3. 动态查询租户的 Key 和 Secret
    try:
        # --- REFACTOR: 11. 签名查询 (JdyKeyInfo) ---
        # 部门名称是唯一的
        department = config_session.query(Department).filter_by(department_name=department_name).first()
        if not department:
            print(f"签名验证失败：未找到部门 '{department_name}'。")
            return jsonify({"error": f"无效的 'dpt' 参数: {department_name}"}), 403

        key_info = config_session.query(JdyKeyInfo).filter_by(department_id=department.id).first()

    except Exception as e:
        # Rollback session if query fails
        config_session.rollback()
        print(f"签名验证期间数据库查询失败: {e}")
        return jsonify({"error": "数据库查询失败"}), 500
    # No finally block needed here as session is managed by teardown_appcontext

    if not key_info:
        print(f"签名验证失败：未找到部门 '{department_name}' 的配置信息。")
        return jsonify({"error": f"无效的 'dpt' 参数: {department_name}"}), 403

    # 4. 获取签名参数
    try:
        payload = request.data.decode('utf-8')
    except UnicodeDecodeError:
        print("签名验证失败：无法将请求体解码为 UTF-8。")
        return jsonify({"error": "请求体编码无效"}), 400

    nonce = request.args.get('nonce')
    timestamp = request.args.get('timestamp')
    signature = request.headers.get('x-jdy-signature')

    if not all([nonce, timestamp, signature]):
        print("签名验证失败：缺少 nonce, timestamp 或 x-jdy-signature。")
        return jsonify({"error": "缺少签名参数"}), 401

    # 5. 使用动态获取的 app_secret 计算签名
    app_secret = key_info.app_secret
    if not app_secret:
        print(f"签名验证失败：部门 '{department_name}' 未配置 App Secret。")
        return jsonify({"error": f"服务器内部错误 - 缺少 App Secret 配置"}), 500

    content = f"{nonce}:{payload}:{app_secret}:{timestamp}".encode('utf-8')
    expected_signature = hashlib.sha1(content).hexdigest()

    if signature != expected_signature:
        print(f"签名验证失败：签名不匹配。 部门: {department_name}")
        # print(f"Received Signature: {signature}")
        # print(f"Expected Signature: {expected_signature}")
        # print(f"Content for SHA1: {content.decode('utf-8')}") # Log content for debugging
        return jsonify({"error": "签名无效"}), 401

    # 6. 验证通过，将 key_info (包含 api_key) 存入 g 以供下游使用
    g.key_info = key_info
    print(f"签名验证通过: 部门={department_name}")


# --- Webhook Endpoint ---
@api_bp.route('/webhook', methods=['POST'])
def webhook():
    """
    接收 Webhook 请求的端点（已重构）。
    """
    # Get sessions from g, opened by before_request in __init__
    config_session = g.get('config_session')

    # --- REFACTOR: 12. 移除静态 TargetSession ---
    # target_session = g.get('target_session')
    key_info = g.get('key_info')  # 由 verify_signature 注入

    if not config_session:
        return jsonify({"status": "error", "message": "内部服务器错误 - DB session missing"}), 500

    # key_info 理论上一定存在，因为 verify_signature 会提前中止 (unless signature check is disabled)
    if not key_info:
        # This might happen if signature verification failed but wasn't properly handled,
        # or if the request didn't go through verify_signature (e.g. direct call without ?dpt=)
        print("严重错误: Webhook 处理器未获取到 key_info (可能签名验证失败或未执行)。")
        return jsonify({"status": "error", "message": "未授权或配置错误"}), 403  # Use 403 Forbidden

    payload = request.json
    data = payload.get('data')
    if not data:
        return jsonify({"status": "error", "message": "Webhook 负载中没有数据"}), 400

    # --- REFACTOR: 13. Webhook 路由逻辑 (按URL参数) ---
    department_name = request.args.get('dpt')
    db_show_name = request.args.get('db')  # 对应 'db'
    table_param = request.args.get('table')  # 对应 'table'

    if not all([department_name, db_show_name, table_param]):
        msg = "Webhook URL 缺少参数: 'dpt', 'db', 或 'table'"
        print(f"错误: {msg}")
        return jsonify({"status": "error", "message": msg}), 400

    task_config = None
    try:
        # 1. 动态获取任务配置 (根据URL参数)
        # 使用 aliased 防止 Department 和 Database 的表名冲突
        Dept = aliased(Department)
        DB = aliased(Database)

        task_config = (config_session.query(SyncTask)
                       .join(Dept, SyncTask.department_id == Dept.id)
                       .join(DB, SyncTask.database_id == DB.id)
                       .options(joinedload(SyncTask.database))  # 确保加载了 database
                       .filter(
            Dept.department_name == department_name,
            DB.db_show_name == db_show_name,
            SyncTask.table_name == table_param,
            SyncTask.sync_type == 'jdy2db'
        )
                       .first())

        # 2. 如果任务不存在 (严格匹配)
        if not task_config:
            msg = f"未找到匹配的任务: dpt={department_name}, db={db_show_name}, table={table_param}, sync_type=jdy2db"
            print(f"错误: {msg}")
            # 不自动创建任务，因为配置必须由用户预先定义
            return jsonify({"status": "error", "message": "未找到匹配的同步任务配置"}), 404

        # 3. (新) 首次 Webhook: 保存 App ID 和 Entry ID
        if not task_config.app_id or not task_config.entry_id:
            app_id, entry_id = None, None
            if isinstance(data, dict):
                app_id = data.get('appId')
                entry_id = data.get('entryId')
            if not all([app_id, entry_id]):
                app_id = payload.get('appId')
                entry_id = payload.get('entryId')

            if all([app_id, entry_id]):
                try:
                    task_config.app_id = app_id
                    task_config.entry_id = entry_id
                    config_session.commit()
                    logging.info(f"任务 {task_config.id} 已自动填充 App ID ({app_id}) 和 Entry ID ({entry_id})。")
                except IntegrityError:
                    # 捕获 uq_app_entry 唯一约束冲突
                    config_session.rollback()
                    logging.warning(f"任务 {task_config.id} 填充 App/Entry ID 失败，该组合已存在。")
                    # 重新查询正确的任务
                    task_config = config_session.query(SyncTask).options(joinedload(SyncTask.database)).filter_by(
                        app_id=app_id, entry_id=entry_id, sync_type='jdy2db').first()
                    if not task_config:
                        return jsonify({"status": "error",
                                        "message": f"App/Entry ID 组合已存在，但与当前 dpt/db/table ({department_name}/{db_show_name}/{table_param}) 不匹配。"}), 409
                except Exception as e:
                    config_session.rollback()
                    logging.error(f"任务 {task_config.id} 填充 App/Entry ID 失败: {e}")

        # 4. 动态实例化 API 客户端 (用于 create_form_fields_mapping if needed)
        api_client = FormApi(
            api_key=key_info.api_key,
            host=current_app.config['JDY_API_HOST']  # Get JDY_API_HOST from app config
        )

        # 5. 调用核心服务处理数据 (移除 target_session)
        # 核心服务 (jdy2db_services) 现在将自己负责创建动态数据库会话
        handle_webhook_data(
            config_session=config_session,
            # target_session=target_session, # (REMOVED)
            payload=payload,
            task_config=task_config,
            api_client=api_client,
            table_param=table_param  # (保留) 用于更新 task.table_name (如果需要)
        )
        # Note: handle_webhook_data now handles its own commits within its scope

        # Commit config_session changes if any were made *before* calling handle_webhook_data
        # config_session.commit() # Already committed after create_sync_task if needed
        # target_session commit is handled within handle_webhook_data

        return jsonify({"status": "success"}), 200

    except Exception as e:
        # 回滚 config_session
        try:
            config_session.rollback()
        except Exception as rb_e:
            print(f"回滚 config_session 时出错: {rb_e}")

        # (target_session 的回滚已移至 jdy2db_services 内部)
        # try:
        #     target_session.rollback()
        # except Exception as rb_e:
        #     print(f"回滚 target_session 时出错: {rb_e}")

        print(f"Webhook处理异常: {e}\n{traceback.format_exc()}")
        # 记录错误时传入动态的 task_config (might be None if task lookup failed)
        log_sync_error(
            task_config=task_config,
            error=e,
            payload=payload,
            extra_info="Error during webhook processing"
        )
        return jsonify({"status": "error", "message": "Internal Server Error"}), 500
    # No finally block needed, sessions closed by teardown_appcontext


# --- 简道云同步数据库 增加内容结束 ---

# --- “测试连接”功能路由 ---
@api_bp.route('/api/databases/test', methods=['POST'])
@jwt_required()
def test_database_connection():
    """
    测试数据库连接。
    接收与创建/更新数据库时相同的 JSON body。
    """
    claims = get_current_user_claims()
    data = request.get_json()

    # 权限检查：确保非超管用户不能测试其他租户的配置
    if not claims.get('is_superuser'):
        # # 如果提供了 department_id 且与用户不符
        # if 'department_id' in data and data.get('department_id') != claims.get('department_id'):
        #     return jsonify({"msg": "Forbidden: Cannot test connection for another tenant."}), 403

        # 强制使用自己的 department_id (虽然测试连接本身不依赖此ID)
        data['department_id'] = claims.get('department_id')

    try:
        success, message = test_db_connection(data)

        if success:
            return jsonify({"msg": message}), 200
        else:
            # 400 Bad Request 表示用户提供的参数（凭据、主机等）有误
            return jsonify({"msg": message}), 400

    except Exception as e:
        logging.error(f"Error during database connection test: {e}")
        return jsonify({"msg": f"Internal server error: {e}"}), 500
    finally:
        pass
