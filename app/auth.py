# -*- coding: utf-8 -*-
import logging
from functools import wraps
from flask import Blueprint, request, jsonify, g
from flask_jwt_extended import (
    create_access_token, create_refresh_token, jwt_required, get_jwt, get_jwt_identity
)
from app.models import ConfigSession, User

# 创建认证蓝图
auth_bp = Blueprint('auth_bp', __name__)


# --- 权限装饰器 ---

def superuser_required(fn):
    """
    自定义装饰器：检查JWT声明，确保用户是超级管理员。
    """

    @wraps(fn)
    @jwt_required()
    def wrapper(*args, **kwargs):
        claims = get_jwt()
        if not claims.get('is_superuser', False):
            return jsonify({"msg": "Admins only!"}), 403
        return fn(*args, **kwargs)

    return wrapper


# --- 认证路由 ---

@auth_bp.route('/api/login', methods=['POST'])
def login():
    """
    用户登录路由
    """
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({"msg": "Missing username or password"}), 400

    # --- 不在此处创建会话 ---
    # session = ConfigSession()
    # 使用 g.config_session，它由 __init__.py 的 before_request 创建
    session = g.config_session
    try:
        # !! 确保 User 模型有 department_id 和 is_superuser
        user = session.query(User).filter_by(username=username).first()

        if user and user.check_password(password):
            if not user.is_active:
                return jsonify({"msg": "User account is disabled"}), 401

            # JWT 的 identity 必须是字符串
            identity = str(user.id)

            additional_claims = {
                "user_id": user.id,
                "username": user.username,
                "is_superuser": user.is_superuser,
                "department_id": user.department_id,
                # 如果 department_id 是外键，我们可以加载 department_name
                "department_name": user.department.department_name if user.department else None
            }

            # --- 创建两种 Token ---
            access_token = create_access_token(
                identity=identity,
                additional_claims=additional_claims
            )
            refresh_token = create_refresh_token(identity=identity)

            # 返回 Token 和用户信息 (用于前端 localStorage)
            return jsonify(
                access_token=access_token,
                refresh_token=refresh_token,
                user_id=user.id,
                username=user.username,
                is_superuser=user.is_superuser,
                department_id=user.department_id,
                department_name=additional_claims["department_name"]
            ), 200
        else:
            return jsonify({"msg": "Bad username or password"}), 401

    except Exception as e:
        logging.error(f"Login error: {e}")
        return jsonify({"msg": "Internal server error"}), 500
    finally:
        # 移除 session.close()，由 teardown_request 统一处理
        pass


@auth_bp.route('/api/users/change-password', methods=['PATCH'])
@jwt_required()
def change_password():
    """
    非超管用户修改自己的密码
    """
    data = request.get_json()
    old_password = data.get('old_password')
    new_password = data.get('new_password')

    if not old_password or not new_password:
        return jsonify({"msg": "Missing old or new password"}), 400

    # 从 JWT 的 identity 中获取 user_id (这将是一个字符串)
    user_id = get_jwt_identity()
    if not user_id:
        return jsonify({"msg": "Invalid token identity"}), 401

    session = g.config_session  # 使用 g.config_session
    try:
        # session.get() 可以处理字符串 '1'
        user = session.query(User).get(user_id)
        if not user:
            return jsonify({"msg": "User not found"}), 404

        # 检查是否是超管在修改自己的密码 (超管也应通过此路由修改自己的密码)
        # 检查旧密码
        if user.check_password(old_password):
            user.set_password(new_password)
            session.commit()
            return jsonify({"msg": "Password updated successfully"}), 200
        else:
            return jsonify({"msg": "Invalid old password"}), 401

    except Exception as e:
        session.rollback()
        logging.error(f"Change password error: {e}")
        return jsonify({"msg": "Internal server error"}), 500
    finally:
        pass  # 移除 session.close()


# --- Refresh 路由 ---
@auth_bp.route('/api/refresh', methods=['POST'])
@jwt_required(refresh=True)  # 关键：只允许 Refresh Token 访问
def refresh():
    """
    使用有效的 Refresh Token 获取一个新的 Access Token
    """
    # get_jwt_identity() 将返回创建 refresh_token 时使用的字符串 identity
    identity = get_jwt_identity()
    session = g.config_session  # 使用 g.config_session
    try:
        # 重新获取用户信息来填充 Access Token 的 claims
        user = session.query(User).get(identity)
        if not user or not user.is_active:
            return jsonify({"msg": "User not found or inactive"}), 401

        additional_claims = {
            "user_id": user.id,
            "username": user.username,
            "is_superuser": user.is_superuser,
            "department_id": user.department_id,
            "department_name": user.department.department_name if user.department else None
        }

        # 只创建一个新的 Access Token
        # 'identity' 已经是我们需要的字符串
        new_access_token = create_access_token(
            identity=identity,
            additional_claims=additional_claims
        )

        return jsonify(access_token=new_access_token), 200
    except Exception as e:
        logging.error(f"Refresh error: {e}")
        return jsonify({"msg": "Internal server error"}), 500
    finally:
        pass  # 移除 session.close()

# /check_auth (检查身份) 不需要： Flask-JWT-Extended 插件通过 @jwt_required() 装饰器自动完成了身份检查
# /logout (登出) 不需要： 因为JWT Token（令牌）被存储在前端（浏览器的 localStorage）中，登出操作由前端删除 localStorage 中的 token 实现