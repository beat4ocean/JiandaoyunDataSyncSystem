# -*- coding: utf-8 -*-
import logging
from functools import wraps
from flask import Blueprint, request, jsonify
from flask_jwt_extended import (
    create_access_token, jwt_required, get_jwt, get_jwt_identity
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

    session = ConfigSession()
    try:
        # !! 确保 User 模型有 department_id 和 is_superuser
        user = session.query(User).filter_by(username=username).first()

        if user and user.check_password(password):
            if not user.is_active:
                return jsonify({"msg": "User account is disabled"}), 401

            # 创建 JWT 声明 (claims)
            # 我们将 user.id 作为 identity，以便于后续操作
            identity = user.id
            additional_claims = {
                "user_id": user.id,
                "username": user.username,
                "is_superuser": user.is_superuser,
                "department_id": user.department_id,
                # 如果 department_id 是外键，我们可以加载 department_name
                "department_name": user.department.department_name if user.department else None
            }

            access_token = create_access_token(
                identity=identity,
                additional_claims=additional_claims
            )

            # 返回 Token 和用户信息 (用于前端 localStorage)
            return jsonify(
                access_token=access_token,
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
        session.close()


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

    # 从 JWT 的 identity 中获取 user_id
    user_id = get_jwt_identity()
    if not user_id:
        return jsonify({"msg": "Invalid token identity"}), 401

    session = ConfigSession()
    try:
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
        session.close()

# 不需要 /logout, /refresh, /check_auth 路由
# 登出操作由前端删除 localStorage 中的 token 实现
# Token 刷新逻辑也可以在前端实现，但为简化，此版本不包含
# 身份检查通过 @jwt_required() 在每个受保护的 API 上自动完成
