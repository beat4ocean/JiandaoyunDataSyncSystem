# -*- coding: utf-8 -*-
from flask import Blueprint, request, jsonify, g
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity, create_refresh_token, \
    set_access_cookies, set_refresh_cookies, unset_jwt_cookies

from app.models import User, ConfigSession

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')


@auth_bp.route('/register', methods=['POST'])
def register():
    """Register a new user (for initial setup primarily)."""
    # In a real app, you might want to restrict this endpoint
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({"msg": "Missing username or password"}), 400

    session = ConfigSession()
    try:
        if session.query(User).filter_by(username=username).first():
            return jsonify({"msg": "Username already exists"}), 400

        new_user = User(username=username)
        new_user.set_password(password)
        session.add(new_user)
        session.commit()
        return jsonify({"msg": "User registered successfully"}), 201
    except Exception as e:
        session.rollback()
        print(f"Error during registration: {e}")
        return jsonify({"msg": "Internal server error during registration"}), 500
    finally:
        session.close()


@auth_bp.route('/login', methods=['POST'])
def login():
    """Login user and return JWT tokens."""
    username = request.json.get('username', None)
    password = request.json.get('password', None)

    session = ConfigSession()
    try:
        user = session.query(User).filter_by(username=username).first()
        if user and user.check_password(password):
            access_token = create_access_token(identity=username)
            refresh_token = create_refresh_token(identity=username)  # Optional: for longer sessions

            response = jsonify({"msg": "Login successful"})
            # Set tokens in HttpOnly cookies for security
            set_access_cookies(response, access_token)
            set_refresh_cookies(response, refresh_token)  # Optional
            return response, 200
        else:
            return jsonify({"msg": "Bad username or password"}), 401
    except Exception as e:
        print(f"Error during login: {e}")
        return jsonify({"msg": "Internal server error during login"}), 500
    finally:
        session.close()


@auth_bp.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    """Refresh access token using refresh token."""
    identity = get_jwt_identity()
    access_token = create_access_token(identity=identity)
    response = jsonify(access_token=access_token)
    set_access_cookies(response, access_token)
    return response


@auth_bp.route('/logout', methods=['POST'])
def logout():
    """Logout user by unsetting JWT cookies."""
    response = jsonify({"msg": "Logout successful"})
    unset_jwt_cookies(response)
    return response, 200


@auth_bp.route('/check_auth', methods=['GET'])
@jwt_required()
def check_auth():
    """Endpoint to check if the user is currently authenticated."""
    # If @jwt_required passes, the user is authenticated
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user), 200
