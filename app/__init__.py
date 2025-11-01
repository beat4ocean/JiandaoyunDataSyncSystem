import re
import time
import os
from datetime import timedelta

from flask import Flask, g, abort, send_from_directory, jsonify
from sqlalchemy.orm import scoped_session
from sqlalchemy.exc import OperationalError
from flask_jwt_extended import JWTManager
from flask_cors import CORS

from app.config import Config
from app.models import ConfigSession, Department, User


# --- 将 Scoped Session 移至顶层，以修复回调函数中的引用错误 ---
config_session_scoped = scoped_session(ConfigSession)


def create_app():
    """
    应用工厂函数
    """
    # static_folder 指向新的 frontend 目录
    app = Flask(__name__, static_folder='../frontend', static_url_path='/')

    # 1. 加载配置
    app.config.from_object(Config)

    # --- JWT 配置 (更新为 Bearer Token) ---
    app.config["JWT_SECRET_KEY"] = os.environ.get("JWT_SECRET_KEY", "super-secret-dev-key")
    # 使用 Bearer 令牌, 而不是 cookies
    app.config["JWT_TOKEN_LOCATION"] = ["headers"]
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=24) # 缩短 Access Token
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(hours=24)  # 添加 Refresh Token

    # (移除) Cookie 相关配置
    # app.config["JWT_TOKEN_LOCATION"] = ["cookies"]
    # app.config["JWT_ACCESS_COOKIE_PATH"] = "/"
    # app.config["JWT_REFRESH_COOKIE_PATH"] = "/auth/refresh"
    # app.config["JWT_COOKIE_CSRF_PROTECT"] = True
    # app.config["JWT_COOKIE_SECURE"] = False
    # app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(days=30)

    jwt = JWTManager(app)

    # JWT 声明加载器 - 确保 User/Department 信息在每个请求中都是最新的
    @jwt.user_lookup_loader
    def user_lookup_callback(_jwt_header, jwt_data):
        """
        在 @jwt_required() 保护的路由中被调用
        确保 g.user 是最新的 User 对象
        """
        identity = jwt_data["sub"]  # 'sub' 是 user.id

        # 使用 'g' 对象上的请求会话 (由 before_request 创建)
        # 不要在此处创建或移除会话
        session = g.config_session

        if session:
            return session.query(User).get(identity)
        return None

    # @jwt.additional_claims_loader
    # def add_claims_to_access_token(identity):
    #     """
    #     在 *创建* token 时，例如 login 自动添加生命
    #     （注意：这在 app/auth.py/login 中时手动完成的，如果在那边做，这里就可以不用）
    #     确保 g.user 是最新的 User 对象
    #     """
    #     # 已在 app/auth.py/login 手动处理，这里保持为空
    #     return {}

    # --- CORS 配置 ---
    CORS(app, supports_credentials=True, origins=[
        "http://localhost:5173", "http://127.0.0.1:5173",  # 常见的 Vite/Vue 开发端口
        "http://localhost:5000", "http://127.0.0.1:5000",  # 后端服务端口

        # 保留服务器配置
        "http://0.0.0.0:5173",
        "http://0.0.0.0:5000",

        # 密码允许所有 10.x.x.x 网段的 IP (http 或 https)
        re.compile(r"^https?://10\..*")
    ])

    # 2.  # 注册认证和 API 蓝图
    from app.auth import auth_bp
    app.register_blueprint(auth_bp)

    from app.routes import api_bp
    app.register_blueprint(api_bp)  # api_bp 现在包含所有 /api/... 路由

    # # 注册一个 CLI 命令来创建第一个超管
    # @app.cli.command("create-admin")
    # def create_admin():
    #     """创建第一个管理员和默认部门"""
    #     print("Creating default department and admin user...")
    #     session = ConfigSession()
    #     try:
    #         # 1. 检查/创建默认部门
    #         default_dept = session.query(Department).filter_by(department_name="default_admin_dept").first()
    #         if not default_dept:
    #             default_dept = Department(
    #                 department_name="default_admin_dept",
    #                 is_active=True
    #             )
    #             session.add(default_dept)
    #             session.commit()
    #             print(f"Created default department (ID: {default_dept.id}).")
    #         else:
    #             print("Default department already exists.")
    #
    #         # 2. 检查/创建管理员
    #         admin_user = session.query(User).filter_by(is_superuser=True, is_active=True).first()
    #         if not admin_user:
    #             admin_username = os.environ.get("ADMIN_USER", "admin")  # 从 .env 或使用默认
    #             admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")  # 从 .env 或使用默认
    #             new_admin = User(
    #                 username=admin_username,
    #                 department_id=default_dept.id,  # 关联到默认部门
    #                 is_superuser=True,
    #                 is_active=True
    #             )
    #             new_admin.set_password(admin_password)
    #             session.add(new_admin)
    #             session.commit()
    #             # print(f"Created admin user '{admin_username}' with password '{admin_password}'.")
    #         # else:
    #         #     print("Admin user already exists.")
    #
    #     except Exception as e:
    #         session.rollback()
    #         print(f"Error creating admin: {e}")
    #     finally:
    #         session.close()

    # 3. (关键) 设置请求生命周期内的会话管理
    # --- 移除此处的 config_session_scoped 定义，因为它已移至顶层 ---
    # 使用 scoped_session 确保线程安全
    # config_session_scoped = scoped_session(ConfigSession)
    # source_session_scoped = scoped_session(SourceSession)

    @app.before_request
    def create_sessions():
        """
        在每个请求前创建数据库会话并存入 g 对象。
        增加3次连接重试
        """

        last_exception = None
        # (关键) 增加3次重试
        for attempt in range(3):
            try:
                g.config_session = config_session_scoped()
                # g.source_session = source_session_scoped()

                # type尝试轻量级查询以激活连接, 从而触发重试
                g.config_session.query(1).first()
                # g.source_session.query(1).first()

                # 连接成功, 退出循环
                return

            except OperationalError as e:
                last_exception = e
                print(f"Warning: Config DB connection failed on attempt {attempt + 1}/3. Retrying in 2s...")
                # 发生连接错误时, 显式回滚/移除会话
                config_session_scoped.remove()
                # source_session_scoped.remove()
                time.sleep(2)  # 简单延迟

        # type如果所有重试都失败
        print(f"CRITICAL: Failed to create Config DB session after 3 attempts: {last_exception}")
        abort(503, "Database connection failed.")  # 返回 JSON 错误

    @app.errorhandler(503)
    def service_unavailable(e):
        return jsonify(msg=str(e.description)), 503

    @app.teardown_request
    def close_sessions(exception=None):
        """
        在每个请求结束后关闭会话。
        """
        # 安全地移除 scoped_session
        config_session = g.pop('config_session', None)
        if config_session is not None:
            config_session_scoped.remove()

        # source_session = g.pop('source_session', None)
        # if source_session is not None:
        #     source_session_scoped.remove()

    # --- Serve Frontend ---
    # 用于服务 Vue SPA 的路由
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_frontend(path):
        # API 路由由蓝图处理, 不会进入这里
        if path.startswith('api/'):
            abort(404)

        if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
            # 服务静态文件 (js, css, etc.)
            return send_from_directory(app.static_folder, path)
        else:
            # 其他所有路径都返回 index.html
            return send_from_directory(app.static_folder, 'index.html')

    print("Flask App created with JWT (Bearer), CORS, and static frontend serving.")
    return app
