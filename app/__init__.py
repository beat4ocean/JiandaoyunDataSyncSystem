import re
import time
import os
from datetime import timedelta

from flask import Flask, g, abort, send_from_directory
from sqlalchemy.orm import scoped_session
from sqlalchemy.exc import OperationalError
from flask_jwt_extended import JWTManager
from flask_cors import CORS

from app.config import Config
from app.models import ConfigSession, SourceSession


def create_app():
    """
    应用工厂函数
    """
    # static_folder 指向新的 frontend 目录
    app = Flask(__name__, static_folder='../frontend', static_url_path='/')

    # 1. 加载配置
    app.config.from_object(Config)

    # --- JWT 配置 ---
    app.config["JWT_SECRET_KEY"] = os.environ.get("JWT_SECRET_KEY", "super-secret-dev-key")
    app.config["JWT_TOKEN_LOCATION"] = ["cookies"]
    app.config["JWT_ACCESS_COOKIE_PATH"] = "/"
    app.config["JWT_REFRESH_COOKIE_PATH"] = "/auth/refresh"
    app.config["JWT_COOKIE_CSRF_PROTECT"] = True
    app.config["JWT_COOKIE_SECURE"] = False  # 在生产中使用 HTTPS 时设为 True
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1)
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(days=30)
    jwt = JWTManager(app)

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

    # 2. 注册蓝图
    from app.routes import main_bp
    app.register_blueprint(main_bp)

    # 注册认证和 API 蓝图
    from app.auth import auth_bp
    app.register_blueprint(auth_bp)
    from app.api import api_bp
    app.register_blueprint(api_bp)

    # 3. (关键) 设置请求生命周期内的会话管理
    # 使用 scoped_session 确保线程安全
    config_session_scoped = scoped_session(ConfigSession)
    source_session_scoped = scoped_session(SourceSession)

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
                g.source_session = source_session_scoped()

                # type尝试轻量级查询以激活连接, 从而触发重试
                g.config_session.query(1).first()
                g.source_session.query(1).first()

                # 连接成功, 退出循环
                return

            except OperationalError as e:
                last_exception = e
                print(f"Warning: DB connection failed on attempt {attempt + 1}/3. Retrying in 2s...")
                # 发生连接错误时, 显式回滚/移除会话
                config_session_scoped.remove()
                source_session_scoped.remove()
                time.sleep(2)  # 简单延迟

        # type如果所有重试都失败
        print(f"CRITICAL: Failed to create DB session after 3 attempts: {last_exception}")
        # 抛出 503 服务不可用
        abort(503, "Database connection failed.")

    @app.teardown_request
    def close_sessions(exception=None):
        """
        在每个请求结束后关闭会话。
        """
        # 安全地移除 scoped_session
        config_session = g.pop('config_session', None)
        if config_session is not None:
            config_session_scoped.remove()

        source_session = g.pop('source_session', None)
        if source_session is not None:
            source_session_scoped.remove()

    # --- Serve Frontend ---
    # 用于服务 Vue SPA 的路由
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_frontend(path):
        if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
            # 服务静态文件 (js, css, etc.)
            return send_from_directory(app.static_folder, path)
        else:
            # 其他所有路径都返回 index.html，交由 Vue Router 处理
            return send_from_directory(app.static_folder, 'index.html')

    print("Flask App created with JWT, CORS, and SPA frontend serving.")
    return app
