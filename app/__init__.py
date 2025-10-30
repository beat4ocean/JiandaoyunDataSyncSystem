import time

from flask import Flask, g, abort
from sqlalchemy.orm import scoped_session
from sqlalchemy.exc import OperationalError

from app.config import Config
from app.models import ConfigSession, SourceSession


def create_app():
    """
    应用工厂函数
    """
    app = Flask(__name__)

    # 1. 加载配置
    app.config.from_object(Config)

    # 2. 注册蓝图
    from app.routes import main_bp
    app.register_blueprint(main_bp)

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

    print("Flask App created with manual session management and connection retry.")
    return app
