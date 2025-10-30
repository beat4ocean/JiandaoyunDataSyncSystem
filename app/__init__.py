from flask import Flask, g
from sqlalchemy.orm import scoped_session

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
        """
        g.config_session = config_session_scoped()
        g.source_session = source_session_scoped()

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

    print("Flask App created with manual session management.")
    return app
