import logging
from flask import Flask
from app.config import Config
from app.models import db
from app.routes import main_bp


def create_app():
    """
    应用工厂函数
    """
    app = Flask(__name__)

    # 1. 加载配置
    app.config.from_object(Config)

    # 2. 配置日志
    logging.basicConfig(level=logging.INFO)
    app.logger.setLevel(logging.INFO)

    # 3. 初始化数据库
    db.init_app(app)

    # 4. 注册蓝图
    app.register_blueprint(main_bp)

    app.logger.info("Flask App created successfully.")

    return app
