import os
from dotenv import load_dotenv

# 加载 .env 文件 (如果存在)
load_dotenv()


class Config:
    """
    Flask 应用配置类
    从环境变量中加载配置
    """

    # 密钥，用于 session 等
    SECRET_KEY = os.getenv('SECRET_KEY', 'a_very_secret_key_fallback')

    # --- 配置数据库 (Config DB - 用于存储任务、日志) ---
    CONFIG_DB_TYPE = os.getenv('CONFIG_DB_TYPE', 'mysql+pymysql')
    CONFIG_DB_USER = os.getenv('CONFIG_DB_USER', 'root')
    CONFIG_DB_PASS = os.getenv('CONFIG_DB_PASS', 'password')
    CONFIG_DB_HOST = os.getenv('CONFIG_DB_HOST', 'localhost')
    CONFIG_DB_PORT = os.getenv('CONFIG_DB_PORT', '3306')
    CONFIG_DB_NAME = os.getenv('CONFIG_DB_NAME', 'jdy_sync_config_db')

    # SQLAlchemy 主数据库 URI
    SQLALCHEMY_DATABASE_URI = (
        f"{CONFIG_DB_TYPE}://"
        f"{CONFIG_DB_USER}:{CONFIG_DB_PASS}@"
        f"{CONFIG_DB_HOST}:{CONFIG_DB_PORT}/{CONFIG_DB_NAME}"
    )

    # --- 源数据库 (Source DB - 业务数据来源) ---
    SOURCE_DB_TYPE = os.getenv('SOURCE_DB_TYPE', 'mysql+pymysql')
    SOURCE_DB_USER = os.getenv('SOURCE_DB_USER', 'root')
    SOURCE_DB_PASS = os.getenv('SOURCE_DB_PASS', 'password')
    SOURCE_DB_HOST = os.getenv('SOURCE_DB_HOST', 'localhost')
    SOURCE_DB_PORT = os.getenv('SOURCE_DB_PORT', '3306')
    SOURCE_DB_NAME = os.getenv('SOURCE_DB_NAME', 'source_business_db')

    # 使用 SQLALCHEMY_BINDS 来配置多个数据库
    # 'source_db' 是我们给源数据库绑定的键
    SQLALCHEMY_BINDS = {
        'source_db': (
            f"{SOURCE_DB_TYPE}://"
            f"{SOURCE_DB_USER}:{SOURCE_DB_PASS}@"
            f"{CONFIG_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
        )
    }

    # --- 简道云 API 配置 ---
    JDY_APP_ID = os.getenv('JDY_APP_ID', '')
    JDY_ENTRY_ID = os.getenv('JDY_ENTRY_ID', '')
    JDY_API_KEY = os.getenv('JDY_API_KEY', '')

    # --- 企微通知 (可选) ---
    WECOM_BOT_KEY = os.getenv('WECOM_BOT_KEY', '')

    # --- Binlog 配置 ---
    # Binlog 连接设置 (与源数据库相同)
    BINLOG_MYSQL_SETTINGS = {
        'host': SOURCE_DB_HOST,
        'port': int(SOURCE_DB_PORT),
        'user': SOURCE_DB_USER,
        'passwd': SOURCE_DB_PASS
    }

    # --- SQLAlchemy 配置 ---
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False  # 设为 True 可查看所有 SQL 查询
