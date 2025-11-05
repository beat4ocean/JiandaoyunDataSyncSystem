import os
from urllib.parse import quote_plus

from dotenv import load_dotenv

# 加载 .env 文件
basedir = os.path.abspath(os.path.dirname(__file__))
env_path = os.path.join(basedir, '..', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    print(f"Warning: .env file not found at {env_path}")


class Config:
    # --- 数据库连接 (从 .env 读取) ---

    # 1. 配置数据库 (存储任务、用户、日志、数据库配置)
    CONFIG_DB_USER = os.getenv('CONFIG_DB_USER', 'root')
    CONFIG_DB_PASSWORD = quote_plus(os.getenv('CONFIG_DB_PASSWORD', 'password'))
    CONFIG_DB_HOST = os.getenv('CONFIG_DB_HOST', 'localhost')
    CONFIG_DB_PORT = int(os.getenv('CONFIG_DB_PORT', '3306'))
    CONFIG_DB_NAME = os.getenv('CONFIG_DB_NAME', 'jdy_sync_config')

    # # 2. 目标/源 数据库 (存储业务数据) - (已重命名为 SOURCE)
    # SOURCE_DB_USER = os.getenv('SOURCE_DB_USER', 'root')
    # SOURCE_DB_PASSWORD = quote_plus(os.getenv('SOURCE_DB_PASSWORD', 'password'))
    # SOURCE_DB_HOST = os.getenv('SOURCE_DB_HOST', 'localhost')
    # SOURCE_DB_PORT = int(os.getenv('SOURCE_DB_PORT', '3306'))
    # # db2jdy_services.py 需要 (Binlog 和 _is_view)
    # SOURCE_DB_NAME = os.getenv('SOURCE_DB_NAME', 'db_show_name')
    #
    # # 3. Binlog 专用连接 (通常是只读副本)
    # BINLOG_DB_USER = os.getenv('BINLOG_DB_USER', 'binlog_user')
    # BINLOG_DB_PASSWORD = os.getenv('BINLOG_DB_PASSWORD', 'binlog_pass')
    # BINLOG_DB_HOST = os.getenv('BINLOG_DB_HOST', 'localhost')
    # BINLOG_DB_PORT = int(os.getenv('BINLOG_DB_PORT', '3306'))

    # --- 数据库连接字符串 ---
    CONFIG_DB_URL = (
        f"mysql+pymysql://{CONFIG_DB_USER}:{CONFIG_DB_PASSWORD}@{CONFIG_DB_HOST}:{CONFIG_DB_PORT}/{CONFIG_DB_NAME}?charset=utf8mb4"
    )

    # SOURCE_DB_URL = (
    #     f"mysql+pymysql://{SOURCE_DB_USER}:{SOURCE_DB_PASSWORD}@"
    #     f"{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}?charset=utf8mb4"
    # )

    # 数据库连接参数
    DB_CONNECT_ARGS = {
        "connect_timeout": 20
    }

    # # Binlog 读取器设置
    # BINLOG_MYSQL_SETTINGS = {
    #     "host": BINLOG_DB_HOST,
    #     "port": int(BINLOG_DB_PORT),
    #     "user": BINLOG_DB_USER,
    #     "passwd": BINLOG_DB_PASSWORD
    # }

    # --- 简道云 API 配置 ---
    JDY_API_HOST = os.getenv('JDY_API_HOST', 'https://api.jiandaoyun.com')

    # --- 调度器配置 ---
    CHECK_INTERVAL_MINUTES = int(os.getenv('CHECK_INTERVAL_MINUTES', 1))
    CACHE_REFRESH_INTERVAL_MINUTES = int(os.getenv('CACHE_REFRESH_INTERVAL_MINUTES', 5))


# 导出实例
CONFIG_DB_URL = Config.CONFIG_DB_URL
# CONFIG_DB_USER = Config.CONFIG_DB_USER
# CONFIG_DB_NAME = Config.CONFIG_DB_NAME

# SOURCE_DB_URL = Config.SOURCE_DB_URL
# SOURCE_DB_NAME = Config.SOURCE_DB_NAME

DB_CONNECT_ARGS = Config.DB_CONNECT_ARGS

# JDY_API_HOST = Config.JDY_API_HOST
# BINLOG_MYSQL_SETTINGS = Config.BINLOG_MYSQL_SETTINGS
# CHECK_INTERVAL_MINUTES = Config.CHECK_INTERVAL_MINUTES
