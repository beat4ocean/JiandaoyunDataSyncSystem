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
    """
    Flask 应用配置类
    从环境变量中加载配置
    """

    # --- 数据库 URL  ---
    CONFIG_DB_USER = os.getenv('CONFIG_DB_USER', 'root')
    CONFIG_DB_PASSWORD = quote_plus(os.getenv('CONFIG_DB_PASSWORD', 'password'))
    CONFIG_DB_HOST = os.getenv('CONFIG_DB_HOST', 'localhost')
    CONFIG_DB_PORT = int(os.getenv('CONFIG_DB_PORT', '3306'))
    CONFIG_DB_NAME = os.getenv('CONFIG_DB_NAME', 'jdy_sync_config_db')
    CONFIG_DB_TYPE = os.getenv('CONFIG_DB_TYPE', 'mysql+pymysql')

    CONFIG_DB_URL = (
        f"{CONFIG_DB_TYPE}://{CONFIG_DB_USER}:{CONFIG_DB_PASSWORD}@"
        f"{CONFIG_DB_HOST}:{CONFIG_DB_PORT}/{CONFIG_DB_NAME}?charset=utf8mb4"
    )

    # --- 目标数据库 (Source DB) ---
    SOURCE_DB_USER = os.getenv('SOURCE_DB_USER', 'root')
    SOURCE_DB_PASSWORD = quote_plus(os.getenv('SOURCE_DB_PASSWORD', 'password'))
    SOURCE_DB_HOST = os.getenv('SOURCE_DB_HOST', 'localhost')
    SOURCE_DB_PORT = int(os.getenv('SOURCE_DB_PORT', '3306'))
    SOURCE_DB_NAME = os.getenv('SOURCE_DB_NAME', 'source_business_db')
    SOURCE_DB_TYPE = os.getenv('SOURCE_DB_TYPE', 'mysql+pymysql')

    SOURCE_DB_URL = (
        f"{SOURCE_DB_TYPE}://{SOURCE_DB_USER}:{SOURCE_DB_PASSWORD}@"
        f"{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}?charset=utf8mb4"
    )

    # 数据库连接参数
    DB_CONNECT_ARGS = {
        'connect_timeout': 10
    }

    # --- 简道云 API 配置 ---
    JDY_API_HOST = os.getenv('JDY_API_HOST', 'https://api.jiandaoyun.com')

    # --- Binlog 配置 (与目标/源数据库相同) ---
    BINLOG_MYSQL_SETTINGS = {
        'host': SOURCE_DB_HOST,
        'port': int(SOURCE_DB_PORT),
        'user': SOURCE_DB_USER,
        'passwd': os.getenv('SOURCE_DB_PASSWORD', 'password'),  # 不能使用 quote_plus()，否则可能会导致 binlog 配置错误
        'charset': 'utf8'
    }


# 导出实例 (方便 models.py 等文件导入)
CONFIG_DB_URL = Config.CONFIG_DB_URL
SOURCE_DB_URL = Config.SOURCE_DB_URL
DB_CONNECT_ARGS = Config.DB_CONNECT_ARGS
