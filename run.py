import atexit
import os
import sys
import traceback

from flask import Flask
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from waitress import serve

# 确保 app 目录在 sys.path 中
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

try:
    from app import create_app
    from app.config import (
        Config, CONFIG_DB_URL, CONFIG_DB_NAME, SOURCE_DB_NAME,
        CONFIG_DB_USER, CONFIG_DB_PASSWORD, CONFIG_DB_HOST, CONFIG_DB_PORT,
        SOURCE_DB_USER, SOURCE_DB_PASSWORD, SOURCE_DB_HOST, SOURCE_DB_PORT,
        DB_CONNECT_ARGS
    )
    from app.models import (
        config_engine, source_engine, config_metadata, source_metadata,
        ConfigSession, User
    )
    from app.scheduler import scheduler, start_scheduler
    from app.utils import log_sync_error
except ImportError as e:
    print(f"启动失败：无法导入应用模块。请确保 app 目录和所有文件都存在。 {e}")
    print(traceback.format_exc())
    sys.exit(1)


def initialize_databases(app: Flask):
    """
    初始化数据库：
    1. 创建数据库 (config 和 source)，如果它们尚不存在。
    2. 在 config 数据库中创建所有表 (sync_tasks, users, jdy_key_info)。
    """
    with app.app_context():  # 进入Flask应用上下文
        print("Initializing databases...")

        # 1. 创建一个"根"连接（不指定数据库名称），用于创建数据库
        try:
            # (Config.CONFIG_DB_PASSWORD 已经是 URL 编码的)
            admin_db_url = (
                f"mysql+pymysql://{Config.CONFIG_DB_USER}:{Config.CONFIG_DB_PASSWORD}@"
                f"{Config.CONFIG_DB_HOST}:{Config.CONFIG_DB_PORT}?charset=utf8mb4"
            )
            # 使用 DB_CONNECT_ARGS
            admin_engine = create_engine(admin_db_url, connect_args=DB_CONNECT_ARGS)

            with admin_engine.connect() as connection:
                # 检查并创建配置数据库
                print(f"Checking/Creating config database: {CONFIG_DB_NAME}")
                connection.execute(text(
                    f"CREATE DATABASE IF NOT EXISTS `{CONFIG_DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                ))
                # 检查并创建源数据库
                print(f"Checking/Creating source database: {SOURCE_DB_NAME}")
                connection.execute(text(
                    f"CREATE DATABASE IF NOT EXISTS `{SOURCE_DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                ))
            admin_engine.dispose()
            print("Database existence check complete.")

        except OperationalError as e:
            if "Access denied" in str(e):
                print(
                    f"CRITICAL: Failed to connect to MySQL server. Check credentials for user '{CONFIG_DB_USER}'. Error: {e}")
            else:
                print(f"CRITICAL: Failed to connect to MySQL server or create databases: {e}")
            print("Please check MySQL connection settings in .env and user permissions (CREATE DATABASE).")
            raise
        except Exception as e:
            print(f"CRITICAL: An unexpected error occurred while creating databases: {e}")
            raise

        # 2. 在 *配置* 数据库中创建所有表
        try:
            config_metadata.create_all(config_engine)
            print("Config database tables checked/created.")
        except Exception as e:
            print(f"CRITICAL: Failed to create config database tables: {e}")
            raise

        # 3. 在 *源* 数据库中创建表 (如果定义了)
        try:
            source_metadata.create_all(source_engine)
            print("Source database tables checked/created (if any were defined).")
        except Exception as e:
            print(f"WARNING: Failed to check/create source database tables: {e}")
            pass

        print("Database initialization complete. Source table checks will run with each task.")


def create_first_admin(app: Flask):
    """
    检查是否已有用户，如果没有，则根据 .env 文件创建第一个管理员。
    """
    admin_user = os.environ.get("ADMIN_USER")
    admin_pass = os.environ.get("ADMIN_PASS")

    if not admin_user or not admin_pass:
        print("警告：未在 .env 文件中设置 ADMIN_USER 或 ADMIN_PASS。")
        print("如果这是第一次启动，你将无法登录。")
        return

    session = ConfigSession()
    try:
        # 检查是否已存在任何用户
        user_exists = session.query(User).first()
        if not user_exists:
            print(f"未找到任何用户，正在创建第一个管理员: {admin_user}")
            new_admin = User(username=admin_user)
            new_admin.set_password(admin_pass)
            session.add(new_admin)
            session.commit()
            print(f"管理员 '{admin_user}' 创建成功。")
        else:
            print("数据库中已存在用户，跳过创建管理员。")
    except Exception as e:
        session.rollback()
        print(f"创建第一个管理员时出错: {e}")
    finally:
        session.close()


def shutdown_scheduler():
    """在应用退出时关闭调度器"""
    print("Shutting down scheduler...")
    if scheduler.running:
        scheduler.shutdown()
    print("Scheduler shut down.")


# --- 应用启动入口 ---
if __name__ == "__main__":
    app = create_app()

    with app.app_context():
        # 1. 初始化数据库
        initialize_databases(app)
        # 2. 检查并创建第一个管理员
        create_first_admin(app)

    # 3. 注册一个钩子，在程序退出时调用 shutdown_scheduler
    atexit.register(shutdown_scheduler)

    # 4. 启动调度器
    start_scheduler(app)

    # 5. 启动 Flask Web 服务器 (使用 Waitress)
    print("Starting Flask web server with Waitress on http://0.0.0.0:5000...")
    try:
        # 我们将保持 5000 端口，并确保前端也使用 5000。
        serve(app, host='0.0.0.0', port=5000, threads=4)
    except (KeyboardInterrupt, SystemExit):
        print("Flask server received shutdown signal.")
    finally:
        # 确保调度器在 Ctrl+C 时也能关闭
        if scheduler.running:
            scheduler.shutdown()
        print("Application exiting.")
