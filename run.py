import atexit
import os
import sys
import traceback
from datetime import datetime, timedelta

import dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from waitress import serve

from app.config import DB_CONNECT_ARGS, Config

# 确保 app 目录在 sys.path 中
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

dotenv.load_dotenv()

try:
    from app import create_app
    from app.models import (
        config_engine, config_metadata, ConfigSession, User, Department
    )
    from app.scheduler import scheduler, start_scheduler, refresh_scheduler
    from app.utils import log_sync_error, TZ_UTC_8
except ImportError as e:
    print(f"启动失败：无法导入应用模块。请确保 app 目录和所有文件都存在。 {e}")
    print(traceback.format_exc())
    sys.exit(1)


def initialize_databases(app: Flask):
    """
    初始化数据库：
    1. 创建数据库 (config)，如果它尚不存在。
    2. 在 config 数据库中创建所有表。
    """
    with app.app_context():  # 进入Flask应用上下文
        print("Initializing databases...")

        # 1. 创建一个"根"连接（不指定数据库名称），用于创建数据库
        try:
            admin_engine = create_engine(
                f"mysql+pymysql://{Config.CONFIG_DB_USER}:{Config.CONFIG_DB_PASSWORD}@{Config.CONFIG_DB_HOST}:{Config.CONFIG_DB_PORT}/?charset=utf8mb4",
                connect_args=DB_CONNECT_ARGS)

            with admin_engine.connect() as connection:
                # 检查并创建配置数据库
                print(f"Checking/Creating config database: {Config.CONFIG_DB_NAME}")
                connection.execute(text(
                    f"CREATE DATABASE IF NOT EXISTS `{Config.CONFIG_DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                ))
                # # 检查并创建源数据库
                # print(f"Checking/Creating source database: {SOURCE_DB_NAME}")
                # connection.execute(text(
                #     f"CREATE DATABASE IF NOT EXISTS `{SOURCE_DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                # ))
            admin_engine.dispose()
            print("Config Database existence check complete.")

        except OperationalError as e:
            if "Access denied" in str(e):
                print(
                    f"CRITICAL: Failed to connect to MySQL server. Check credentials for user '{Config.CONFIG_DB_USER}'. Error: {e}")
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

        # # 3. 在 *源* 数据库中创建表
        # try:
        #     source_metadata.create_all(source_engine)
        #     print("Source database tables checked/created (if any were defined).")
        # except Exception as e:
        #     print(f"WARNING: Failed to check/create source database tables: {e}")
        #     pass

        print("Database initialization complete.")


def create_first_admin():
    """创建第一个管理员和默认部门"""
    print("Creating default department and admin user...")
    session = ConfigSession()
    try:
        # 1. 检查/创建默认部门
        default_dept = session.query(Department).filter_by(department_name="default_admin_dept").first()
        if not default_dept:
            default_dept = Department(
                department_name="default_admin_dept",
                is_active=True
            )
            session.add(default_dept)
            session.commit()
            print(f"Created default department (ID: {default_dept.id}).")
        else:
            print("Default department already exists.")

        # 2. 检查/创建管理员
        admin_user = session.query(User).filter_by(is_superuser=True, is_active=True).first()
        if not admin_user:
            admin_username = os.environ.get("ADMIN_USER", "admin")  # 从 .env 或使用默认
            admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")  # 从 .env 或使用默认
            new_admin = User(
                username=admin_username,
                department_id=default_dept.id,  # 关联到默认部门
                is_superuser=True,
                is_active=True
            )
            new_admin.set_password(admin_password)
            session.add(new_admin)
            session.commit()
            # print(f"Created admin user '{admin_username}' with password '{admin_password}'.")
        # else:
        #     print("Admin user already exists.")

    except Exception as e:
        session.rollback()
        print(f"Error creating admin: {e}")
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
        create_first_admin()

    # 3. 注册一个钩子，在程序退出时调用 shutdown_scheduler
    atexit.register(shutdown_scheduler)

    # 4. 启动调度器
    # 4.1. 添加 BINLOG 监听器管理器
    # 4.2. 添加字段映射刷新器
    start_scheduler(app)

    # 5、将定时刷新任务作业 *添加* 到主调度器
    try:
        # 使用从 app.scheduler 导入的 *同一个* 调度器实例
        scheduler.add_job(
            refresh_scheduler,
            'interval',
            minutes=Config.CHECK_INTERVAL_MINUTES,
            args=[app],
            id='task_refresher',  # 添加一个唯一的 ID
            replace_existing=True,
            misfire_grace_time=60,
            # 15秒后启动, 避开其他启动任务
            next_run_time=datetime.now(TZ_UTC_8) + timedelta(seconds=15)
        )

        print(f"Task refresher job added to main scheduler, runs every {Config.CHECK_INTERVAL_MINUTES} minutes.")

    except Exception as e:
        print(f"启动调度器失败: {e}")
        log_sync_error(error=e, extra_info="Failed to start APScheduler")
        sys.exit(1)

    # 6. 启动 Flask Web 服务器 (使用 Waitress)
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
