import atexit
import sys

from flask import Flask
from sqlalchemy import text, inspect, create_engine
from sqlalchemy.exc import OperationalError, NoSuchTableError
from sqlalchemy.orm import scoped_session

from app import create_app
from app.config import Config, CONFIG_DB_URL, CONFIG_DB_NAME, SOURCE_DB_NAME
from app.models import (
    ConfigSession, config_engine, source_engine, config_metadata, SyncTask,
    source_metadata
)
from app.scheduler import scheduler, start_scheduler
from app.utils import log_sync_error


def initialize_databases(app: Flask):
    """
    初始化数据库：
    1. 创建数据库 (config 和 source)，如果它们尚不存在。
    2. 在 config 数据库中创建所有表 (例如 sync_tasks)。
    """
    with app.app_context():  # 进入Flask应用上下文
        print("Initializing databases...")  # 打印初始化信息

        # 1. 创建一个"根"连接（不指定数据库名称），用于创建数据库
        try:
            # 密码在 Config.CONFIG_DB_PASSWORD 中已经是URL编码的
            admin_db_url = (
                f"mysql+pymysql://{Config.CONFIG_DB_USER}:{Config.CONFIG_DB_PASSWORD}@"
                f"{Config.CONFIG_DB_HOST}:{Config.CONFIG_DB_PORT}?charset=utf8mb4"
            )
            # 创建一个临时的管理引擎
            admin_engine = create_engine(admin_db_url, connect_args=Config.DB_CONNECT_ARGS)

            with admin_engine.connect() as connection:  # 建立连接
                # 检查并创建配置数据库
                print(f"Checking/Creating config database: {CONFIG_DB_NAME}")
                connection.execute(text(
                    f"CREATE DATABASE IF NOT EXISTS `{CONFIG_DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                ))
                # connection.commit() # CREATE DATABASE 通常会自动提交

                # 检查并创建源数据库
                print(f"Checking/Creating source database: {SOURCE_DB_NAME}")
                connection.execute(text(
                    f"CREATE DATABASE IF NOT EXISTS `{SOURCE_DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                ))
                # connection.commit()

            print("Database existence check complete.")  # 数据库存在性检查完成

        except OperationalError as e:  # 捕获操作错误（如连接失败）
            # 单独处理访问拒绝错误（例如密码错误）
            if "Access denied" in str(e):
                print(
                    f"CRITICAL: Failed to connect to MySQL server. Check credentials for user '{Config.CONFIG_DB_USER}'. Error: {e}")
            else:
                print(f"CRITICAL: Failed to connect to MySQL server or create databases: {e}")
            print("Please check MySQL connection settings in .env and user permissions (CREATE DATABASE).")
            raise  # 抛出异常，中断执行

        except Exception as e:  # 捕获其他意外错误
            print(f"CRITICAL: An unexpected error occurred while creating databases: {e}")
            raise

        # 2. 数据库已存在，现在在 *配置* 数据库中创建表
        try:
            # config_engine (来自 models.py) 连接到 CONFIG_DB_NAME
            config_metadata.create_all(config_engine)  # 创建所有在 config_metadata 中定义的表
            print("Config database tables checked/created.")  # 配置数据库表检查/创建完毕
        except Exception as e:
            print(f"CRITICAL: Failed to create config database tables: {e}")
            raise

        # 3. (可选) 在 *源* 数据库中创建表，如果它们在 source_metadata 中有定义
        # 在您的情况下，app/models.py 没有为 source_metadata 定义模型，
        # 所以这里什么也不会做，但这是一个正确的做法。
        try:
            source_metadata.create_all(source_engine)  # 创建所有在 source_metadata 中定义的表
            print("Source database tables checked/created (if any were defined).")
        except Exception as e:
            print(f"WARNING: Failed to check/create source database tables: {e}")
            # 不认为这是一个严重错误，因为源数据库通常已经存在
            pass

        # 4. 检查源表中的列
        # 这个逻辑现在在 services.SyncService._prepare_source_table() 中执行

        # # 2. 检查并修改源库表
        # print("Checking source tables for `_id` column...")
        #
        # # 需要一个 config session 来读取任务
        # config_session = scoped_session(ConfigSession)
        # try:
        #     tasks = config_session.query(SyncTask).all()
        #     db_name = Config.SOURCE_DB_NAME  # (已修复: 使用 SOURCE_DB_NAME)
        #
        #     with source_engine.connect() as source_conn:
        #         for task in tasks:
        #             if not task.source_table:
        #                 continue
        #
        #             try:
        #                 # 检查表是否存在
        #                 inspector = inspect(source_engine)
        #                 if not inspector.has_table(task.source_table):
        #                     print(
        #                         f"Warning: Table '{task.source_table}' for task {task.task_id} not found in source DB.")
        #                     continue
        #
        #                 # 检查是否为物理表 (BASE TABLE), 而不是视图 (VIEW)
        #                 table_type_query = text(
        #                     "SELECT table_type FROM information_schema.tables "
        #                     "WHERE table_schema = :db_name AND table_name = :table_name"
        #                 )
        #                 result = source_conn.execute(table_type_query,
        #                                              {"db_name": db_name, "table_name": task.source_table}).fetchone()
        #
        #                 table_type = result[0] if result else None
        #
        #                 if table_type == 'VIEW':
        #                     print(f"Skipping `_id` check for VIEW: '{task.source_table}'")
        #                     continue
        #
        #                 if table_type != 'BASE TABLE':
        #                     print(
        #                         f"Warning: Skipping `_id` check for unknown table type '{table_type}': '{task.source_table}'")
        #                     continue
        #
        #                 # 检查 `_id` 列是否存在
        #                 columns = [col['name'] for col in inspector.get_columns(task.source_table)]
        #                 if '_id' not in columns:
        #                     print(f"Adding `_id` column to table '{task.source_table}'...")
        #                     try:
        #                         # 为 _id 添加 varchar(50) 和索引
        #                         source_conn.execute(text(
        #                             f"ALTER TABLE `{task.source_table}` ADD COLUMN `_id` VARCHAR(50) NULL DEFAULT NULL"))
        #                         source_conn.execute(
        #                             text(f"ALTER TABLE `{task.source_table}` ADD INDEX `idx__id` (`_id`)"))
        #                         source_conn.commit()  # 提交 DDL
        #                         print(f"Successfully added `_id` column and index to '{task.source_table}'.")
        #                     except Exception as alter_e:
        #                         source_conn.rollback()  # 回滚 DDL
        #                         print(f"ERROR: Failed to add `_id` column to '{task.source_table}': {alter_e}")
        #
        #             except NoSuchTableError:
        #                 print(
        #                     f"Warning: Table '{task.source_table}' for task {task.task_id} not found (NoSuchTableError).")
        #             except Exception as task_e:
        #                 print(f"Error processing table for task {task.task_id} ('{task.source_table}'): {task_e}")
        #
        # except OperationalError as e:
        #     print(f"ERROR: Cannot connect to databases during initialization: {e}")
        # except Exception as e:
        #     print(f"ERROR: Failed during database initialization: {e}")
        # finally:
        #     config_session.remove()
        #
        # print("Database initialization complete.")

        print("Database initialization complete. Source table checks will run with each task.")


def shutdown_scheduler():
    """在应用退出时关闭调度器"""
    print("Shutting down scheduler...")
    if scheduler.running:  # 检查调度器是否在运行
        scheduler.shutdown()  # 关闭调度器
    print("Scheduler shut down.")


# --- 应用启动入口 ---
if __name__ == "__main__":
    app = create_app()  # 创建Flask应用实例

    # 1. 初始化数据库
    initialize_databases(app)

    # 2. 注册一个钩子，在程序退出时调用 shutdown_scheduler
    atexit.register(shutdown_scheduler)

    # 3. 启动调度器
    start_scheduler(app)

    # 4. 启动 Flask Web 服务器
    # 注意：在生产环境中，应使用 Gunicorn 或 uWSGI
    print("Starting Flask web server...")
    try:
        app.run(host='0.0.0.0', port=5000)  # 监听所有接口的5000端口
    except (KeyboardInterrupt, SystemExit):  # 捕获Ctrl+C或系统退出信号
        print("Flask server received shutdown signal.")
    finally:
        # 确保调度器在 Ctrl+C 时也能关闭
        if scheduler.running:
            scheduler.shutdown()
        print("Application exiting.")
