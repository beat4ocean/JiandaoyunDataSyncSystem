import atexit

from flask import Flask
from sqlalchemy import text, inspect
from sqlalchemy.exc import OperationalError, NoSuchTableError
from sqlalchemy.orm import scoped_session

from app import create_app
from app.config import Config
from app.models import (
    ConfigSession, config_engine, source_engine, config_metadata, SyncTask
)
from app.scheduler import scheduler, start_scheduler


def initialize_databases(app: Flask):
    """
    初始化数据库：
    1. 创建配置库中的所有表 (如 sync_tasks)。
    2. 检查源库中的业务表，并按需添加 `_id` 字段。
    (增加对 'BASE TABLE' 的检查, 修复视图问题)
    """
    with app.app_context():
        print("Initializing databases...")

        # 1. 创建配置库表
        try:
            config_metadata.create_all(config_engine)
            print("Config database tables checked/created.")
        except Exception as e:
            print(f"CRITICAL: Failed to create config database tables: {e}")
            raise

        # 2. 检查并修改源库表
        print("Checking source tables for `_id` column...")

        # 需要一个 config session 来读取任务
        config_session = scoped_session(ConfigSession)
        try:
            tasks = config_session.query(SyncTask).all()
            db_name = Config.SOURCE_DB_NAME  # (已修复: 使用 SOURCE_DB_NAME)

            with source_engine.connect() as source_conn:
                for task in tasks:
                    if not task.source_table:
                        continue

                    try:
                        # 检查表是否存在
                        inspector = inspect(source_engine)
                        if not inspector.has_table(task.source_table):
                            print(
                                f"Warning: Table '{task.source_table}' for task {task.task_id} not found in source DB.")
                            continue

                        # 检查是否为物理表 (BASE TABLE), 而不是视图 (VIEW)
                        table_type_query = text(
                            "SELECT table_type FROM information_schema.tables "
                            "WHERE table_schema = :db_name AND table_name = :table_name"
                        )
                        result = source_conn.execute(table_type_query,
                                                     {"db_name": db_name, "table_name": task.source_table}).fetchone()

                        table_type = result[0] if result else None

                        if table_type == 'VIEW':
                            print(f"Skipping `_id` check for VIEW: '{task.source_table}'")
                            continue

                        if table_type != 'BASE TABLE':
                            print(
                                f"Warning: Skipping `_id` check for unknown table type '{table_type}': '{task.source_table}'")
                            continue

                        # 检查 `_id` 列是否存在
                        columns = [col['name'] for col in inspector.get_columns(task.source_table)]
                        if '_id' not in columns:
                            print(f"Adding `_id` column to table '{task.source_table}'...")
                            try:
                                # 为 _id 添加 varchar(50) 和索引
                                source_conn.execute(text(
                                    f"ALTER TABLE `{task.source_table}` ADD COLUMN `_id` VARCHAR(50) NULL DEFAULT NULL"))
                                source_conn.execute(
                                    text(f"ALTER TABLE `{task.source_table}` ADD INDEX `idx__id` (`_id`)"))
                                source_conn.commit()  # 提交 DDL
                                print(f"Successfully added `_id` column and index to '{task.source_table}'.")
                            except Exception as alter_e:
                                source_conn.rollback()  # 回滚 DDL
                                print(f"ERROR: Failed to add `_id` column to '{task.source_table}': {alter_e}")

                    except NoSuchTableError:
                        print(
                            f"Warning: Table '{task.source_table}' for task {task.task_id} not found (NoSuchTableError).")
                    except Exception as task_e:
                        print(f"Error processing table for task {task.task_id} ('{task.source_table}'): {task_e}")

        except OperationalError as e:
            print(f"ERROR: Cannot connect to databases during initialization: {e}")
        except Exception as e:
            print(f"ERROR: Failed during database initialization: {e}")
        finally:
            config_session.remove()

        print("Database initialization complete.")


def shutdown_scheduler():
    """在应用退出时关闭调度器"""
    print("Shutting down scheduler...")
    if scheduler.running:
        scheduler.shutdown()
    print("Scheduler shut down.")


# --- 应用启动 ---
if __name__ == "__main__":
    app = create_app()

    # 1. 初始化数据库 (建表, 添加 _id 字段)
    initialize_databases(app)

    # 2. 注册退出时关闭调度器的钩子
    atexit.register(shutdown_scheduler)

    # 3. 启动调度器
    start_scheduler(app)

    # 4. 启动 Flask Web 服务器
    # 注意: 在生产环境中, 应使用 Gunicorn 或 uWSGI
    print("Starting Flask web server...")
    try:
        app.run(host='0.0.0.0', port=5000)
    except (KeyboardInterrupt, SystemExit):
        print("Flask server received shutdown signal.")
    finally:
        # 确保调度器在 Ctrl+C 时也能关闭
        if scheduler.running:
            scheduler.shutdown()
        print("Application exiting.")
