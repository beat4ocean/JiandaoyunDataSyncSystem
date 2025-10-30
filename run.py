from sqlalchemy import text, inspect

from app import create_app
from app.models import config_metadata, config_engine, source_engine, ConfigSession, SyncTask
from app.scheduler import start_scheduler

# 1. 创建 App 实例
app = create_app()


def initialize_databases():
    """
    初始化数据库表
    """
    app.logger.info("Initializing Config Database (jdy_sync_config_db)...")
    try:
        config_metadata.create_all(config_engine)
        app.logger.info("Config Database tables created/checked.")
    except Exception as e:
        app.logger.error(f"CRITICAL: Failed to initialize Config Database: {e}")
        raise

    app.logger.info("Checking Source Database (source_business_db)...")
    try:
        # 检查目标数据库连接
        with source_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        app.logger.info("Source Database connection successful.")

        # 检查源表中的 _id 字段 (使用手动会话)
        with ConfigSession() as session:
            tasks_tables = session.query(SyncTask.source_table).distinct().all()
            source_tables = [table for (table,) in tasks_tables]

        if not source_tables:
            app.logger.info("No tasks found, skipping _id column check.")
            return

        inspector = inspect(source_engine)
        db_name = app.config['SOURCE_DB_NAME']
        table_type_query = text(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema = :db_name AND table_name = :table_name"
        )

        with source_engine.connect() as conn:
            for table_name in source_tables:
                if not inspector.has_table(table_name):
                    app.logger.warning(f"Source table '{table_name}' does not exist. Skipping _id check.")
                    continue

                table_type = None
                table_type_result = conn.execute(table_type_query,
                                                 {"db_name": db_name, "table_name": table_name}).fetchone()
                if table_type_result:
                    table_type = table_type_result[0]

                if table_type == 'VIEW':
                    app.logger.info(f"Source table {table_name} is a VIEW. Skipping _id column check.")
                    continue

                if table_type == 'BASE TABLE':
                    columns = [col['name'] for col in inspector.get_columns(table_name)]
                    if '_id' not in columns:
                        try:
                            # 尝试添加字段
                            conn.execute(text(
                                f"ALTER TABLE `{table_name}` ADD COLUMN _id VARCHAR(50) NULL DEFAULT NULL COMMENT 'JDY _id'"))
                            conn.commit()
                            app.logger.info(f"Added '_id' column to source table {table_name}.")
                        except Exception as e:
                            app.logger.warning(f"Could not add '_id' column to {table_name}: {e}")
                    else:
                        app.logger.info(f"Column '_id' already exists in {table_name}.")
                else:
                    app.logger.warning(
                        f"Source table {table_name} is of unknown type '{table_type}'. Skipping _id check.")

        app.logger.info("Source Database schema check completed.")

    except Exception as e:
        app.logger.error(f"CRITICAL: Failed to connect or check Source Database! {e}")
        # 在生产环境中可能希望停止启动
        # raise


if __name__ == '__main__':
    # 1. 初始化数据库
    initialize_databases()

    # 2. 启动调度器 (传入 app 以读取 config)
    start_scheduler(app)

    # 3. 启动 Flask Web 服务器
    app.logger.info("Starting Flask Web Server on 0.0.0.0:5000...")
    # use_reloader=False 对 APScheduler 很重要
    app.run(host='0.0.0.0', port=5000, use_reloader=False)
