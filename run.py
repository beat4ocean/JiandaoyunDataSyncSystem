import logging
from app import create_app
from app.models import db, SyncTask
from app.scheduler import start_scheduler
from sqlalchemy import text, inspect

# 1. 创建 App 实例
app = create_app()


def initialize_databases():
    """
    初始化数据库表
    """
    with app.app_context():
        app.logger.info("Initializing Config Database (jdy_sync_config_db)...")
        # 1. 创建配置库中的所有表 (SyncTask, FormFieldMapping, SyncErrLog)
        db.create_all()
        app.logger.info("Config Database tables created/checked.")

        # 2. (可选) 检查源数据库连接并尝试添加 _id 字段
        app.logger.info("Checking Source Database (source_business_db)...")
        try:
            engine = db.session.get_bind('source_db')
            inspector = inspect(engine)  # 使用 inspector 检查

            with engine.connect() as conn:
                # 检查连接
                conn.execute(text("SELECT 1"))

                # 获取所有已配置的源表
                tasks_tables = db.session.query(SyncTask.source_table).distinct().all()
                source_tables = [table for (table,) in tasks_tables]

                for table_name in source_tables:
                    if not inspector.has_table(table_name):
                        app.logger.warning(f"Source table '{table_name}' does not exist. Skipping _id check.")
                        continue

                    # 检查 _id 字段是否存在
                    columns = [col['name'] for col in inspector.get_columns(table_name)]
                    if '_id' not in columns:
                        try:
                            # 尝试添加字段
                            conn.execute(text(
                                f"ALTER TABLE {table_name} ADD COLUMN _id VARCHAR(50) NULL DEFAULT NULL COMMENT 'JDY _id'"))
                            conn.commit()
                            app.logger.info(f"Added '_id' column to source table {table_name}.")
                        except Exception as e:
                            app.logger.warning(f"Could not add '_id' column to {table_name}: {e}")
                    else:
                        app.logger.info(f"Column '_id' already exists in {table_name}.")

            app.logger.info("Source Database connection successful and schema checked.")

        except Exception as e:
            app.logger.error(f"CRITICAL: Failed to connect or check Source Database! {e}")
            # 在生产环境中，这可能是致命错误
            # raise


if __name__ == '__main__':
    # 1. 初始化数据库
    initialize_databases()

    # 2. 启动调度器 (必须在 app 上下文创建后)
    # 将 app 实例传递给调度器，以便作业可以创建上下文
    start_scheduler(app)

    # 3. 启动 Flask Web 服务器
    app.logger.info("Starting Flask Web Server on 0.0.0.0:5000...")
    app.run(host='0.0.0.0', port=5000, use_reloader=False)  # use_reloader=False 对 APScheduler 很重要
