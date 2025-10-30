import atexit
import time
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from flask import current_app

from app.models import db, SyncTask
from app.services import SyncService, FieldMappingService

scheduler = BackgroundScheduler(daemon=True)
running_binlog_tasks = set()  # 跟踪正在运行的 binlog 线程


def scheduled_job_runner(app):
    """
    运行 FULL_REPLACE 和 INCREMENTAL 任务
    """
    with app.app_context():
        current_app.logger.info("Scheduler: Running scheduled_job_runner...")
        tasks = SyncTask.query.filter(
            SyncTask.sync_mode.in_(['FULL_REPLACE', 'INCREMENTAL']),
            SyncTask.status == 'running'  # 'running' 表示启用
        ).all()

        sync_service = SyncService()

        for task in tasks:
            current_app.logger.info(f"Scheduler: Executing task {task.task_id} ({task.sync_mode})")
            try:
                if task.sync_mode == 'FULL_REPLACE':
                    sync_service.run_full_replace(task)
                elif task.sync_mode == 'INCREMENTAL':
                    sync_service.run_incremental(task)
            except Exception as e:
                current_app.logger.error(f"Scheduler: Error running task {task.task_id}: {e}")
                # service 内部已经记录了错误


def check_and_start_new_binlog_listeners(app):
    """
    检查并启动新的 (或崩溃的) BINLOG 任务
    """
    with app.app_context():
        current_app.logger.debug("Scheduler: Running check_and_start_new_binlog_listeners...")
        tasks = SyncTask.query.filter_by(sync_mode='BINLOG', status='running').all()

        sync_service = SyncService()  # 在上下文中创建

        for task in tasks:
            if task.task_id not in running_binlog_tasks:
                current_app.logger.info(f"Scheduler: Found new/stopped BINLOG task {task.task_id}. Starting thread...")

                # 必须将 app 实例传递给线程
                thread = Thread(
                    target=sync_service.run_binlog_listener,
                    args=(task, app),
                    daemon=True
                )
                thread.start()
                running_binlog_tasks.add(task.task_id)

        # (可选) 清理已停止的任务
        # 实际中需要更复杂的线程监控，这里简化处理
        # 假设 run_binlog_listener 崩溃时会更新状态为 'error'
        stopped_tasks = SyncTask.query.filter(
            SyncTask.sync_mode == 'BINLOG',
            SyncTask.status != 'running'
        ).all()
        for task in stopped_tasks:
            if task.task_id in running_binlog_tasks:
                current_app.logger.info(f"Scheduler: Removing disabled/errored task {task.task_id} from running list.")
                running_binlog_tasks.remove(task.task_id)


def update_all_field_mappings_job(app):
    """
    定时更新所有任务的字段映射（例如每天一次）
    """
    with app.app_context():
        current_app.logger.info("Scheduler: Running update_all_field_mappings_job...")
        tasks = SyncTask.query.filter_by(status='running').all()
        mapping_service = FieldMappingService()

        for task in tasks:
            try:
                mapping_service.update_form_fields_mapping(task)
                time.sleep(1)  # 避免 API 限制
            except Exception as e:
                current_app.logger.error(f"Scheduler: Failed to update mappings for task {task.task_id}: {e}")


def start_scheduler(app):
    """
    由 run.py 调用以启动调度器
    """
    current_app.logger.info("Starting APScheduler...")

    # 添加 FULL_REPLACE 和 INCREMENTAL 任务运行器
    # 假设每 5 分钟运行一次
    scheduler.add_job(
        func=scheduled_job_runner,
        trigger='interval',
        minutes=5,
        args=[app],
        id='scheduled_job_runner'
    )

    # 添加 BINLOG 任务检查器
    # 每 30 秒检查一次是否有新任务或崩溃的任务
    scheduler.add_job(
        func=check_and_start_new_binlog_listeners,
        trigger='interval',
        seconds=30,
        args=[app],
        id='check_binlog_listeners'
    )

    # 添加字段映射更新任务
    # 每天凌晨 3 点运行
    scheduler.add_job(
        func=update_all_field_mappings_job,
        trigger='cron',
        hour=3,
        minute=0,
        args=[app],
        id='update_all_mappings'
    )

    scheduler.start()

    # 确保程序退出时关闭调度器
    atexit.register(lambda: scheduler.shutdown())
