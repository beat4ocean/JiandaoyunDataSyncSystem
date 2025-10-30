import atexit
import time
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app.models import ConfigSession, SyncTask
from app.services import SyncService, FieldMappingService

scheduler = BackgroundScheduler(daemon=True)
running_binlog_tasks = set()  # 跟踪正在运行的 binlog 线程


def scheduled_job_runner():
    """
    运行 FULL_REPLACE 和 INCREMENTAL 任务
    """
    print("Scheduler: Running scheduled_job_runner...")

    with ConfigSession() as config_session:
        try:
            tasks = config_session.query(SyncTask).filter(
                SyncTask.sync_mode.in_(['FULL_REPLACE', 'INCREMENTAL']),
                SyncTask.is_active == True,
                SyncTask.status != 'running'  # 假设 'running' 是 Binlog 专用
            ).all()

            if not tasks:
                print("Scheduler: No FULL_REPLACE/INCREMENTAL tasks scheduled to run.")
                return

            sync_service = SyncService()

            for task in tasks:
                print(f"Scheduler: Executing task {task.task_id} ({task.sync_mode})")
                try:
                    if task.sync_mode == 'FULL_REPLACE':
                        sync_service.run_full_replace(config_session, task)
                    elif task.sync_mode == 'INCREMENTAL':
                        sync_service.run_incremental(config_session, task)
                except Exception as e:
                    print(f"Scheduler: Error running task {task.task_id}: {e}")
                    # service 内部已使用新的 log_sync_error

        except Exception as session_err:
            print(f"Scheduler: Failed to query tasks: {session_err}")


def check_and_start_new_binlog_listeners():
    """
    检查并启动新的 (或崩溃的) BINLOG 任务
    """
    print("Scheduler: Running check_and_start_new_binlog_listeners...")

    with ConfigSession() as config_session:
        try:
            tasks = config_session.query(SyncTask).filter_by(
                sync_mode='BINLOG',
                is_active=True
            ).all()

            # 检查哪些任务应该在运行但不在 running_binlog_tasks
            for task in tasks:
                if task.task_id not in running_binlog_tasks:
                    print(f"Scheduler: Found new/stopped BINLOG task {task.task_id}. Starting thread...")

                    sync_service = SyncService()
                    thread = Thread(
                        source=sync_service.run_binlog_listener,
                        args=(task,),
                        daemon=True
                    )
                    thread.start()
                    running_binlog_tasks.add(task.task_id)

            # 检查哪些任务在 running_binlog_tasks 但已不活跃
            active_task_ids = {t.task_id for t in tasks}
            tasks_to_remove = running_binlog_tasks - active_task_ids
            for task_id in tasks_to_remove:
                print(f"Scheduler: Removing disabled task {task_id} from running list.")
                running_binlog_tasks.remove(task_id)

            # 检查崩溃的线程
            errored_tasks = config_session.query(SyncTask).filter_by(
                sync_mode='BINLOG',
                status='error'
            ).all()
            for task in errored_tasks:
                if task.task_id in running_binlog_tasks:
                    print(f"Scheduler: Removing errored task {task.task_id} from running list.")
                    running_binlog_tasks.remove(task.task_id)

        except Exception as session_err:
            print(f"Scheduler: Failed to check binlog tasks: {session_err}")


def update_all_field_mappings_job():
    """
    定时更新所有任务的字段映射（缓存刷新）
    """
    print("Scheduler: Running update_all_field_mappings_job...")

    with ConfigSession() as config_session:
        try:
            tasks = config_session.query(SyncTask).filter_by(is_active=True).all()
            mapping_service = FieldMappingService()

            for task in tasks:
                try:
                    mapping_service.update_form_fields_mapping(config_session, task)
                    time.sleep(2)  # 适配 V5 QPS (30)
                except Exception as e:
                    print(f"Scheduler: Failed to update mappings for task {task.task_id}: {e}")
                    # service 内部已记录日志
        except Exception as session_err:
            print(f"Scheduler: Failed to query tasks for mapping update: {session_err}")


def start_scheduler(app: Flask):
    """
    由 run.py 调用以启动调度器
    """

    # 从 app.config 读取间隔
    check_interval = app.config.get('CHECK_INTERVAL_MINUTES', 1)
    cache_refresh_interval = app.config.get('CACHE_REFRESH_INTERVAL_MINUTES', 5)

    print(f"Starting APScheduler...")
    print(f"  - Full/Incremental check interval: {check_interval} minutes")
    print(f"  - Mapping cache refresh interval: {cache_refresh_interval} minutes")
    print(f"  - Binlog listener check interval: 30 seconds")

    scheduler.add_job(
        func=scheduled_job_runner,
        trigger='interval',
        minutes=check_interval,  # 使用配置
        id='scheduled_job_runner'
    )

    scheduler.add_job(
        func=check_and_start_new_binlog_listeners,
        trigger='interval',
        seconds=30,  # 保持 Binlog 检查的高频率
        id='check_binlog_listeners'
    )

    scheduler.add_job(
        func=update_all_field_mappings_job,
        trigger='interval',  # 触发器已从 cron 更改为 interval
        minutes=cache_refresh_interval,  # 使用配置
        id='update_all_mappings'
    )

    scheduler.start()

    # 确保程序退出时关闭调度器
    atexit.register(lambda: scheduler.shutdown())
