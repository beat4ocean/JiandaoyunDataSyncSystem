import threading
import time
import traceback
from datetime import datetime, timedelta
from threading import current_thread

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from sqlalchemy.exc import OperationalError

from app.config import Config
from app.models import ConfigSession, SourceSession, SyncTask
from app.services import SyncService, FieldMappingService
from app.utils import log_sync_error, TZ_UTC_8

# 全局调度器实例
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

# 正在运行的 binlog 监听器
running_binlog_listeners = set()


def scheduled_job_runner():
    """
    APScheduler 作业: 运行 FULL_REPLACE 和 INCREMENTAL 任务。
    (增加 prepare_source_table 调用)
    """
    thread_name = "SchedulerThread"
    current_thread().name = thread_name
    print(f"[{thread_name}] Checking for scheduled tasks...")

    with ConfigSession() as config_session:
        try:
            tasks_to_run = config_session.query(SyncTask).filter(
                SyncTask.is_active == True,
                SyncTask.status.in_(['idle', 'error']),
                SyncTask.sync_mode.in_(['FULL_REPLACE', 'INCREMENTAL'])
            ).all()

            if not tasks_to_run:
                # print(f"[{thread_name}] No scheduled tasks to run.")
                return

            print(f"[{thread_name}] Found {len(tasks_to_run)} tasks to run.")
            sync_service = SyncService()  # 实例化

            for task in tasks_to_run:
                try:
                    # 在运行任务前, 准备源表 (添加 _id 等)
                    sync_service._prepare_source_table(task)

                    # 重新加载 task, 以防 prepare_source_table
                    config_session.refresh(task)

                    if task.sync_mode == 'FULL_REPLACE':
                        with SourceSession() as source_session:
                            sync_service.run_full_replace(config_session, source_session, task)

                    elif task.sync_mode == 'INCREMENTAL':
                        with SourceSession() as source_session:
                            sync_service.run_incremental(config_session, source_session, task)

                except Exception as task_err:
                    print(f"[{thread_name}] Error running task {task.task_id}: {task_err}")
                    traceback.print_exc()
                    # 标记任务失败, 但不停止调度器
                    try:
                        task.status = 'error'
                        config_session.commit()
                    except Exception as status_err:
                        print(
                            f"[{thread_name}] CRITICAL: Failed to set error status for task {task.task_id}: {status_err}")
                        config_session.rollback()


        except OperationalError as e:
            print(f"[{thread_name}] DB connection error in scheduled_job_runner: {e}")
        except Exception as e:
            print(f"[{thread_name}] Error in scheduled_job_runner: {e}")
            traceback.print_exc()


def run_binlog_listener_in_thread(task_id: int):
    """
    包装器，用于在单独的线程中运行 binlog 监听器
    """
    try:
        # 在新线程中创建服务实例
        sync_service = SyncService()

        # 获取任务对象 (binlog 监听器需要它)
        with ConfigSession() as session:
            task = session.query(SyncTask).get(task_id)
            if not task:
                print(f"[BinlogListener-{task_id}] Task not found. Exiting thread.")
                return

        # 运行长连接监听器
        sync_service.run_binlog_listener(task)

    except Exception as e:
        print(f"[BinlogListener-{task_id}] Thread CRASHED: {e}")
        traceback.print_exc()
        # 发生严重错误, 更新任务状态
        with ConfigSession() as session:
            try:
                task = session.query(SyncTask).get(task_id)
                if task:
                    task.status = 'error'
                    session.commit()
            except Exception as db_e:
                print(f"[BinlogListener-{task_id}] CRITICAL: Failed to set error status after crash: {db_e}")
    finally:
        # 线程结束, 从集合中移除
        print(f"[BinlogListener-{task_id}] Thread finished.")
        running_binlog_listeners.discard(task_id)


def check_and_start_new_binlog_listeners():
    """
    APScheduler 作业: 检查并启动新的 BINLOG 监听器。
    (增加 prepare_source_table 调用)
    """
    thread_name = "BinlogManagerThread"
    current_thread().name = thread_name
    # print(f"[{thread_name}] Checking BINLOG listener status...")

    with ConfigSession() as config_session:
        try:
            # 1. 查找所有激活的 BINLOG 任务
            active_binlog_tasks = config_session.query(SyncTask).filter(
                SyncTask.is_active == True,
                SyncTask.sync_mode == 'BINLOG'
            ).all()

            active_task_ids = {task.task_id for task in active_binlog_tasks}

            # 2. 查找需要停止的监听器
            tasks_to_stop = running_binlog_listeners - active_task_ids
            for task_id in tasks_to_stop:
                print(f"[{thread_name}] Task {task_id} is no longer active. (Listener will stop on next event)")
                # 实际停止由 run_binlog_listener 内部的 is_active 检查处理

            # 3. 查找需要启动的新监听器
            tasks_to_start = active_task_ids - running_binlog_listeners

            if tasks_to_start:
                print(f"[{thread_name}] Found {len(tasks_to_start)} new BINLOG tasks to start.")
                sync_service = SyncService()  # 实例化

                for task_id in tasks_to_start:
                    task = config_session.query(SyncTask).get(task_id)  # 获取任务对象
                    if not task:
                        continue

                    print(f"[{thread_name}] Starting listener for task: {task.task_id}...")

                    # 在启动监听器前, 准备源表 (添加 _id 等)
                    sync_service._prepare_source_table(task)

                    listener_thread = threading.Thread(
                        target=run_binlog_listener_in_thread,
                        args=(task_id,),
                        daemon=True  # 守护线程随主程序退出
                    )
                    listener_thread.start()
                    running_binlog_listeners.add(task_id)
                    time.sleep(1)  # 错开启动

        except OperationalError as e:
            print(f"[{thread_name}] DB connection error in binlog_manager: {e}")
        except Exception as e:
            print(f"[{thread_name}] Error in check_and_start_new_binlog_listeners: {e}")
            traceback.print_exc()


def update_all_field_mappings_job():
    """
    APScheduler 作业: 定期刷新所有*激活*任务的字段映射。
    """
    thread_name = "FieldMappingUpdateThread"
    current_thread().name = thread_name
    print(f"[{thread_name}] Starting scheduled field mapping refresh...")

    with ConfigSession() as config_session:
        try:
            tasks_to_update = config_session.query(SyncTask).filter(
                SyncTask.is_active == True
            ).all()

            if not tasks_to_update:
                print(f"[{thread_name}] No active tasks to refresh mappings for.")
                return

            mapping_service = FieldMappingService()
            for task in tasks_to_update:
                try:
                    mapping_service.update_form_fields_mapping(config_session, task)
                except Exception as task_err:
                    print(f"[{thread_name}] Failed to update mappings for task {task.task_id}: {task_err}")
                    # 记录错误, 但继续处理其他任务
                    log_sync_error(task_config=task, error=task_err,
                                   extra_info="Scheduled field mapping update failed.")

            print(f"[{thread_name}] Field mapping refresh complete ({len(tasks_to_update)} tasks).")

        except OperationalError as e:
            print(f"[{thread_name}] DB connection error in mapping_job: {e}")
        except Exception as e:
            print(f"[{thread_name}] Error in update_all_field_mappings_job: {e}")
            traceback.print_exc()


def start_scheduler(app: Flask):
    """
    添加作业并启动调度器。
    """
    with app.app_context():
        print("Starting APScheduler...")
        try:
            # 1. 添加 FULL_REPLACE / INCREMENTAL 运行器
            scheduler.add_job(
                scheduled_job_runner,
                trigger='interval',
                minutes=Config.CHECK_INTERVAL_MINUTES,
                id='scheduled_job_runner',
                replace_existing=True,
                next_run_time=datetime.now(TZ_UTC_8) + timedelta(seconds=10)  # 10秒后启动
            )

            # 2. 添加 BINLOG 监听器管理器
            scheduler.add_job(
                check_and_start_new_binlog_listeners,
                trigger='interval',
                seconds=30,  # 每 30 秒检查一次是否有新/停止的 binlog 任务
                id='binlog_manager',
                replace_existing=True,
                next_run_time=datetime.now(TZ_UTC_8) + timedelta(seconds=5)  # 5秒后启动
            )

            # 3. 添加字段映射刷新器
            scheduler.add_job(
                update_all_field_mappings_job,
                trigger='interval',
                minutes=Config.CACHE_REFRESH_INTERVAL_MINUTES,
                id='field_mapping_updater',
                replace_existing=True,
                next_run_time=datetime.now(TZ_UTC_8) + timedelta(seconds=3)  # 3秒后首次启动
            )

            scheduler.start()
            print("Scheduler started successfully.")
        except Exception as e:
            print(f"CRITICAL: Failed to start scheduler: {e}")
            traceback.print_exc()
