import logging
import threading
import time
import traceback
from datetime import datetime, timedelta
from threading import current_thread

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import joinedload

from app.config import Config
from app.jdy2db_services import Jdy2DbSyncService
from app.jdy_api import DataApi
from app.models import ConfigSession, SyncTask, Department
from app.db2jdy_services import Db2JdySyncService, FieldMappingService
from app.utils import log_sync_error, TZ_UTC_8

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 全局调度器实例
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

# 正在运行的 binlog 监听器
running_binlog_listeners = set()


# 用于 FULL_REPLACE 和 INCREMENTAL 模式的包装器
def run_db2jdy_task_wrapper(task_id: int):
    """
    APScheduler 作业包装器: 运行单个 (db2jdy) 任务。
    """
    thread_name = f"TaskRunner-{task_id}"
    current_thread().name = thread_name

    sync_service = Db2JdySyncService()

    with (ConfigSession() as config_session):
        try:
            # 预加载 department 和 database
            task = config_session.query(SyncTask).options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                joinedload(SyncTask.database)
            ).filter(SyncTask.sync_type == 'db2jdy').get(task_id)

            # 1. 检查任务是否有效
            if not task:
                print(f"[{thread_name}] Task {task_id} not found. Removing job.")
                scheduler.remove_job(f"task_{task_id}")
                return

            if not task.is_active:
                print(f"[{thread_name}] Task {task_id} is disabled. Skipping.")
                return

            # 检查 API Key
            if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
                print(f"[{thread_name}] Task {task_id} missing API Key. Skipping.")
                log_sync_error(task_config=task, extra_info="Task skipped: Missing API Key.")
                return

            # 检查源数据库配置
            if not task.database:
                print(f"[{thread_name}] Task {task_id} missing Source Database config. Skipping.")
                log_sync_error(task_config=task, extra_info="Task skipped: Missing Source Database config.")
                return

            # 2. (关键) 并发检查: 如果任务已在运行, 则丢弃本次执行
            if task.sync_status == 'running':
                print(f"[{thread_name}] Task {task_id} is already running. Skipping this run.")
                return

            print(f"[{thread_name}] Starting task: {task.task_name} (Mode: {task.sync_mode})")

            # # 3. 运行前准备 (添加 _id 等)
            # sync_service._prepare_table(task)

            # 4. 执行任务
            if task.sync_mode == 'FULL_REPLACE':
                sync_service.run_full_replace(config_session, task)

            elif task.sync_mode == 'INCREMENTAL':
                sync_service.run_incremental(config_session, task)

        except OperationalError as e:
            print(f"[{thread_name}] DB connection error in task runner: {e}")
            log_sync_error(task_config=task, error=e, extra_info="Task runner DB connection error.")
            # 状态已在 service 中设置为 'error'
        except Exception as e:
            print(f"[{thread_name}] Unknown error in task runner: {e}")
            traceback.print_exc()
            log_sync_error(task_config=task, error=e, extra_info="Task runner unknown error.")
            # 确保状态被设置
            try:
                if task:
                    task.sync_status = 'error'
                    config_session.commit()
            except:
                config_session.rollback()


# --- jdy2db 任务的包装器 ---
def run_jdy2db_task_wrapper(task_id: int):
    """
    APScheduler 作业包装器: 运行 'jdy2db' 的每日全量同步。
    """
    thread_name = f"Jdy2DbTaskRunner-{task_id}"
    current_thread().name = thread_name

    # print(f"[{thread_name}] Starting Jdy->DB daily full sync...")
    with ConfigSession() as config_session:
        try:
            # logger.info(f"[Scheduler] jdy2db: Running full sync for Task {task_id}...")

            # 预加载所有需要的关系
            task = config_session.query(SyncTask).options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                joinedload(SyncTask.database)
            ).filter(SyncTask.sync_type == 'jdy2db').get(task_id)

            # 1. 检查任务是否有效
            if not task:
                print(f"[{thread_name}] Task {task_id} not found. Removing job.")
                scheduler.remove_job(f"task_{task_id}")
                return

            if not task.is_active:
                logger.info(f"[Scheduler] jdy2db: Task {task_id} is not active. Skipping.")
                return

            # 检查 API Key
            if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
                print(f"[{thread_name}] Task {task_id} missing API Key. Skipping.")
                log_sync_error(task_config=task, extra_info="Task skipped: Missing API Key.")
                return

            # 检查源数据库配置
            if not task.database:
                print(f"[{thread_name}] Task {task_id} missing Source Database config. Skipping.")
                log_sync_error(task_config=task, extra_info="Task skipped: Missing Source Database config.")
                return

            # 2. (关键) 并发检查: 如果任务已在运行, 则丢弃本次执行
            if task.sync_status == 'running':
                print(f"[{thread_name}] Task {task_id} is already running. Skipping this run.")
                return

            # [关键] 检查 app_id 和 entry_id 是否已填充
            if not task.app_id or not task.entry_id:
                logger.warning(
                    f"[Scheduler] jdy2db: Task {task_id} has no app_id/entry_id. Skipping full sync. (Waiting for first webhook)")
                return

            print(f"[{thread_name}] Starting task: {task.task_name} (Mode: {task.daily_sync_type})")

            # 实例化 DataApi (用于数据查询)
            api_client = DataApi(
                api_key=task.department.jdy_key_info.api_key,
                host=Config.JDY_API_HOST,
                qps=30
            )

            # 调用现有的全量同步逻辑
            sync_service = Jdy2DbSyncService()  # 实例化
            sync_service.sync_historical_data(task, api_client)

            # [关键] 如果同步类型是 ONCE，在成功后禁用任务 (或调度)
            if task.daily_sync_type == 'ONCE':
                logger.info(f"[Scheduler] jdy2db: Task {task_id} was set to ONCE. Disabling task.")
                task.is_active = False  # 禁用任务
                config_session.commit()
                # 我们还需要从调度器中移除它
                remove_task_from_scheduler(task_id)

        except Exception as e:
            print(f"[{thread_name}] Unknown error in task runner: {e}")
            traceback.print_exc()
            logger.error(f"[Scheduler] jdy2db: Full sync for Task {task_id} failed: {e}", exc_info=True)
            # 确保状态被设置
            try:
                if task:
                    task.sync_status = 'error'
                    config_session.commit()
            except:
                config_session.rollback()


def run_binlog_listener_in_thread(task_id: int):
    """
    包装器，用于在单独的线程中运行 binlog 监听器
    """
    try:
        # 在新线程中创建服务实例
        sync_service = Db2JdySyncService()

        # 获取任务对象 (binlog 监听器需要它)
        with ConfigSession() as session:
            # 预加载 department 和 database
            task = session.query(SyncTask).options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info),  # 预加载 Key
                joinedload(SyncTask.database)
            ).filter(SyncTask.sync_type == 'db2jdy').get(task_id)

            if not task:
                print(f"[BinlogListener-{task_id}] Task not found. Exiting thread.")
                return

            # 检查 API Key
            if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
                print(f"[BinlogListener-{task_id}] Task missing API Key. Exiting thread.")
                log_sync_error(task_config=task, extra_info="Binlog listener stopped: Missing API Key.")
                return

            # 检查源数据库配置
            if not task.database:
                print(f"[{task_id}] Task {task_id} missing Source Database config. Skipping.")
                log_sync_error(task_config=task, extra_info="Task skipped: Missing Source Database config.")
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
                    task.sync_status = 'error'
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
    """
    thread_name = "BinlogManagerThread"
    current_thread().name = thread_name
    # print(f"[{thread_name}] Checking BINLOG listener status...")

    with ConfigSession() as config_session:
        try:
            # 1. 查找所有激活的 BINLOG 任务
            # 预加载 department 和 database
            active_binlog_tasks = config_session.query(SyncTask).options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                joinedload(SyncTask.database)
            ).filter(
                SyncTask.is_active == True,
                SyncTask.sync_mode == 'BINLOG',
                SyncTask.sync_type == 'db2jdy'  # 确保只启动 db2jdy 的
            ).all()

            active_task_ids = {task.id for task in active_binlog_tasks}

            # 2. 查找需要停止的监听器
            tasks_to_stop = running_binlog_listeners - active_task_ids
            for task_id in tasks_to_stop:
                print(f"[{thread_name}] Task {task_id} is no longer active. (Listener will stop on next event)")
                # 实际停止由 run_binlog_listener 内部的 is_active 检查处理

            # 3. 查找需要启动的新监听器
            tasks_to_start = active_task_ids - running_binlog_listeners

            if tasks_to_start:
                print(f"[{thread_name}] Found {len(tasks_to_start)} new BINLOG tasks to start.")
                sync_service = Db2JdySyncService()  # 实例化

                for task_id in tasks_to_start:
                    # 从已加载的列表中获取任务，而不是重新查询
                    task = next((t for t in active_binlog_tasks if t.id == task_id), None)
                    if not task:
                        continue

                    # (关键) 检查是否已在运行 (以防万一)
                    if task.sync_status == 'running' and task.id in running_binlog_listeners:
                        continue

                    # 检查 API Key
                    if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
                        print(f"[{thread_name}] Task {task.id} missing API Key. Cannot start listener.")
                        log_sync_error(task_config=task, extra_info="Binlog listener cannot start: Missing API Key.")
                        continue

                    # 检查源数据库
                    if not task.database:
                        print(f"[{thread_name}] Task {task.id} missing Source Database. Cannot start listener.")
                        log_sync_error(task_config=task,
                                       extra_info="Binlog listener cannot start: Missing Source Database.")
                        continue

                    print(f"[{thread_name}] Starting listener for task: {task.id}...")

                    # # 在启动监听器前, 准备源表 (添加 _id 等)
                    # sync_service._prepare_table(task)

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
            # 预加载 department
            tasks_to_update = config_session.query(SyncTask).options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info)
            ).filter(
                SyncTask.is_active == True
            ).all()

            if not tasks_to_update:
                print(f"[{thread_name}] No active tasks to refresh mappings for.")
                return

            mapping_service = FieldMappingService()
            for task in tasks_to_update:
                try:
                    # 只刷新有 app_id 和 entry_id 的任务
                    if not task.app_id or not task.entry_id:
                        print(f"[{thread_name}] Task {task.id} missing app/entry ID. Skipping mapping update.")
                        continue

                    if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
                        print(f"[{thread_name}] Task {task.id} missing API Key. Skipping mapping update.")
                        log_sync_error(task_config=task, extra_info="Mapping update skipped: Missing API Key.")
                        continue

                    if task.sync_type == 'db2jdy':
                        mapping_service.update_form_fields_mapping(config_session, task)
                except Exception as task_err:
                    print(f"[{thread_name}] Failed to update mappings for task {task.id}: {task_err}")
                    # 记录错误, 但继续处理其他任务
                    log_sync_error(task_config=task, error=task_err,
                                   extra_info="Scheduled field mapping update failed.")

            print(f"[{thread_name}] Field mapping refresh complete ({len(tasks_to_update)} tasks).")

        except OperationalError as e:
            print(f"[{thread_name}] DB connection error in mapping_job: {e}")
        except Exception as e:
            print(f"[{thread_name}] Error in update_all_field_mappings_job: {e}")
            traceback.print_exc()


# --- 调度器辅助函数 ---

def remove_task_from_scheduler(task_id: int):
    """从调度器中移除一个作业 (db2jdy OR jdy2db)"""
    job_id = f"task_{task_id}"
    try:
        scheduler.remove_job(job_id)
        print(f"[{job_id}] Removed job from scheduler.")
    except JobLookupError:
        # 作业不存在，这没问题
        # print(f"[{job_id}] Job not found in scheduler, nothing to remove.") # 减少噪音
        pass
    except Exception as e:
        # 记录其他潜在错误
        print(f"[{job_id}] Error removing job: {e}")


def add_or_update_task_in_scheduler(task: SyncTask):
    """
    根据 SyncTask 对象在调度器中添加或更新一个作业。
    防止刷新时重置现有的 next_run_time
    """
    job_id = f"task_{task.id}"
    existing_job = scheduler.get_job(job_id)

    # 1. 任务被禁用
    if not task.is_active:
        if existing_job:
            remove_task_from_scheduler(task.id)
            print(f"[{job_id}] Task is inactive, removing from schedule.")
        return

    # 2. 定义新 trigger 的参数
    new_trigger_type = None
    new_trigger_args = {}
    job_func = None

    # --- 按 sync_type 分支 ---
    mode_str = None
    if task.sync_type == 'db2jdy':
        job_func = run_db2jdy_task_wrapper  # db2jdy 的包装器

        if task.sync_mode == 'FULL_REPLACE':
            if task.full_replace_time:
                mode_str = "FULL_REPLACE"
                new_trigger_type = 'cron'
                new_trigger_args = {
                    'hour': task.full_replace_time.hour,
                    'minute': task.full_replace_time.minute
                }
            else:
                print(f"Warning: Task {task.id} (db2jdy - FULL_REPLACE) is active but has no time. Removing.")
                if existing_job:
                    remove_task_from_scheduler(task.id)
                return

        elif task.sync_mode == 'INCREMENTAL':
            if task.incremental_interval and task.incremental_interval > 0:
                mode_str = "INCREMENTAL"
                new_trigger_type = 'interval'
                new_trigger_args = {'minutes': task.incremental_interval}
            else:
                print(f"Warning: Task {task.id} (db2jdy - INCREMENTAL) is active but has no interval. Removing.")
                if existing_job:
                    remove_task_from_scheduler(task.id)
                return

        elif task.sync_mode == 'BINLOG':
            # BINLOG 任务由 binlog_manager 自动处理。
            # 确保移除旧的作业（例如从 CRON 切换过来的）
            if existing_job:
                print(f"[{job_id}] Task changed to BINLOG. Removing old CRON/INTERVAL job.")
                remove_task_from_scheduler(task.id)
            return  # BINLOG tasks don't have a 'task_X' job

    elif task.sync_type == 'jdy2db':
        # jdy2db 任务只处理 "DAILY" 类型的全量同步
        if task.daily_sync_type == 'DAILY' and task.daily_sync_time:
            job_func = run_jdy2db_task_wrapper  # jdy2db 的包装器
            mode_str = "DAILY"
            new_trigger_type = 'cron'
            new_trigger_args = {
                'hour': task.daily_sync_time.hour,
                'minute': task.daily_sync_time.minute
            }
        else:
            # 不是 DAILY 类型，或者时间未设置，确保没有调度
            print(f"[{job_id}] Task (jdy2db) is not configured for DAILY sync. Removing from schedule.")
            if existing_job: remove_task_from_scheduler(task.id)
            return

    # 3. Job 存在: 检查是否需要修改
    if existing_job:
        trigger_changed = False

        # 检查执行函数是否已更改
        if existing_job.func.__name__ != job_func.__name__:
            trigger_changed = True
            print(f"[{job_id}] Job function changed (e.g., db2jdy -> jdy2db).")

        # 检查 trigger 类型是否匹配
        elif (new_trigger_type == 'cron' and not isinstance(existing_job.trigger, CronTrigger)) or \
                (new_trigger_type == 'interval' and not isinstance(existing_job.trigger, IntervalTrigger)):
            trigger_changed = True
            print(f"[{job_id}] Trigger type changed (e.g., CRON -> INTERVAL).")

        # 检查 trigger 参数是否匹配
        elif new_trigger_type == 'cron':
            # 兼容 APScheduler 3.x/4.x:
            current_hour_str = 'None'
            current_min_str = 'None'

            # 尝试 APScheduler 3.x 风格 (obj.hour)
            if hasattr(existing_job.trigger, 'hour'):
                current_hour_str = str(existing_job.trigger.hour)
                current_min_str = str(existing_job.trigger.minute)
            # 尝试 APScheduler 4.x 风格 (obj.fields)
            elif hasattr(existing_job.trigger, 'fields'):
                try:
                    hour_field = next((f for f in existing_job.trigger.fields if f.name == 'hour'), None)
                    min_field = next((f for f in existing_job.trigger.fields if f.name == 'minute'), None)
                    current_hour_str = str(hour_field) if hour_field else 'None'
                    current_min_str = str(min_field) if min_field else 'None'
                except:
                    pass  # 保留 'None'

            new_hour_str = str(new_trigger_args['hour'])
            new_min_str = str(new_trigger_args['minute'])

            if (current_hour_str != new_hour_str or current_min_str != new_min_str):
                trigger_changed = True
                print(
                    f"[{job_id}] CRON time changed from {current_hour_str}:{current_min_str} to {new_hour_str}:{new_min_str}.")

        elif new_trigger_type == 'interval':
            # APScheduler 将 interval 存储为 timedelta
            current_minutes = existing_job.trigger.interval.total_seconds() / 60
            if current_minutes != new_trigger_args['minutes']:
                trigger_changed = True
                print(f"[{job_id}] Interval changed from {current_minutes}m to {new_trigger_args['minutes']}m.")

        if trigger_changed:
            print(f"[{job_id}] Rescheduling job...")
            scheduler.reschedule_job(job_id, func=job_func, trigger=new_trigger_type, **new_trigger_args)
        # else:
        #     print(f"[{job_id}] Job exists and trigger is unchanged. Skipping.") # (跳过是期望的行为)

    # 4. Job 不存在: 添加 (且 trigger 和 func 有效)
    elif new_trigger_type and job_func:
        if task.sync_type == 'db2jdy':
            if task.sync_mode == 'INCREMENTAL':
                print(f"Scheduling new {job_id} ({mode_str}) every {new_trigger_args.get('minutes', 'N/A')} minutes.")
            elif task.sync_mode == 'FULL_REPLACE':
                # 格式化分钟，确保 11:1 变为 11:01
                minute_str = str(new_trigger_args.get('minute', 'N/A')).zfill(2)
                print(f"Scheduling new {job_id} ({mode_str}) at {new_trigger_args.get('hour', 'N/A')}:{minute_str}.")

        elif task.sync_type == 'jdy2db':
            minute_str = str(new_trigger_args.get('minute', 'N/A')).zfill(2)
            print(f"Scheduling new {job_id} ({mode_str}) at {new_trigger_args.get('hour', 'N/A')}:{minute_str}.")

        # 仅在新任务(且为INCREMENTAL)时设置 10s 延迟
        next_run = None
        if task.sync_type == 'db2jdy' and task.sync_mode == 'INCREMENTAL':
            next_run = datetime.now(TZ_UTC_8) + timedelta(seconds=10)

        scheduler.add_job(
            job_func,  # 使用动态函数
            trigger=new_trigger_type,
            args=[task.id],
            id=job_id,
            max_instances=1,
            misfire_grace_time=60,
            next_run_time=next_run,  # CRON 模式下为 None 是正确的
            **new_trigger_args
        )


# --- 启动调度器 ---

def start_scheduler(app: Flask):
    """
    添加作业并启动调度器。
    为每个任务动态创建作业。
    """
    with app.app_context():
        print("Starting APScheduler...")
        try:
            # 1. 添加 BINLOG 监听器管理器
            scheduler.add_job(
                check_and_start_new_binlog_listeners,
                trigger='interval',
                minutes=Config.CHECK_INTERVAL_MINUTES,  # 每 CHECK_INTERVAL_MINUTES 检查一次是否有新/停止的 binlog 任务
                id='binlog_manager',
                replace_existing=True,
                max_instances=1,  # (关键) 防止并发
                next_run_time=datetime.now(TZ_UTC_8) + timedelta(seconds=5),  # 5秒后启动
                misfire_grace_time=60  # 增加misfire_grace_time以防任务堆积
            )

            # 2. 添加字段映射刷新器
            scheduler.add_job(
                update_all_field_mappings_job,
                trigger='interval',
                minutes=Config.CACHE_REFRESH_INTERVAL_MINUTES,
                id='field_mapping_updater',
                replace_existing=True,
                max_instances=1,  # (关键) 防止并发
                next_run_time=datetime.now(TZ_UTC_8) + timedelta(seconds=3),  # 3秒后首次启动
                misfire_grace_time=60  # 增加misfire_grace_time以防任务堆积
            )

            # 3. 在启动时加载所有 *激活* 任务 (db2jdy 和 jdy2db)
            try:
                with ConfigSession() as config_session:
                    active_tasks = config_session.query(SyncTask).options(
                        joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                        joinedload(SyncTask.database)
                    ).filter_by(is_active=True).all()

                    print(f"Loading {len(active_tasks)} active tasks at startup...")
                    for task in active_tasks:
                        add_or_update_task_in_scheduler(task)
            except Exception as e:
                print(f"Warning: Failed to pre-load tasks at startup (will retry on first refresh): {e}")

            scheduler.start()
            print("Scheduler started successfully with dynamic jobs.")

        except Exception as e:
            print(f"CRITICAL: Failed to start scheduler: {e}")
            traceback.print_exc()


# --- 刷新调度器 ---

def refresh_scheduler(app: Flask):
    """
    更新作业并启动调度器。
    现在为每个任务动态创建作业。
    """
    with app.app_context():
        print("Refreshing APScheduler...")
        try:
            # 1. 添加定时任务
            with ConfigSession() as config_session:

                # 1. 删除失效任务，我们需要知道数据库中所有的 task_id，而不仅仅是 active=False 的
                all_db_task_ids = {task.id for task in config_session.query(SyncTask.id).all()}
                all_scheduler_task_ids = {
                    int(job.id.split('_')[-1]) for job in scheduler.get_jobs()
                    # 排除 task_refresher 任务，应为 job_id = f"task_{task.id}"
                    if job.id.startswith('task_') and job.id.split('_')[-1].isdigit()
                }

                tasks_to_remove_ids = all_scheduler_task_ids - all_db_task_ids

                # 还需要检查那些在数据库中但 is_active=False 的
                inactive_tasks_in_db = config_session.query(SyncTask.id).filter_by(is_active=False).all()
                inactive_task_ids = {task_id for (task_id,) in inactive_tasks_in_db}

                tasks_to_remove_ids.update(inactive_task_ids.intersection(all_scheduler_task_ids))

                if tasks_to_remove_ids:  # 仅在有任务要移除时打印
                    print(f"Found {len(tasks_to_remove_ids)} tasks to remove from scheduler.")
                    for task_id in tasks_to_remove_ids:
                        remove_task_from_scheduler(task_id)

                # 2. 添加/更新生效任务
                active_tasks = config_session.query(SyncTask).options(
                    joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                    joinedload(SyncTask.database)
                ).filter_by(is_active=True).all()
                print(f"Found {len(active_tasks)} active tasks to check/update.")

                for task in active_tasks:
                    # 调用新的可重用函数来添加或更新每个作业
                    add_or_update_task_in_scheduler(task)

            # scheduler.start() # 调度器已在 start_scheduler 中启动
            print("Scheduler refreshed successfully with dynamic jobs.")

        except Exception as e:
            print(f"CRITICAL: Failed to refresh scheduler: {e}")
            traceback.print_exc()
