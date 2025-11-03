import json
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, time as time_obj
from threading import current_thread
from typing import Tuple, List, Any, Generator
from urllib.parse import quote_plus

import requests
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from sqlalchemy import text, inspect, create_engine
from sqlalchemy.exc import OperationalError, IntegrityError, NoSuchTableError
from sqlalchemy.orm import Session, sessionmaker, joinedload

from app.config import Config, DB_CONNECT_ARGS
from app.jdy_api import FormApi, DataApi
from app.models import ConfigSession, SyncTask, FormFieldMapping, DatabaseInfo
from app.utils import log_sync_error, json_serializer, TZ_UTC_8, retry

# 动态引擎缓存
# 缓存 {database_info_id: (engine, SessionLocal)}
dynamic_engine_cache = {}


@contextmanager
def get_dynamic_session(task: SyncTask) -> Generator[Any, Any, None]:
    """
    一个上下文管理器，用于根据任务动态获取源数据库会话。
    它会缓存引擎以提高性能。
    """
    if not task.source_database:
        # 在加载任务时应使用 joinedload('source_database')
        # 如果没有，我们必须在 ConfigSession 中手动加载它
        with ConfigSession() as config_session:
            db_info = config_session.query(DatabaseInfo).get(task.source_db_id)
            if not db_info:
                raise ValueError(f"task_id:[{task.task_id}] Source DatabaseInfo (ID: {task.source_db_id}) not found.")
    else:
        db_info = task.source_database

    db_id = db_info.id

    # 检查缓存
    if db_id not in dynamic_engine_cache:
        print(f"task_id:[{task.task_id}] Creating new dynamic engine for source DB: {db_info.db_show_name} (ID: {db_id})")
        # 构建连接字符串
        db_url = (
            f"{db_info.db_type}://{db_info.db_user}:{quote_plus(db_info.db_password)}@"
            f"{db_info.db_host}:{db_info.db_port}/{db_info.db_name}?{db_info.db_args}"
        )

        engine = create_engine(db_url, pool_recycle=3600, connect_args=DB_CONNECT_ARGS)
        session_local = sessionmaker(bind=engine)

        # 缓存引擎和会话工厂
        dynamic_engine_cache[db_id] = (engine, session_local)

    # 从缓存中获取会话工厂
    _, session_local = dynamic_engine_cache[db_id]

    session = session_local()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_dynamic_engine(task: SyncTask):
    """获取动态引擎（主要用于 inspect）"""
    # 确保引擎在缓存中
    with get_dynamic_session(task):
        pass
    # 从缓存返回引擎
    return dynamic_engine_cache[task.source_db_id][0]


class FieldMappingService:
    """
    处理字段映射的缓存和更新
    """

    @retry()
    def get_payload_mapping(self, config_session: Session, task_id: int) -> dict:
        """
        获取用于构建 API *payload* 的映射。
        返回: { 'widget_alias': 'jdy_widget_name' }
               (e.g., { 'name': '_widget_123' })
        """
        mappings = config_session.query(FormFieldMapping).filter_by(task_id=task_id).all()

        result = {}
        for m in mappings:
            # 判断 widget_alias 是否是 _widget_数字 的格式
            if m.widget_alias and m.widget_alias.startswith('_widget_') and m.widget_alias[8:].isdigit():
                # 如果是 _widget_数字 格式，使用 m.label 作为 表字段名
                key, value = m.label, m.widget_name
            else:
                # 如果不是，使用 m.widget_alias 作为 表字段名
                # key, value = m.widget_alias, m.widget_alias
                key, value = m.widget_alias, m.widget_name

            result[key] = value

        return result

    @retry()
    def get_alias_mapping(self, config_session: Session, task_id: int) -> dict:
        """
        获取用于 API *查询过滤* 的映射。
        返回: { 'mysql_column_name': 'jdy_widget_alias' }
               (e.g., { 'name': 'name' })
        """
        mappings = config_session.query(FormFieldMapping).filter_by(task_id=task_id).all()

        result = {}
        for m in mappings:
            # 判断 widget_alias 是否是 _widget_数字 的格式
            if m.widget_alias and m.widget_alias.startswith('_widget_') and m.widget_alias[8:].isdigit():
                # 如果是 _widget_数字 格式，使用 m.label
                key, value = m.label, m.widget_name
            else:
                # 如果不是，使用 m.widget_alias
                # key, value = m.widget_alias, m.widget_alias
                key, value = m.widget_alias, m.widget_name

            result[key] = value

        return result

    @retry()
    def update_form_fields_mapping(self, config_session: Session, task: SyncTask):
        """
        从简道云 API 更新指定任务的字段映射缓存
        """
        print(f"task_id:[{task.task_id}] Updating field mappings for task...")

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            log_sync_error(
                task_config=task,
                error=ValueError(f"Task {task.task_id} missing department or API key."),
                extra_info="Failed to update field mappings."
            )
            return
        api_key = task.department.jdy_key_info.api_key

        try:
            # 1. 实例化
            form_api = FormApi(api_key=api_key, host=Config.JDY_API_HOST, qps=30)

            # 2. 调用
            response = form_api.get_form_widgets(task.jdy_app_id, task.jdy_entry_id)

            # 3. V5 响应结构
            widgets = response.get('widgets', [])
            data_modify_time_str = response.get("dataModifyTime")  # "2021-09-08T03:40:26.586Z"
            form_name = response.get('name') or response.get('formName', '')

            data_modify_time = None
            if data_modify_time_str:
                try:
                    data_modify_time = datetime.fromisoformat(data_modify_time_str.replace('Z', '+00:00'))
                except ValueError:
                    print(f"task_id:[{task.task_id}] Warning: Could not parse dataModifyTime '{data_modify_time_str}'.")

            if not widgets:
                print(f"task_id:[{task.task_id}] No widgets found for form.")
                return

            # 4. 删除旧映射
            config_session.query(FormFieldMapping).filter_by(task_id=task.task_id).delete()

            # 5. 插入新映射
            new_mappings = []
            for field in widgets:
                # V5 API 结构
                widget_alias = field.get('name')  # 'name' 字段 (用于查询/匹配)
                widget_name = field.get('widgetName')  # '_widget_xxx' (用于提交)
                label = field.get('label')
                widget_type = field.get('type')

                if not (widget_alias and widget_name and label and widget_type):
                    # 跳过无效的字段 (例如 'SerialId' 可能没有 widget_name)
                    continue

                mapping = FormFieldMapping(
                    task_id=task.task_id,
                    form_name=form_name,
                    widget_name=widget_name,  # _widget_xxx
                    widget_alias=widget_alias,  # name
                    label=label,
                    type=widget_type,
                    data_modify_time=data_modify_time
                )
                new_mappings.append(mapping)

            config_session.add_all(new_mappings)
            config_session.commit()
            print(f"task_id:[{task.task_id}] Successfully updated {len(new_mappings)} field mappings.")

        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            config_session.rollback()
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.task_id}] Failed to update field mappings via V5 API."
            )
        except IntegrityError as e:
            config_session.rollback()
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.task_id}] Failed to update mappings due to IntegrityError (e.g., duplicate widget_alias?)."
            )


class SyncService:
    """
    处理核心同步逻辑
    """

    def __init__(self):
        # (移除) 视图缓存
        # self._view_status_cache = {}
        pass

    def _prepare_source_table(self, task: SyncTask):
        """
        检查源表, 如果是物理表 (BASE TABLE) 则添加 _id 字段和索引。
        此方法在任务首次运行前调用。
        """
        print(f"task_id:[{task.task_id}] Preparing source table: {task.source_table}...")

        # 动态获取引擎和会话
        try:
            dynamic_engine = get_dynamic_engine(task)
            with get_dynamic_session(task) as source_conn:  # 使用 connect() 行为

                # db_name 来自 task
                db_name = task.source_database.db_name

                # 1. 检查表是否存在
                inspector = inspect(dynamic_engine)
                if not inspector.has_table(task.source_table):
                    log_sync_error(task_config=task,
                                   extra_info=f"Source table '{task.source_table}' not found in source DB.")
                    return

                # 2. 检查是否为物理表 (BASE TABLE)
                table_type_query = text(
                    "SELECT table_type FROM information_schema.tables "
                    "WHERE table_schema = :db_name AND table_name = :table_name"
                )
                result = source_conn.execute(table_type_query,
                                             {"db_name": db_name, "table_name": task.source_table}).fetchone()

                table_type = result[0] if result else None

                if table_type == 'VIEW':
                    print(f"task_id:[{task.task_id}] Source is a VIEW. Skipping _id column check.")
                    return  # 视图, 正常退出

                if table_type != 'BASE TABLE':
                    log_sync_error(task_config=task,
                                   extra_info=f"Source is not a BASE TABLE (type: {table_type}). Skipping _id column check.")
                    return

                # 3. 检查 `_id` 列是否存在
                columns = [col['name'] for col in inspector.get_columns(task.source_table)]
                if '_id' not in columns:
                    print(f"task_id:[{task.task_id}] Adding `_id` column to table '{task.source_table}'...")
                    try:
                        # 提交在会话级别处理
                        source_conn.execute(
                            text(f"ALTER TABLE `{task.source_table}` ADD COLUMN `_id` VARCHAR(50) NULL DEFAULT NULL"))
                        source_conn.execute(text(f"ALTER TABLE `{task.source_table}` ADD INDEX `idx__id` (`_id`)"))
                        source_conn.commit()
                        print(f"task_id:[{task.task_id}] Successfully added `_id` column and index.")
                    except Exception as alter_e:
                        source_conn.rollback()
                        log_sync_error(task_config=task, error=alter_e,
                                       extra_info=f"Failed to add `_id` column to '{task.source_table}'.")
                else:
                    print(f"task_id:[{task.task_id}] `_id` column already exists.")

        except NoSuchTableError:
            log_sync_error(task_config=task,
                           extra_info=f"Source table '{task.source_table}' not found (NoSuchTableError).")
        except Exception as e:
            log_sync_error(task_config=task, error=e, extra_info=f"Error preparing source table '{task.source_table}'.")

    def _is_view(self, task: SyncTask) -> bool:
        """
        检查源表是否是一个视图 (VIEW)
        """
        # # (移除) 实例缓存
        # cache_key = task.source_table
        # if cache_key in self._view_status_cache:
        #     return self._view_status_cache[cache_key]

        try:
            dynamic_engine = get_dynamic_engine(task)
            db_name = task.source_database.db_name

            inspector = inspect(dynamic_engine)

            if not inspector.has_table(task.source_table):
                raise ValueError(f"Source table {task.source_table} does not exist.")

            table_type_query = text(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_schema = :db_name AND table_name = :table_name"
            )
            with dynamic_engine.connect() as conn:  # 使用 engine.connect
                result = conn.execute(table_type_query,
                                      {"db_name": db_name, "table_name": task.source_table}).fetchone()

            is_view = (result and result[0] == 'VIEW')

            # (移除) 缓存
            # self._view_status_cache[cache_key] = is_view

            if is_view:
                print(f"task_id:[{task.task_id}] Source table {task.source_table} is a VIEW.")

            return is_view

        except Exception as e:
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.task_id}] Failed to check if table is view."
            )
            return False  # 默认不是视图以防止意外

    def _transform_row_to_jdy(self, row: dict, payload_map: dict) -> dict:
        """
        将数据库行 (dict) 转换为简道云 API data 负载
        """
        data_payload = {}
        for col_name, value in row.items():
            if col_name in payload_map:
                widget_name = payload_map[col_name]

                processed_value = value

                # 1. 尝试解析是否为 JSON 字符串 (用于子表单, 地址, 成员等)
                if isinstance(value, str) \
                        and value.strip().startswith(('{', '[')) and value.strip().endswith(('}', ']')):
                    try:
                        processed_value = json.loads(value)
                    except json.JSONDecodeError:
                        # 不是有效的 JSON，保持为原始字符串
                        processed_value = value

                # 2. 序列化简单类型 (datetime, decimal)
                #    如果 processed_value 是 list/dict, json_serializer 不会被调用
                #    如果 processed_value 是 简单类型, 它将被正确序列化
                try:
                    # 此步骤确保 datetime, Decimal 等被正确转换为 str/float
                    processed_value = json.loads(json.dumps(processed_value, default=json_serializer))
                except TypeError:
                    # 回退: 适用于不由 json_serializer 处理的复杂对象
                    processed_value = str(processed_value)

                data_payload[widget_name] = {"value": processed_value}

        return data_payload

    def _get_pk_fields_and_values(self, task: SyncTask, row: dict) -> Tuple[List[str], List[Any]]:
        """
        解析复合主键并从行中提取值
        """
        if not task.pk_field_names:
            raise ValueError(f"task_id:[{task.task_id}] pk_field_names is not configured.")

        # pk_field_name 格式 "pk1,pk2,pk3"
        pk_fields = [pk.strip() for pk in task.pk_field_names.split(',')]
        pk_values = []

        for field in pk_fields:
            if field not in row:
                raise ValueError(f"task_id:[{task.task_id}] Composite PK field '{field}' not found in row data.")
            pk_values.append(row[field])

        return pk_fields, pk_values

    @retry()
    def _find_jdy_id_by_pk(
            self,
            task: SyncTask,
            row_dict: dict,  # 传入整行数据
            data_api_query: DataApi,
            data_api_delete: DataApi,
            alias_map: dict
    ) -> str | None:
        """
        通过主键 (PK) 在简道云中查找对应的 _id
        """

        try:
            # 1. 获取复合主键字段和值
            pk_fields, pk_values = self._get_pk_fields_and_values(task, row_dict)

            filter_conditions = []
            log_pk_values = {}  # 用于日志

            # 2. 构建复合查询
            for i, field_name in enumerate(pk_fields):
                if field_name not in alias_map:
                    raise ValueError(f"PK field '{field_name}' not in alias map.")

                jdy_pk_field = alias_map[field_name]
                pk_value = pk_values[i]
                log_pk_values[field_name] = pk_value

                filter_conditions.append({
                    "field": jdy_pk_field,
                    "method": "eq",
                    "value": [pk_value]  # 传入数组
                })

            filter_payload = {
                "rel": "and",
                "cond": filter_conditions
            }

            # 3. V5 API (QPS 30) - 查找所有重复项
            response = data_api_query.query_list_data(
                app_id=task.jdy_app_id,
                entry_id=task.jdy_entry_id,
                limit=100,  # 查找所有重复项 (最多100个)
                fields=["_id"],
                filter=filter_payload
            )

            jdy_data = response.get('data', [])
            log_pk_str = json.dumps(log_pk_values)  # 用于日志

            if not jdy_data:
                return None  # 未找到

            # 4. 主键去重逻辑
            total_deleted = 0

            if len(jdy_data) > 1:
                id_to_keep = jdy_data[0].get('_id')
                ids_to_delete = [d.get('_id') for d in jdy_data[1:] if d.get('_id')]

                log_sync_error(
                    task_config=task,
                    extra_info=f"task_id:[{task.task_id}] Found {len(jdy_data)} duplicate entries for PK {log_pk_str}. Keeping {id_to_keep}, deleting {len(ids_to_delete)}."
                )

                try:
                    # 调用批量删除 (QPS 10)
                    data_ids = [d['_id'] for d in jdy_data]
                    delete_responses = data_api_delete.delete_batch_data(task.jdy_app_id, task.jdy_entry_id,
                                                                         ids_to_delete)
                    success_count = sum(resp.get('success_count', 0) for resp in delete_responses)
                    total_deleted += success_count
                    if success_count != len(data_ids):
                        log_sync_error(task_config=task,
                                       extra_info=f"task_id:[{task.task_id}] Delete mismatch. Requested: {len(data_ids)}, Deleted: {success_count}.")
                except Exception as e:
                    log_sync_error(
                        task_config=task,
                        error=e,
                        extra_info=f"task_id:[{task.task_id}] Failed to delete duplicate entries for PK {log_pk_str}."
                    )

                return id_to_keep  # 返回保留的 ID

            # 5. 正常情况
            return jdy_data[0].get('_id')  # 只有一个, 正常返回

        except Exception as e:  # 捕获包括 ValueError
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.task_id}] V5 API error finding Jdy ID by PK."
            )
            return None

    @retry()
    def _writeback_id_to_source(
            self,
            source_session: Session,
            task: SyncTask,
            jdy_id: str,
            row_dict: dict  # 传入整行数据
    ):
        """
        将简道云 _id 回写到源数据库
        """
        if self._is_view(task):
            # print(f"task_id:[{task.task_id}] Skipping _id writeback for VIEW.")
            return  # 视图不能回写

        try:
            # 1. 获取复合主键字段和值
            pk_fields, pk_values = self._get_pk_fields_and_values(task, row_dict)

            # 2. 构建复合 WHERE 子句
            where_clauses = []
            params = {"jdy_id": jdy_id}
            log_pk_values = {}

            for i, field_name in enumerate(pk_fields):
                param_name = f"pk_val_{i}"
                where_clauses.append(f"`{field_name}` = :{param_name}")
                params[param_name] = pk_values[i]
                log_pk_values[field_name] = pk_values[i]

            where_sql = " AND ".join(where_clauses)

            # 3. 构建并执行
            update_stmt = text(
                f'UPDATE `{task.source_table}` SET `_id` = :jdy_id '
                f'WHERE {where_sql} AND `_id` IS NULL'
            )

            source_session.execute(update_stmt, params)
            source_session.commit()

        except Exception as e:
            source_session.rollback()
            log_pk_str = json.dumps(log_pk_values) if 'log_pk_values' in locals() else "UNKNOWN"
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.task_id}] Failed to writeback _id {jdy_id} to PK {log_pk_str}."
            )

    def _update_task_status(
            self,
            config_session: Session,
            task: SyncTask,
            status: str,
            binlog_file: str = None,
            binlog_pos: int = None,
            last_sync_time: datetime = None,
            is_full_replace_first: bool = None
    ):
        """
        安全地更新任务状态 (使用传入的会话)
        """
        try:
            task.status = status
            if binlog_file:
                task.last_binlog_file = binlog_file
            if binlog_pos:
                task.last_binlog_pos = binlog_pos
            if last_sync_time:
                task.last_sync_time = last_sync_time
            if is_full_replace_first is not None:
                task.is_full_replace_first = is_full_replace_first

            config_session.commit()
        except Exception as e:
            config_session.rollback()
            print(f"task_id:[{task.task_id}] CRITICAL: Failed to update task status to {status}: {e}")

    # --- 公共同步方法 ---
    # --- 首次全量同步的内部方法 ---
    @retry()
    def _run_full_sync(self, config_session: Session, task: SyncTask, delete_first: bool):
        """
        内部全量同步逻辑, 支持SQL过滤和选择性删除
        """
        mode = "FULL_REPLACE" if delete_first else "INITIAL_SYNC"
        print(f"task_id:[{task.task_id}] Running {mode} sync...")

        total_deleted = 0
        total_created = 0

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            raise ValueError(f"Task {task.task_id} missing department or API key for {mode}.")
        api_key = task.department.jdy_key_info.api_key

        try:
            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.task_id)
            if not payload_map:
                raise ValueError("Field mapping is empty.")

            # 1. 实例化
            data_api_query = DataApi(api_key, Config.JDY_API_HOST, qps=30)
            data_api_delete = DataApi(api_key, Config.JDY_API_HOST, qps=10)
            data_api_create = DataApi(api_key, Config.JDY_API_HOST, qps=10)

            # 2. 仅在 delete_first=True 时删除
            if delete_first:
                print(f"task_id:[{task.task_id}] Deleting all data from Jdy...")
                data_id = None
                while True:
                    response = data_api_query.query_list_data(
                        task.jdy_app_id, task.jdy_entry_id,
                        limit=500, data_id=data_id, fields=["_id"]
                    )
                    jdy_data = response.get('data', [])
                    if not jdy_data:
                        break

                    data_ids = [d['_id'] for d in jdy_data]
                    delete_responses = data_api_delete.delete_batch_data(task.jdy_app_id, task.jdy_entry_id, data_ids)

                    # delete_batch_data 返回一个列表，需要对列表中的每个响应求和
                    success_count = sum(resp.get('success_count', 0) for resp in delete_responses)
                    total_deleted += success_count
                    if success_count != len(data_ids):
                        log_sync_error(task_config=task,
                                       extra_info=f"task_id:[{task.task_id}] Delete mismatch. Requested: {len(data_ids)}, Deleted: {success_count}.")

                    data_id = jdy_data[-1]['_id']
                print(f"task_id:[{task.task_id}] Jdy data deleted ({total_deleted} items). Fetching from source DB...")
            else:
                print(f"task_id:[{task.task_id}] Skipping deletion for {mode}.")

            # 3. 构建带 SQL 过滤的查询
            with get_dynamic_session(task) as source_session:
                base_query = f"SELECT * FROM `{task.source_table}`"
                params = {}
                if task.source_filter_sql:
                    base_query += f" WHERE {task.source_filter_sql}"

                rows = source_session.execute(text(base_query), params).mappings().all()

            # 4. 批量创建数据
            batch_data = []
            for row in rows:
                row_dict = dict(row)
                data_payload = self._transform_row_to_jdy(row_dict, payload_map)
                if data_payload:
                    batch_data.append(data_payload)

                if len(batch_data) >= 100:  # API 限制 100
                    trans_id = str(uuid.uuid4())
                    responses = data_api_create.create_batch_data(
                        task.jdy_app_id, task.jdy_entry_id,
                        data_list=batch_data, transaction_id=trans_id
                    )
                    # 密码create_batch_data 返回一个列表，需要对列表中的每个响应求和
                    success_count = sum(resp.get('success_count', 0) for resp in responses)
                    total_created += success_count
                    if success_count != len(batch_data):
                        log_sync_error(task_config=task,
                                       extra_info=f"task_id:[{task.task_id}] Create mismatch. Req: {len(batch_data)}, Created: {success_count}. TransID: {trans_id}")
                    batch_data = []

            if batch_data:
                trans_id = str(uuid.uuid4())
                responses = data_api_create.create_batch_data(
                    task.jdy_app_id, task.jdy_entry_id,
                    data_list=batch_data, transaction_id=trans_id
                )
                # 密码create_batch_data 返回一个列表，需要对列表中的每个响应求和
                success_count = sum(resp.get('success_count', 0) for resp in responses)
                total_created += success_count
                if success_count != len(batch_data):
                    log_sync_error(task_config=task,
                                   extra_info=f"task_id:[{task.task_id}] Create mismatch. Req: {len(batch_data)}, Created: {success_count}. TransID: {trans_id}")

            print(
                f"task_id:[{task.task_id}] {mode} sync completed. Source rows: {len(rows)}, Created in Jdy: {total_created}.")
            if len(rows) != total_created:
                log_sync_error(task_config=task,
                               extra_info=f"task_id:[{task.task_id}] FINAL COUNT MISMATCH. Source: {len(rows)}, Created: {total_created}.")

            # 更新时间, 但不更新状态 (由调用者更新)
            self._update_task_status(config_session, task,
                                     status=task.status,  # 保持状态不变
                                     last_sync_time=datetime.now(TZ_UTC_8))

        except Exception as e:
            # 不更新状态, 只记录日志, 抛出异常
            log_sync_error(task_config=task, error=e, extra_info=f"task_id:[{task.task_id}] {mode} failed.")
            raise e  # 抛出异常, 让调用者处理状态

    @retry()
    def run_full_replace(self, config_session: Session, task: SyncTask):
        """
        执行全量替换同步
        (视图检查, 事务 ID, 修复响应逻辑)
        """
        # 视图支持全量覆盖
        # if self._is_view(task):
        #     log_sync_error(task_config=task,
        #                    extra_info=f"task_id:[{task.task_id}] FULL_REPLACE mode is not allowed for VIEWS. Skipping task.")
        #     return

        print(f"task_id:[{task.task_id}] Running FULL_REPLACE sync (Scheduled)...")
        self._update_task_status(config_session, task, status='running')

        try:
            # 调用新的内部方法, 强制删除
            self._run_full_sync(config_session, task, delete_first=True)
            # 成功, 设置为空闲
            self._update_task_status(config_session, task, status='idle')

        except Exception:
            # _run_full_sync 已经记录了日志
            config_session.rollback()
            self._update_task_status(config_session, task, status='error')

    @retry()
    def run_incremental(self, config_session: Session, task: SyncTask):
        """
        执行增量同步 (Upsert)
        (接受会话, 事务 ID, 去重, 复合主键，支持首次全量同步 和 source_filter_sql)
        """
        print(f"task_id:[{task.task_id}] Running INCREMENTAL sync...")
        self._update_task_status(config_session, task, status='running')

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            log_sync_error(
                task_config=task,
                error=ValueError(f"Task {task.task_id} missing department or API key for INCREMENTAL."),
                extra_info="INCREMENTAL failed."
            )
            self._update_task_status(config_session, task, status='error')
            return
        api_key = task.department.jdy_key_info.api_key

        try:
            current_sync_time = datetime.now(TZ_UTC_8)

            # 1. 检查是否需要首次全量同步
            if task.is_full_replace_first:
                print(f"task_id:[{task.task_id}] First run: Executing initial full replace...")
                try:
                    # 调用全量同步
                    self._run_full_sync(config_session, task, delete_first=True)
                    # 成功后, 更新状态并退出
                    self._update_task_status(config_session, task,
                                             status='idle',
                                             last_sync_time=current_sync_time,
                                             is_full_replace_first=False)
                    print(f"task_id:[{task.task_id}] Initial full sync complete.")
                    return  # 本次运行结束
                except Exception as e:
                    # 首次全量同步失败, 保持 is_full_replace_first=True, 设为 error
                    config_session.rollback()
                    self._update_task_status(config_session, task, status='error')
                    return  # 退出

            # 2. 正常增量逻辑
            if not task.incremental_field:
                raise ValueError("Incremental field (e.g., last_modified) is not configured.")

            # # 2a. 动态获取 API Key
            # if not task.department:
            #     raise ValueError(f"Task {task.task_id} missing department/api_key configuration for run_incremental.")
            # api_key = task.department.jdy_key_info.api_key

            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.task_id)
            alias_map = mapping_service.get_alias_mapping(config_session, task.task_id)
            if not payload_map or not alias_map:
                raise ValueError("Field mapping is empty.")

            # 3. 实例化
            data_api_query = DataApi(api_key, Config.JDY_API_HOST, qps=30)
            data_api_delete = DataApi(api_key, Config.JDY_API_HOST, qps=10)  # 用于去重
            data_api_create = DataApi(api_key, Config.JDY_API_HOST, qps=20)  # Single create
            data_api_update = DataApi(api_key, Config.JDY_API_HOST, qps=20)  # Single update

            # 4. 确定时间戳
            last_sync_time = task.last_sync_time or datetime(1970, 1, 1, tzinfo=TZ_UTC_8)

            # 动态创建 source_session 和 engine
            dynamic_engine = get_dynamic_engine(task)
            with get_dynamic_session(task) as source_session:

                # --- 数据探测逻辑 ---
                inspector = inspect(dynamic_engine)
                col_info = None
                try:
                    columns = inspector.get_columns(task.source_table)
                    col_info = next((col for col in columns if col['name'] == task.incremental_field), None)
                except NoSuchTableError:
                    raise ValueError(f"Incremental field's table '{task.source_table}' not found.")

                if not col_info:
                    raise ValueError(
                        f"Incremental field '{task.incremental_field}' not found in table '{task.source_table}'.")

                col_type_name = str(col_info['type']).upper()
                last_sync_time_for_query = None

                # 1. 如果是 DATE 类型，总是截断
                if col_type_name == 'DATE':
                    last_sync_time_for_query = last_sync_time.replace(hour=0, minute=0, second=0, microsecond=0)
                    print(f"task_id:[{task.task_id}] Detected DATE type. Querying >= {last_sync_time_for_query} (Truncated)")

                # 2. 如果是 DATETIME，执行数据探测
                elif col_type_name.startswith('DATETIME'):
                    print(f"task_id:[{task.task_id}] Detected DATETIME type. Probing data ...")
                    is_fake_datetime = False

                    # 探测查询，限制1000条
                    probe_query = text(
                        f"SELECT `{task.incremental_field}` FROM `{task.source_table}` "
                        f"WHERE `{task.incremental_field}` IS NOT NULL LIMIT 1000"
                    )
                    probe_results = source_session.execute(probe_query).fetchall()

                    # 没有数据，无法判断。为安全起见，使用截断（防止丢失数据）
                    if not probe_results:
                        print(f"task_id:[{task.task_id}] No data found for probing.")
                        is_fake_datetime = True
                    else:
                        min_time = time_obj(0, 0, 0)
                        all_are_midnight = True
                        for row in probe_results:
                            dt_val = row[0]
                            if dt_val is not None and dt_val.time() != min_time:
                                # print(f"task_id:[{task.task_id}] Detected DATETIME type is yyyy-MM-dd HH:mm:ss.")
                                all_are_midnight = False
                                break
                        is_fake_datetime = all_are_midnight
                    if is_fake_datetime:
                        print(
                            f"task_id:[{task.task_id}] Probe confirms yyyy-MM-dd 00:00:00 DATETIME format. Using truncated timestamp.")
                        last_sync_time_for_query = last_sync_time.replace(hour=0, minute=0, second=0, microsecond=0)
                    else:
                        print(
                            f"task_id:[{task.task_id}] Probe found yyyy-MM-dd HH:mm:ss DATETIME format. Using exact timestamp.")
                        last_sync_time_for_query = last_sync_time
                else:
                    # 3. 如果是 TIMESTAMP 或其他类型，使用精确时间
                    last_sync_time_for_query = last_sync_time
                    print(f"task_id:[{task.task_id}] Detected {col_type_name} type. Querying >= {last_sync_time_for_query}")

                # 5. 获取源数据 (带 SQL 过滤)
                base_query = (
                    f"SELECT * FROM `{task.source_table}` "
                    f"WHERE `{task.incremental_field}` >= :last_sync_time"
                )
                # 使用动态确定的时间戳
                params = {"last_sync_time": last_sync_time_for_query}
                if task.source_filter_sql:
                    base_query += f" AND ({task.source_filter_sql})"

                rows = source_session.execute(text(base_query), params).mappings().all()

                if not rows:
                    print(f"task_id:[{task.task_id}] No new data found since {last_sync_time_for_query}.")
                    self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)
                    return

                # 6. 遍历并 Upsert
                count_new, count_updated = 0, 0
                for row in rows:
                    row_dict = dict(row)
                    try:
                        self._get_pk_fields_and_values(task, row_dict)
                    except ValueError as e:
                        log_sync_error(task_config=task, error=e,
                                       extra_info=f"task_id:[{task.task_id}] Row missing PK. Skipping.",
                                       payload=row_dict)
                        continue

                    data_payload = self._transform_row_to_jdy(row_dict, payload_map)
                    if not data_payload:
                        log_sync_error(task_config=task,
                                       payload=row_dict,
                                       extra_info=f"task_id:[{task.task_id}] Row missing required fields. Skipping.")
                        continue

                    # 传入 row_dict 以进行复合主键去重
                    jdy_id = row_dict.get('_id') or self._find_jdy_id_by_pk(
                        task, row_dict,
                        data_api_query, data_api_delete, alias_map
                    )
                    trans_id = str(uuid.uuid4())
                    if jdy_id:
                        # 更新
                        update_response = data_api_update.update_single_data(
                            task.jdy_app_id, task.jdy_entry_id, jdy_id,
                            data_payload, transaction_id=trans_id
                        )
                        update_jdy_id = update_response.get('data', {}).get('_id')
                        if not update_jdy_id:
                            log_sync_error(task_config=task,
                                           payload=data_payload,
                                           error=update_response,
                                           extra_info=f"task_id:[{task.task_id}] Failed to update data.")
                        else:
                            count_updated += 1
                    else:
                        # 新增
                        create_response = data_api_create.create_single_data(
                            task.jdy_app_id, task.jdy_entry_id,
                            data_payload, transaction_id=trans_id
                        )
                        new_jdy_id = create_response.get('data', {}).get('_id')
                        if not new_jdy_id:
                            log_sync_error(task_config=task,
                                           payload=data_payload,
                                           error=create_response,
                                           extra_info=f"task_id:[{task.task_id}] Failed to create data.")
                        # 是否需要回写，有待商榷，实际可不用回写
                        # if new_jdy_id:
                        #     count_new += 1
                        #     # 传入 row_dict 以进行复合主键回写
                        #     self._writeback_id_to_source(source_session, task, new_jdy_id, row_dict)

            print(f"task_id:[{task.task_id}] INCREMENTAL sync completed. New: {count_new}, Updated: {count_updated}.")
            self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)

        except Exception as e:
            config_session.rollback()
            log_sync_error(task_config=task, error=e, extra_info=f"task_id:[{task.task_id}] INCREMENTAL failed.")
            self._update_task_status(config_session, task, status='error')

    @retry()
    def run_binlog_listener(self, task: SyncTask):
        """
        运行一个长连接的 Binlog 监听器
        (独立创建会话, 事务 ID, 去重, 复合主键，支持首次全量同步)
        """
        if self._is_view(task):
            log_sync_error(task_config=task,
                           extra_info=f"task_id:[{task.task_id}] BINLOG mode is not allowed for VIEWS. Stopping listener.")
            return

        thread_name = f"BinlogListener-{task.task_id}"
        current_thread().name = thread_name
        print(f"[{thread_name}] Starting...")

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            log_sync_error(
                task_config=task,
                error=ValueError(f"Task {task.task_id} missing department or API key for BINLOG."),
                extra_info="BINLOG listener stopped."
            )
            # 状态将在 run_binlog_listener_in_thread 的 finally 块中被设置为 error
            return
        api_key = task.department.jdy_key_info.api_key

        # 必须加载 source_database 才能获取连接信息
        if not task.source_database:
            with ConfigSession() as config_session:
                task = config_session.query(SyncTask).options(
                    joinedload(SyncTask.source_database)
                ).get(task.task_id)
                if not task.source_database:
                    log_sync_error(task_config=task,
                                   error=ValueError(f"Task {task.task_id} missing source_database link."),
                                   extra_info="BINLOG listener stopped.")
                    return

        db_info = task.source_database

        # 动态构建 Binlog 设置
        dynamic_binlog_settings = {
            "host": db_info.db_host,
            "port": db_info.db_port,
            "user": db_info.db_user,
            "passwd": db_info.db_password  # (注意) quote_plus 是用于 URL的, 这里用原始密码
        }

        data_api_query = DataApi(api_key, Config.JDY_API_HOST, qps=30)
        data_api_delete = DataApi(api_key, Config.JDY_API_HOST, qps=10)  # 用于去重和删除
        data_api_create = DataApi(api_key, Config.JDY_API_HOST, qps=20)
        data_api_update = DataApi(api_key, Config.JDY_API_HOST, qps=20)

        stream = None
        try:
            # 2. 检查是否需要首次全量同步 (在启动监听器之前)
            if task.is_full_replace_first:
                print(f"[{thread_name}] First run: Executing initial full replace...")
                try:
                    with ConfigSession() as config_session:
                        self._run_full_sync(config_session, task, delete_first=True)

                        # 成功后, 更新状态
                        self._update_task_status(config_session, task,
                                                 status='running',  # 保持 running, 因为我们要继续启动 binlog
                                                 last_sync_time=datetime.now(TZ_UTC_8),
                                                 is_full_replace_first=False)
                        print(f"[{thread_name}] Initial full sync complete. Proceeding to binlog...")
                except Exception as e:
                    # 首次全量同步失败, 记录日志, 将任务设为 error 并退出线程
                    log_sync_error(task_config=task, error=e,
                                   extra_info=f"[{thread_name}] Initial full sync failed. Stopping binlog listener.")
                    with ConfigSession() as config_session:
                        self._update_task_status(config_session, task, status='error')
                    return  # 退出线程

            # 3. 在线程启动时创建一次性的 ConfigSession 来更新状态 (如果上面没运行)
            if not task.is_full_replace_first:  # 仅在非首次运行时
                with ConfigSession() as session:
                    self._update_task_status(session, task, status='running')
                    session.refresh(task)  # 确保 task 对象是最新的

            # 4. 独立创建会话和映射 (在循环外)
            # Binlog 线程需要自己的会话
            with ConfigSession() as config_session:
                mapping_service = FieldMappingService()
                payload_map = mapping_service.get_payload_mapping(config_session, task.task_id)
                alias_map = mapping_service.get_alias_mapping(config_session, task.task_id)

            if not payload_map or not alias_map:
                raise ValueError(f"[{thread_name}] Field mapping is empty. Stopping.")

            stream = BinLogStreamReader(
                connection_settings=dynamic_binlog_settings,
                server_id=100 + task.task_id,  # 唯一的 server_id
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                only_tables=[task.source_table],
                only_schemas=[db_info.db_name],
                log_file=task.last_binlog_file,
                log_pos=task.last_binlog_pos,
                resume_stream=True,
                blocking=True,
                skip_to_timestamp=task.last_sync_time.timestamp() if task.last_sync_time else None
            )

            print(f"[{thread_name}] Listening for binlog events...")

            for binlog_event in stream:
                log_file = stream.log_file
                log_pos = stream.log_pos
                try:
                    # 5. 在循环内部为 *每个事件* 创建短暂的会话
                    # 动态创建 source_session
                    with ConfigSession() as config_session, get_dynamic_session(task) as source_session:
                        current_task_state = config_session.query(SyncTask).get(task.task_id)
                        if not current_task_state or not current_task_state.is_active:
                            print(f"[{thread_name}] Task disabled. Stopping listener.")
                            break  # 退出 for 循环

                        for row in binlog_event.rows:
                            trans_id = str(uuid.uuid4())  # 每个 row 操作都是一个事务

                            if isinstance(binlog_event, WriteRowsEvent):
                                data_payload = self._transform_row_to_jdy(row['values'], payload_map)

                                if not data_payload:
                                    log_sync_error(task_config=task,
                                                   payload=row['values'],
                                                   extra_info=f"task_id:[{task.task_id}] Row missing required fields. Skipping.")
                                    continue

                                create_response = data_api_create.create_single_data(
                                    task.jdy_app_id, task.jdy_entry_id,
                                    data_payload, transaction_id=trans_id
                                )
                                new_jdy_id = create_response.get('data', {}).get('_id')
                                if not new_jdy_id:
                                    log_sync_error(task_config=task,
                                                   payload=data_payload,
                                                   error=create_response,
                                                   extra_info=f"[{thread_name}] Failed to create data.")
                                # binlog 模式不需要回写_id, 会导致binlog被重复激发
                                # if new_jdy_id:
                                #     # 传入 row['values']
                                #     self._writeback_id_to_source(source_session, task, new_jdy_id, row['values'])
                                # print(f"[{thread_name}] Created data.")

                            elif isinstance(binlog_event, UpdateRowsEvent):
                                # 传入 row['after_values']
                                jdy_id = row['after_values'].get('_id') or self._find_jdy_id_by_pk(
                                    task, row['after_values'],
                                    data_api_query, data_api_delete, alias_map
                                )

                                if jdy_id:
                                    data_payload = self._transform_row_to_jdy(row['after_values'], payload_map)

                                    if not data_payload:
                                        log_sync_error(task_config=task,
                                                       payload=row['after_values'],
                                                       extra_info=f"task_id:[{task.task_id}] Row missing required fields. Skipping.")
                                        continue

                                    update_response = data_api_update.update_single_data(
                                        task.jdy_app_id, task.jdy_entry_id, jdy_id,
                                        data_payload, transaction_id=trans_id
                                    )
                                    update_jdy_id = update_response.get('data', {}).get('_id')
                                    if not update_jdy_id:
                                        log_sync_error(task_config=task,
                                                       payload=data_payload,
                                                       error=update_response,
                                                       extra_info=f"task_id:[{task.task_id}] Failed to update data.")
                                    else:
                                        print(f"[{thread_name}] Updated data.")
                                else:
                                    # log_sync_error(task_config=task,
                                    #                extra_info=f"[{thread_name}] Update event skipped: Jdy ID not found.",
                                    #                payload=row['after_values'])
                                    # 简道云中没有，则新增
                                    data_payload = self._transform_row_to_jdy(row['after_values'], payload_map)
                                    if not data_payload:
                                        log_sync_error(task_config=task,
                                                       payload=row['after_values'],
                                                       extra_info=f"task_id:[{task.task_id}] Row missing required fields. Skipping.")
                                        continue

                                    create_response = data_api_create.create_single_data(
                                        task.jdy_app_id, task.jdy_entry_id,
                                        data_payload, transaction_id=trans_id
                                    )
                                    new_jdy_id = create_response.get('data', {}).get('_id')
                                    if not new_jdy_id:
                                        log_sync_error(task_config=task,
                                                       payload=data_payload,
                                                       error=create_response,
                                                       extra_info=f"[{thread_name}] Failed to create data.")
                                    else:
                                        print(f"[{thread_name}] Update event: Jdy ID not found, Created data.")

                            elif isinstance(binlog_event, DeleteRowsEvent):
                                # 传入 row['values']
                                jdy_id = row['values'].get('_id') or self._find_jdy_id_by_pk(
                                    task, row['values'],
                                    data_api_query, data_api_delete, alias_map
                                )

                                if jdy_id:
                                    # Delete single 没有 transaction_id
                                    delete_response = data_api_delete.delete_single_data(task.jdy_app_id,
                                                                                         task.jdy_entry_id, jdy_id)
                                    success = delete_response.get('status')
                                    if not success:
                                        log_sync_error(task_config=task,
                                                       payload=row['values'],
                                                       error=delete_response,
                                                       extra_info=f"[{thread_name}] Failed to delete data.")
                                    else:
                                        print(f"[{thread_name}] Deleted data.")
                                else:
                                    log_sync_error(task_config=task,
                                                   extra_info=f"[{thread_name}] Delete event skipped: Jdy ID not found.",
                                                   payload=row['values'])

                        # 6. 事件处理完成后, 在会话内更新位置
                        self._update_task_status(config_session, task, 'running', log_file, log_pos)

                except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as api_err:
                    # API 错误 (例如 404, 500, QPS), 记录日志但不停止监听器
                    log_sync_error(task_config=task, error=api_err,
                                   extra_info=f"[{thread_name}] API error during binlog event processing (will retry).")
                    time.sleep(10)  # 发生 API 错误时暂停
                except OperationalError as db_err:
                    # 数据库连接错误 (例如 'gone away'), 记录日志但不停止监听器
                    log_sync_error(task_config=task, error=db_err,
                                   extra_info=f"[{thread_name}] DB OperationalError (will retry).")
                    time.sleep(10)  # 发生 DB 错误时暂停
                except Exception as event_err:
                    # 其他事件处理错误 (例如数据转换失败), 记录日志并跳过此事件
                    log_sync_error(task_config=task, error=event_err,
                                   extra_info=f"[{thread_name}] Error processing binlog event (skipping).")
                    # 仍然在会话内更新位置, 以跳过错误事件
                    with ConfigSession() as error_session:
                        self._update_task_status(error_session, task, 'running', log_file, log_pos)


        except Exception as e:
            # 这是启动监听器时的严重错误 (例如连接失败)
            log_sync_error(task_config=task, error=e, extra_info=f"[{thread_name}] CRITICAL error. Listener stopped.")
            with ConfigSession() as session:
                self._update_task_status(session, task, status='error')

        finally:
            if stream:
                stream.close()
            print(f"[{thread_name}] Listener shut down.")
            with ConfigSession() as session:
                task_status = session.query(SyncTask).get(task.task_id)
                if task_status and task_status.status == 'running':
                    self._update_task_status(session, task, status='idle')  # 正常关闭
