from datetime import datetime
import json
import time
import uuid
from threading import current_thread
from typing import Tuple, List, Any

import requests
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from sqlalchemy import text, inspect
from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.orm import Session

from app.config import Config
from app.jdy_api import FormApi, DataApi
from app.models import (
    ConfigSession, SourceSession, source_engine,
    SyncTask, FormFieldMapping
)
from app.utils import log_sync_error, json_serializer, TZ_UTC_8, retry

# 缓存视图状态
view_status_cache = {}


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
        # widget_alias 现在是 MySQL 的列名
        return {m.widget_alias: m.widget_name for m in mappings}

    @retry()
    def get_alias_mapping(self, config_session: Session, task_id: int) -> dict:
        """
        获取用于 API *查询过滤* 的映射。
        返回: { 'mysql_column_name': 'jdy_widget_alias' }
               (e.g., { 'name': 'name' })
        """
        mappings = config_session.query(FormFieldMapping).filter_by(task_id=task_id).all()
        # widget_alias 既是 MySQL 列名, 也是 Jdy API filter 的 'name'
        return {m.widget_alias: m.widget_alias for m in mappings}

    @retry()
    def update_form_fields_mapping(self, config_session: Session, task: SyncTask):
        """
        从简道云 API 更新指定任务的字段映射缓存
        """
        print(f"[{task.task_id}] Updating field mappings for task...")
        try:
            # 1. 实例化
            form_api = FormApi(api_key=task.jdy_api_key, host=Config.JDY_API_HOST, qps=30)

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
                    print(f"[{task.task_id}] Warning: Could not parse dataModifyTime '{data_modify_time_str}'.")

            if not widgets:
                print(f"[{task.task_id}] No widgets found for form.")
                return

            # 4. 删除旧映射
            config_session.query(FormFieldMapping).filter_by(task_id=task.task_id).delete()

            # 5. 插入新映射
            new_mappings = []
            for field in widgets:
                # 假设 V5 API 结构
                widget_alias = field.get('name')  # 'name' 字段 (用于查询/匹配)
                widget_name = field.get('widgetName')  # '_widget_xxx_' (用于提交)
                label = field.get('label')
                widget_type = field.get('type')

                if not (widget_alias and widget_name and label and widget_type):
                    # 跳过无效的字段 (例如 'SerialId' 可能没有 widget_name)
                    continue

                mapping = FormFieldMapping(
                    task_id=task.task_id,
                    form_name=form_name,
                    widget_name=widget_name,  # _widget_xxx_
                    widget_alias=widget_alias,  # name
                    label=label,
                    widget_type=widget_type,
                    data_modify_time=data_modify_time
                )
                new_mappings.append(mapping)

            config_session.add_all(new_mappings)
            config_session.commit()
            print(f"[{task.task_id}] Successfully updated {len(new_mappings)} field mappings.")

        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            config_session.rollback()
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"[{task.task_id}] Failed to update field mappings via V5 API."
            )
        except IntegrityError as e:
            config_session.rollback()
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"[{task.task_id}] Failed to update mappings due to IntegrityError (e.g., duplicate widget_alias?)."
            )


class SyncService:
    """
    处理核心同步逻辑
    """

    def __init__(self):
        self._view_status_cache = {}  # 实例级别的缓存

    def _is_view(self, task: SyncTask) -> bool:
        """
        检查源表是否是一个视图 (VIEW)
        """
        cache_key = task.source_table
        if cache_key in self._view_status_cache:
            return self._view_status_cache[cache_key]

        try:
            inspector = inspect(source_engine)
            db_name = Config.SOURCE_DB_NAME  # 从 Config 获取

            if not inspector.has_table(task.source_table):
                raise ValueError(f"Source table {task.source_table} does not exist.")

            table_type_query = text(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_schema = :db_name AND table_name = :table_name"
            )
            with source_engine.connect() as conn:
                result = conn.execute(table_type_query,
                                      {"db_name": db_name, "table_name": task.source_table}).fetchone()

            is_view = (result and result[0] == 'VIEW')
            self._view_status_cache[cache_key] = is_view

            if is_view:
                print(f"[{task.task_id}] Source table {task.source_table} is a VIEW.")

            return is_view

        except Exception as e:
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"[{task.task_id}] Failed to check if table is view."
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
            raise ValueError(f"[{task.task_id}] pk_field_name is not configured.")

        # pk_field_name 格式 "pk1,pk2,pk3"
        pk_fields = [pk.strip() for pk in task.pk_field_names.split(',')]
        pk_values = []

        for field in pk_fields:
            if field not in row:
                raise ValueError(f"[{task.task_id}] Composite PK field '{field}' not found in row data.")
            pk_values.append(row[field])

        return pk_fields, pk_values

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
            if len(jdy_data) > 1:
                id_to_keep = jdy_data[0].get('_id')
                ids_to_delete = [d.get('_id') for d in jdy_data[1:] if d.get('_id')]

                log_sync_error(
                    task_config=task,
                    extra_info=f"[{task.task_id}] Found {len(jdy_data)} duplicate entries for PK {log_pk_str}. Keeping {id_to_keep}, deleting {len(ids_to_delete)}."
                )

                try:
                    # 调用批量删除 (QPS 10)
                    data_api_delete.delete_batch_data(task.jdy_app_id, task.jdy_entry_id, ids_to_delete)
                except Exception as e:
                    log_sync_error(
                        task_config=task,
                        error=e,
                        extra_info=f"[{task.task_id}] Failed to delete duplicate entries for PK {log_pk_str}."
                    )

                return id_to_keep  # 返回保留的 ID

            # 5. 正常情况
            return jdy_data[0].get('_id')  # 只有一个, 正常返回

        except Exception as e:  # 捕获包括 ValueError
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"[{task.task_id}] V5 API error finding Jdy ID by PK."
            )
            return None

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
            # print(f"[{task.task_id}] Skipping _id writeback for VIEW.")
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
                extra_info=f"[{task.task_id}] Failed to writeback _id {jdy_id} to PK {log_pk_str}."
            )

    def _update_task_status(
            self,
            config_session: Session,
            task: SyncTask,
            status: str,
            binlog_file: str = None,
            binlog_pos: int = None,
            last_sync_time: datetime = None
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

            config_session.commit()
        except Exception as e:
            config_session.rollback()
            print(f"[{task.task_id}] CRITICAL: Failed to update task status to {status}: {e}")

    # --- 公共同步方法 ---
    @retry()
    def run_full_replace(self, config_session: Session, source_session: Session, task: SyncTask):
        """
        执行全量替换同步
        (视图检查, 事务 ID, 修复响应逻辑)
        """
        if self._is_view(task):
            log_sync_error(task_config=task,
                           extra_info=f"[{task.task_id}] FULL_REPLACE mode is not allowed for VIEWS. Skipping task.")
            return

        print(f"[{task.task_id}] Running FULL_REPLACE sync...")
        self._update_task_status(config_session, task, status='running')

        total_deleted = 0
        total_created = 0

        try:
            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.task_id)
            if not payload_map:
                raise ValueError("Field mapping is empty.")

            # 1. 实例化
            data_api_query = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=30)
            data_api_delete = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=10)
            data_api_create = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=10)

            # 2. 删除简道云所有数据
            print(f"[{task.task_id}] Deleting all data from Jdy...")
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
                delete_response = data_api_delete.delete_batch_data(task.jdy_app_id, task.jdy_entry_id, data_ids)

                # 检查 success_count
                success_count = delete_response.get('success_count', 0)
                total_deleted += success_count
                if success_count != len(data_ids):
                    log_sync_error(task_config=task,
                                   extra_info=f"[{task.task_id}] Delete mismatch. Requested: {len(data_ids)}, Deleted: {success_count}.")

                data_id = jdy_data[-1]['_id']

            print(f"[{task.task_id}] Jdy data deleted ({total_deleted} items). Fetching from source DB...")

            # 3. 获取源数据
            rows = source_session.execute(text(f"SELECT * FROM `{task.source_table}`")).mappings().all()

            # 4. 批量创建数据
            batch_data = []
            for row in rows:
                row_dict = dict(row)
                data_payload = self._transform_row_to_jdy(row_dict, payload_map)
                if data_payload:
                    batch_data.append(data_payload)

                if len(batch_data) >= 100:  # API 限制 100
                    trans_id = str(uuid.uuid4())
                    response = data_api_create.create_batch_data(
                        task.jdy_app_id, task.jdy_entry_id,
                        data_list=batch_data, transaction_id=trans_id
                    )

                    # 检查 success_count
                    success_count = response.get('success_count', 0)
                    total_created += success_count
                    if success_count != len(batch_data):
                        log_sync_error(task_config=task,
                                       extra_info=f"[{task.task_id}] Create mismatch. Requested: {len(batch_data)}, Created: {success_count}. TransID: {trans_id}")

                    batch_data = []

            if batch_data:
                trans_id = str(uuid.uuid4())
                response = data_api_create.create_batch_data(
                    task.jdy_app_id, task.jdy_entry_id,
                    data_list=batch_data, transaction_id=trans_id
                )
                # 检查 success_count
                success_count = response.get('success_count', 0)
                total_created += success_count
                if success_count != len(batch_data):
                    log_sync_error(task_config=task,
                                   extra_info=f"[{task.task_id}] Create mismatch. Requested: {len(batch_data)}, Created: {success_count}. TransID: {trans_id}")

            print(
                f"[{task.task_id}] FULL_REPLACE sync completed. Source rows: {len(rows)}, Created in Jdy: {total_created}.")
            if len(rows) != total_created:
                log_sync_error(task_config=task,
                               extra_info=f"[{task.task_id}] FINAL COUNT MISMATCH. Source: {len(rows)}, Created: {total_created}.")

            self._update_task_status(config_session, task, status='idle',
                                     last_sync_time=datetime.now(TZ_UTC_8))

        except Exception as e:
            config_session.rollback()  # 确保状态回滚
            log_sync_error(task_config=task, error=e, extra_info=f"[{task.task_id}] FULL_REPLACE failed.")
            self._update_task_status(config_session, task, status='error')

    @retry()
    def run_incremental(self, config_session: Session, source_session: Session, task: SyncTask):
        """
        执行增量同步 (Upsert)
        (接受会话, 事务 ID, 去重, 复合主键)
        """
        print(f"[{task.task_id}] Running INCREMENTAL sync...")
        self._update_task_status(config_session, task, status='running')

        try:
            if not task.incremental_field:
                raise ValueError("Incremental field (e.g., last_modified) is not configured.")

            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.task_id)
            alias_map = mapping_service.get_alias_mapping(config_session, task.task_id)
            if not payload_map or not alias_map:
                raise ValueError("Field mapping is empty.")

            # 1. 实例化
            data_api_query = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=30)
            data_api_delete = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=10)  # 用于去重
            data_api_create = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=20)  # Single create
            data_api_update = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=20)  # Single update

            # 2. 确定时间戳
            last_sync = task.last_sync_time or datetime(1970, 1, 1, tzinfo=TZ_UTC_8)
            current_sync_time = datetime.now(TZ_UTC_8)

            # 3. 获取源数据
            query = text(
                f"SELECT * FROM `{task.source_table}` "
                f"WHERE `{task.incremental_field}` >= :last_sync"
            )
            rows = source_session.execute(query, {"last_sync": last_sync}).mappings().all()

            if not rows:
                print(f"[{task.task_id}] No new data found since {last_sync}.")
                self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)
                return

            # 4. 遍历并 Upsert
            count_new, count_updated = 0, 0
            for row in rows:
                row_dict = dict(row)

                # 检查复合主键
                try:
                    self._get_pk_fields_and_values(task, row_dict)
                except ValueError as e:
                    log_sync_error(task_config=task, error=e, extra_info=f"[{task.task_id}] Row missing PK. Skipping.",
                                   payload=row_dict)
                    continue

                data_payload = self._transform_row_to_jdy(row_dict, payload_map)

                # 传入 row_dict 以进行复合主键去重
                jdy_id = row_dict.get('_id') or self._find_jdy_id_by_pk(
                    task, row_dict,
                    data_api_query, data_api_delete, alias_map
                )

                trans_id = str(uuid.uuid4())
                if jdy_id:
                    # 更新
                    data_api_update.update_single_data(
                        task.jdy_app_id, task.jdy_entry_id, jdy_id,
                        data_payload, transaction_id=trans_id
                    )
                    count_updated += 1
                else:
                    # 新增
                    response = data_api_create.create_single_data(
                        task.jdy_app_id, task.jdy_entry_id,
                        data_payload, transaction_id=trans_id
                    )
                    new_jdy_id = response.get('data', {}).get('_id')
                    if new_jdy_id:
                        count_new += 1
                        # 传入 row_dict 以进行复合主键回写
                        self._writeback_id_to_source(source_session, task, new_jdy_id, row_dict)

            print(f"[{task.task_id}] INCREMENTAL sync completed. New: {count_new}, Updated: {count_updated}.")
            self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)

        except Exception as e:
            config_session.rollback()
            log_sync_error(task_config=task, error=e, extra_info=f"[{task.task_id}] INCREMENTAL failed.")
            self._update_task_status(config_session, task, status='error')

    @retry()
    def run_binlog_listener(self, task: SyncTask):
        """
        运行一个长连接的 Binlog 监听器
        (独立创建会话, 事务 ID, 去重, 复合主键)
        """
        if self._is_view(task):
            log_sync_error(task_config=task,
                           extra_info=f"[{task.task_id}] BINLOG mode is not allowed for VIEWS. Stopping listener.")
            return

        thread_name = f"BinlogListener-{task.task_id}"
        current_thread().name = thread_name
        print(f"[{thread_name}] Starting...")

        # 1. 实例化
        # 注意: 实例化一次, 在循环中使用
        data_api_query = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=30)
        data_api_delete = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=10)  # 用于去重和删除
        data_api_create = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=20)
        data_api_update = DataApi(task.jdy_api_key, Config.JDY_API_HOST, qps=20)

        stream = None
        try:
            # 2. 在线程启动时创建一次性的 ConfigSession 来更新状态
            with ConfigSession() as session:
                self._update_task_status(session, task, status='running')
                # 重新加载 task 对象以获取最新状态
                session.refresh(task)

            # 3. 独立创建会话和映射 (在循环外)
            # Binlog 线程需要自己的会话
            with ConfigSession() as config_session:
                mapping_service = FieldMappingService()
                payload_map = mapping_service.get_payload_mapping(config_session, task.task_id)
                alias_map = mapping_service.get_alias_mapping(config_session, task.task_id)

            if not payload_map or not alias_map:
                raise ValueError(f"[{thread_name}] Field mapping is empty. Stopping.")

            stream = BinLogStreamReader(
                connection_settings=Config.BINLOG_MYSQL_SETTINGS,
                server_id=100 + task.task_id,  # 唯一的 server_id
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                only_tables=[task.source_table],
                only_schemas=[Config.SOURCE_DB_NAME],
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

                # 4. 在循环内部为 *每个事件* 创建短暂的会话
                try:
                    with ConfigSession() as config_session, SourceSession() as source_session:

                        # 重新加载 task 以检查 is_active
                        current_task_state = config_session.query(SyncTask).get(task.task_id)
                        if not current_task_state or not current_task_state.is_active:
                            print(f"[{thread_name}] Task disabled. Stopping listener.")
                            break  # 退出 for 循环

                        for row in binlog_event.rows:
                            trans_id = str(uuid.uuid4())  # 每个 row 操作都是一个事务

                            if isinstance(binlog_event, WriteRowsEvent):
                                data_payload = self._transform_row_to_jdy(row['values'], payload_map)
                                response = data_api_create.create_single_data(
                                    task.jdy_app_id, task.jdy_entry_id,
                                    data_payload, transaction_id=trans_id
                                )
                                new_jdy_id = response.get('data', {}).get('_id')
                                if new_jdy_id:
                                    # 传入 row['values']
                                    self._writeback_id_to_source(source_session, task, new_jdy_id, row['values'])
                                print(f"[{thread_name}] Created data.")

                            elif isinstance(binlog_event, UpdateRowsEvent):
                                # 传入 row['after_values']
                                jdy_id = row['after_values'].get('_id') or self._find_jdy_id_by_pk(
                                    task, row['after_values'],
                                    data_api_query, data_api_delete, alias_map
                                )

                                if jdy_id:
                                    data_payload = self._transform_row_to_jdy(row['after_values'], payload_map)
                                    data_api_update.update_single_data(
                                        task.jdy_app_id, task.jdy_entry_id, jdy_id,
                                        data_payload, transaction_id=trans_id
                                    )
                                    print(f"[{thread_name}] Updated data.")
                                else:
                                    log_sync_error(task_config=task,
                                                   extra_info=f"[{thread_name}] Update event skipped: Jdy ID not found.",
                                                   payload=row['after_values'])

                            elif isinstance(binlog_event, DeleteRowsEvent):
                                # 传入 row['values']
                                jdy_id = row['values'].get('_id') or self._find_jdy_id_by_pk(
                                    task, row['values'],
                                    data_api_query, data_api_delete, alias_map
                                )

                                if jdy_id:
                                    # Delete single 没有 transaction_id
                                    data_api_delete.delete_single_data(task.jdy_app_id, task.jdy_entry_id, jdy_id)
                                    print(f"[{thread_name}] Deleted data.")
                                else:
                                    log_sync_error(task_config=task,
                                                   extra_info=f"[{thread_name}] Delete event skipped: Jdy ID not found.",
                                                   payload=row['values'])

                        # 5. 事件处理完成后, 在会话内更新位置
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
