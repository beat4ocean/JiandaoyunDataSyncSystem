# -*- coding: utf-8 -*-
import json
import logging
import re
import time
import uuid
from datetime import datetime, time as time_obj
from threading import current_thread
from typing import Tuple, List, Any

import requests
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import HeartbeatLogEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from sqlalchemy import text, inspect
from sqlalchemy.exc import OperationalError, IntegrityError, NoSuchTableError
from sqlalchemy.orm import Session, joinedload

from app.config import Config
from app.database import get_dynamic_engine, get_dynamic_session
from app.jdy_api import FormApi, DataApi
from app.models import ConfigSession, SyncTask, FormFieldMapping, Department
from app.utils import json_serializer, TZ_UTC_8, retry, log_sync_error

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
            if m.widget_alias and m.widget_alias.startswith('_widget_'):
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
            if m.widget_alias and m.widget_alias.startswith('_widget_'):
                # 如果是 _widget_数字 格式，使用 m.label
                key, value = m.label, m.widget_name
            else:
                # 如果不是，必须使用 m.widget_alias
                key, value = m.widget_alias, m.widget_alias
                # key, value = m.widget_alias, m.widget_name

            result[key] = value

        return result

    @retry()
    def update_form_fields_mapping(self, config_session: Session, task: SyncTask):
        """
        从简道云 API 更新指定任务的字段映射缓存
        """
        logger.info(f"task_id:[{task.id}] Updating field mappings for task...")

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            log_sync_error(
                task_config=task,
                error=ValueError(f"Task {task.id} missing department or API key."),
                extra_info=f"task_id:[{task.id}] Failed to update field mappings."
            )
            return
        api_key = task.department.jdy_key_info.api_key

        try:
            # 1. 实例化
            form_api = FormApi(api_key=api_key, host=Config.JDY_API_BASE_URL, qps=30)

            # 2. 调用
            response = form_api.get_form_widgets(task.app_id, task.entry_id)

            # 3. V5 响应结构
            widgets = response.get('widgets', [])
            data_modify_time_str = response.get("dataModifyTime")  # "2021-09-08T03:40:26.586Z"
            form_name = response.get('name') or response.get('formName', '')

            data_modify_time = None
            if data_modify_time_str:
                try:
                    data_modify_time = datetime.fromisoformat(data_modify_time_str.replace('Z', '+00:00'))
                except ValueError:
                    logger.warning(f"task_id:[{task.id}] Could not parse dataModifyTime '{data_modify_time_str}'.")

            if not widgets:
                logger.info(f"task_id:[{task.id}] No widgets found for form.")
                return

            # 4. 删除旧映射
            config_session.query(FormFieldMapping).filter_by(task_id=task.id).delete()

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
                    task_id=task.id,
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
            logger.info(f"task_id:[{task.id}] Successfully updated {len(new_mappings)} field mappings.")

        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            config_session.rollback()
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.id}] Failed to update field mappings via V5 API."
            )
        except IntegrityError as e:
            config_session.rollback()
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.id}] Failed to update mappings due to IntegrityError (e.g., duplicate widget_alias?)."
            )


class Db2JdySyncService:
    """
    处理核心同步逻辑
    """

    def __init__(self):
        # (移除) 视图缓存
        # self._view_status_cache = {}
        pass

    def _prepare_table(self, task: SyncTask):
        """
        检查源表, 如果是物理表 (BASE TABLE) 则添加 _id 字段和索引。
        此方法在任务首次运行前调用。
        """
        logger.info(f"task_id:[{task.id}] Preparing source table: {task.table_name}...")

        # 动态获取引擎和会话
        try:
            dynamic_engine = get_dynamic_engine(task)
            with get_dynamic_session(task) as source_conn:  # 使用 connect() 行为

                # db_name 来自 task
                db_name = task.database.db_name

                # 1. 检查表是否存在
                inspector = inspect(dynamic_engine)
                if not inspector.has_table(task.table_name):
                    log_sync_error(task_config=task,
                                   extra_info=f"task_id:[{task.id}] Source table '{task.table_name}' not found in source DB.")
                    return

                # 2. 检查是否为物理表 (BASE TABLE)
                table_type_query = text(
                    "SELECT table_type FROM information_schema.tables "
                    "WHERE table_schema = :db_name AND table_name = :table_name"
                )
                result = source_conn.execute(table_type_query,
                                             {"db_name": db_name, "table_name": task.table_name}).fetchone()

                table_type = result[0] if result else None

                if table_type == 'VIEW':
                    logger.info(f"task_id:[{task.id}] Source is a VIEW. Skipping _id column check.")
                    return  # 视图, 正常退出

                if table_type != 'BASE TABLE':
                    log_sync_error(task_config=task,
                                   extra_info=f"Source is not a BASE TABLE (type: {table_type}). Skipping _id column check.")
                    return

                # 3. 检查 `_id` 列是否存在
                columns = [col['name'] for col in inspector.get_columns(task.table_name)]
                if '_id' not in columns:
                    logger.info(f"task_id:[{task.id}] Adding `_id` column to table '{task.table_name}'...")
                    try:
                        # 提交在会话级别处理
                        source_conn.execute(
                            text(f"ALTER TABLE `{task.table_name}` ADD COLUMN `_id` VARCHAR(50) NULL DEFAULT NULL"))
                        source_conn.execute(text(f"ALTER TABLE `{task.table_name}` ADD INDEX `idx__id` (`_id`)"))
                        source_conn.commit()
                        logger.info(f"task_id:[{task.id}] Successfully added `_id` column and index.")
                    except Exception as alter_e:
                        source_conn.rollback()
                        log_sync_error(task_config=task, error=alter_e,
                                       extra_info=f"task_id:[{task.id}] Failed to add `_id` column to '{task.table_name}'.")
                else:
                    logger.info(f"task_id:[{task.id}] `_id` column already exists.")

        except NoSuchTableError:
            log_sync_error(task_config=task,
                           extra_info=f"task_id:[{task.id}] Source table '{task.table_name}' not found (NoSuchTableError).")
        except Exception as e:
            log_sync_error(task_config=task, error=e,
                           extra_info=f"task_id:[{task.id}] Error preparing source table '{task.table_name}'.")

    def _is_view(self, task: SyncTask) -> bool:
        """
        检查源表是否是一个视图 (VIEW)
        """
        # # (移除) 实例缓存
        # cache_key = task.table_name
        # if cache_key in self._view_status_cache:
        #     return self._view_status_cache[cache_key]

        try:
            dynamic_engine = get_dynamic_engine(task)
            db_name = task.database.db_name

            inspector = inspect(dynamic_engine)

            if not inspector.has_table(task.table_name):
                raise ValueError(f"Source table {task.table_name} does not exist.")

            table_type_query = text(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_schema = :db_name AND table_name = :table_name"
            )
            with dynamic_engine.connect() as conn:  # 使用 engine.connect
                result = conn.execute(table_type_query,
                                      {"db_name": db_name, "table_name": task.table_name}).fetchone()

            is_view = (result and result[0] == 'VIEW')

            # (移除) 缓存
            # self._view_status_cache[cache_key] = is_view

            if is_view:
                logger.info(f"task_id:[{task.id}] Source table {task.table_name} is a VIEW.")

            return is_view

        except Exception as e:
            log_sync_error(
                task_config=task,
                error=e,
                extra_info=f"task_id:[{task.id}] Failed to check if table is view."
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
                        # 如果解析JSON失败 (e.g., "[1,2,3"), 它是一个普通字符串
                        try:
                            processed_value = json_serializer(value)
                        except TypeError:
                            processed_value = str(value)

                # 2. 序列化所有其他类型 (Decimals, AND Date Strings)
                else:
                    try:
                        # 错误写法，无法正确解析字符串str格式的时间
                        # processed_value = json.loads(json.dumps(processed_value, default=json_serializer))
                        # 移除 json.dumps/loads，直接调用 json_serializer
                        processed_value = json_serializer(processed_value)
                    except TypeError:
                        # (如果 serializer 不支持该类型, e.g., list/dict, 回退)
                        processed_value = str(processed_value)

                data_payload[widget_name] = {"value": processed_value}

        return data_payload

    def _get_pk_fields_and_values(self, task: SyncTask, row: dict) -> Tuple[List[str], List[Any]]:
        """
        解析复合主键并从行中提取值
        """
        if not task.business_keys:
            raise ValueError(f"task_id:[{task.id}] business_keys is not configured.")

        # pk_field_name 格式 "pk1,pk2,pk3"
        pk_fields = [pk.strip() for pk in task.business_keys.split(',') if pk and pk.strip()]
        pk_values = []

        for field in pk_fields:
            if field not in row:
                raise ValueError(f"task_id:[{task.id}] Composite PK field '{field}' not found in row data.")
            # 修复 TypeError: Object of type date is not JSON serializable bug
            # pk_values.append(row[field])
            # 错误写法，无法正确解析字符串str格式的时间
            # row_value = json.loads(json.dumps(row[field], default=json_serializer))
            try:
                # 直接调用 serializer，它会正确处理 datetime 对象、
                # date 字符串 (转UTC)、Decimal 和普通字符串
                row_value = json_serializer(row[field])
            except TypeError:
                # 如果 serializer 无法处理 (例如一个 list 或 dict)，则回退
                row_value = str(row[field])

            pk_values.append(row_value)

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

        filter_payload = {}
        log_pk_str = ""

        try:
            # 1. 获取复合主键字段和值
            pk_fields, pk_values = self._get_pk_fields_and_values(task, row_dict)

            filter_conditions = []
            log_pk_values = {}  # 用于日志

            # 2. 构建复合查询
            for i, field_name in enumerate(pk_fields):
                if field_name not in alias_map:
                    raise ValueError(f"task_id:[{task.id}] PK field '{field_name}' not in alias map.")

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
                app_id=task.app_id,
                entry_id=task.entry_id,
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
                    payload=filter_payload,
                    extra_info=f"task_id:[{task.id}] Found {len(jdy_data)} duplicate entries for PK {log_pk_str}. Keeping {id_to_keep}, deleting {len(ids_to_delete)}."
                )

                try:
                    # 调用批量删除 (QPS 10)
                    data_ids = [d['_id'] for d in jdy_data]
                    delete_responses = data_api_delete.delete_batch_data(task.app_id, task.entry_id,
                                                                         ids_to_delete)
                    success_count = sum(resp.get('success_count', 0) for resp in delete_responses)
                    total_deleted += success_count
                    # if success_count != len(data_ids):
                    #     log_sync_error(task_config=task,
                    #                    extra_info=f"task_id:[{task.id}] Delete mismatch. Requested: {len(data_ids)}, Deleted: {success_count}.")
                except Exception as e:
                    log_sync_error(
                        task_config=task,
                        payload=filter_payload,
                        error=e,
                        extra_info=f"task_id:[{task.id}] Failed to delete duplicate entries for PK {log_pk_str}."
                    )

                return id_to_keep  # 返回保留的 ID

            # 5. 正常情况
            return jdy_data[0].get('_id')  # 只有一个, 正常返回

        except Exception as e:  # 捕获包括 ValueError
            log_sync_error(
                task_config=task,
                payload=filter_payload,
                error=e,
                extra_info=f"task_id:[{task.id}] V5 API error finding Jdy _id by PK. Details: {log_pk_str}"
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
            # logger.info(f"task_id:[{task.id}] Skipping _id writeback for VIEW.")
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
                f'UPDATE `{task.table_name}` SET `_id` = :jdy_id '
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
                extra_info=f"task_id:[{task.id}] Failed to writeback _id {jdy_id} to PK {log_pk_str}."
            )

    def _update_task_status(
            self,
            config_session: Session,
            task: SyncTask,
            status: str,
            binlog_file: str = None,
            binlog_pos: int = None,
            last_sync_time: datetime = None,
            is_full_sync_first: bool = None,
            is_delete_first: bool = None,
    ):
        """
        安全地更新任务状态 (使用传入的会话)
        """
        try:
            task.sync_status = status
            if binlog_file:
                task.last_binlog_file = binlog_file
            if binlog_pos:
                task.last_binlog_pos = binlog_pos
            if last_sync_time:
                task.last_sync_time = last_sync_time
            if is_full_sync_first is not None:
                task.is_full_sync_first = is_full_sync_first
            if is_delete_first is not None:
                task.is_delete_first = is_delete_first

            config_session.commit()
        except Exception as e:
            config_session.rollback()
            logger.info(f"task_id:[{task.id}] CRITICAL: Failed to update task status to {status}: {e}")

    # --- 公共同步方法 ---
    # --- 清空简道云表单数据方法
    @retry()
    def _truncate_jdy_data(self, config_session: Session, task: SyncTask):
        """
        清空简道云表单
        """
        # --- 检查任务类型 ---
        if task.sync_type != 'db2jdy':
            logger.error(f"task_id:[{task.id}] _truncate_jdy_form failed: Task type is not 'db2jdy'.")
            return

        logger.info(f"task_id:[{task.id}] Running _truncate_jdy_form ...")

        total_deleted = 0

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            raise ValueError(
                f"task_id:[{task.id}] Task {task.id} missing department or API key for _truncate_jdy_form.")
        api_key = task.department.jdy_key_info.api_key

        try:
            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.id)
            if not payload_map:
                raise ValueError(f"task_id:[{task.id}] Field mapping is empty.")

            # 1. 实例化
            data_api_query = DataApi(api_key, Config.JDY_API_BASE_URL, qps=30)
            data_api_delete = DataApi(api_key, Config.JDY_API_BASE_URL, qps=10)

            # 2. 仅在 delete_first=True 时删除
            logger.info(f"task_id:[{task.id}] Deleting all data from Jdy...")
            data_id = None
            while True:
                response = data_api_query.query_list_data(
                    task.app_id, task.entry_id,
                    limit=100, data_id=data_id, fields=["_id"]
                )
                jdy_data = response.get('data', [])
                if not jdy_data:
                    break

                data_ids = [d['_id'] for d in jdy_data]
                delete_responses = data_api_delete.delete_batch_data(task.app_id, task.entry_id, data_ids)

                # delete_batch_data 返回一个列表，需要对列表中的每个响应求和
                success_count = sum(resp.get('success_count', 0) for resp in delete_responses)
                logger.info(f"task_id:[{task.id}] Deleted {success_count} items.")

                total_deleted += success_count
                # if success_count != len(data_ids):
                #     log_sync_error(task_config=task,
                #                    extra_info=f"task_id:[{task.id}] Delete mismatch. Requested: {len(data_ids)}, Deleted: {success_count}.")

                data_id = jdy_data[-1]['_id']
            logger.info(f"task_id:[{task.id}] Jdy data deleted ({total_deleted} items). Fetching from source DB...")

        except Exception as e:
            # 不更新状态, 只记录日志, 抛出异常
            log_sync_error(task_config=task, error=e, extra_info=f"task_id:[{task.id}] _truncate_jdy_form failed.")
            raise e  # 抛出异常, 让调用者处理状态

    # --- 首次全量同步的内部方法 ---
    @retry()
    def _insert_jdy_data_with_no_primary_key(self, config_session: Session, task: SyncTask):
        """
        内部全量同步逻辑, 支持SQL过滤和选择性删除
        """
        # --- 检查任务类型 ---
        if task.sync_type != 'db2jdy':
            logger.error(f"task_id:[{task.id}] _run_full_sync failed: Task type is not 'db2jdy'.")
            return

        logger.info(f"task_id:[{task.id}] Running _insert_jdy_data_with_no_primary_key...")

        total_created = 0
        total_source_rows = 0

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            raise ValueError(
                f"task_id:[{task.id}] Task {task.id} missing department or API key for _insert_jdy_data_with_no_primary_key.")
        api_key = task.department.jdy_key_info.api_key

        try:
            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.id)
            if not payload_map:
                raise ValueError(f"task_id:[{task.id}] Field mapping is empty.")

            # 1. 实例化
            data_api_create = DataApi(api_key, Config.JDY_API_BASE_URL, qps=10)

            # 3. 构建带 SQL 过滤的查询, 并使用流式处理
            with get_dynamic_session(task) as source_session:
                base_query = f"SELECT * FROM `{task.table_name}`"
                params = {}
                if task.source_filter_sql:
                    base_query += f" WHERE {task.source_filter_sql}"

                # --- 性能优化: 使用流式查询 ---
                # 移除: rows = source_session.execute(text(base_query), params).mappings().all()
                # sqlalchemy < 2 版本
                # result_stream = source_session.execution_options(stream_results=True).execute(text(base_query), params)
                # sqlalchemy >= 2 版本
                result_stream = source_session.connection().execution_options(stream_results=True).execute(
                    text(base_query), params)

                has_processed_rows = False

                # 4. 批量创建数据
                batch_data = []
                for row in result_stream.mappings():
                    has_processed_rows = True
                    total_source_rows += 1

                    row_dict = dict(row)
                    data_payload = self._transform_row_to_jdy(row_dict, payload_map)
                    if data_payload:
                        batch_data.append(data_payload)

                    if len(batch_data) >= 100:  # API 限制 100
                        trans_id = str(uuid.uuid4())
                        responses = data_api_create.create_batch_data(
                            task.app_id, task.entry_id,
                            data_list=batch_data, transaction_id=trans_id
                        )
                        # create_batch_data 返回一个列表，需要对列表中的每个响应求和
                        success_count = sum(resp.get('success_count', 0) for resp in responses)
                        logger.info(f"task_id:[{task.id}] Created {success_count} items.")

                        total_created += success_count
                        if success_count != len(batch_data):
                            log_sync_error(task_config=task,
                                           extra_info=f"task_id:[{task.id}] Create mismatch. Req: {len(batch_data)}, Created: {success_count}. Trans_id: {trans_id}")
                        batch_data = []

                # 处理最后一个批次
                if batch_data:
                    trans_id = str(uuid.uuid4())
                    responses = data_api_create.create_batch_data(
                        task.app_id, task.entry_id,
                        data_list=batch_data, transaction_id=trans_id
                    )
                    # create_batch_data 返回一个列表，需要对列表中的每个响应求和
                    success_count = sum(resp.get('success_count', 0) for resp in responses)
                    logger.info(f"task_id:[{task.id}] Created {success_count} items.")

                    total_created += success_count
                    if success_count != len(batch_data):
                        log_sync_error(task_config=task,
                                       extra_info=f"task_id:[{task.id}] Create mismatch. Req: {len(batch_data)}, Created: {success_count}. TransID: {trans_id}")

                # 检查是否因为没有数据而退出循环
                if not has_processed_rows:
                    if task.source_filter_sql:
                        logger.info(f"task_id:[{task.id}] No data found WHERE {task.source_filter_sql}.")
                    else:
                        logger.info(f"task_id:[{task.id}] No data found.")

            logger.info(
                f"task_id:[{task.id}] _insert_jdy_data_with_no_primary_key completed. Source rows: {total_source_rows}, Created in Jdy: {total_created}.")
            if total_source_rows != total_created:
                log_sync_error(task_config=task,
                               extra_info=f"task_id:[{task.id}] FINAL COUNT MISMATCH. Source: {total_source_rows}, Created: {total_created}.")

            # 更新时间, 但不更新状态 (由调用者更新)
            self._update_task_status(config_session, task,
                                     status=task.sync_status,  # 保持状态不变
                                     last_sync_time=datetime.now(TZ_UTC_8))

        except Exception as e:
            # 不更新状态, 只记录日志, 抛出异常
            log_sync_error(task_config=task, error=e,
                           extra_info=f"task_id:[{task.id}] _insert_jdy_data_with_no_primary_key failed.")
            raise e  # 抛出异常, 让调用者处理状态

    @retry()
    def _insert_jdy_data_with_primary_key(self, config_session: Session, task: SyncTask):
        """
        执行增量同步 (Upsert)
        (接受会话, 事务 ID, 去重, 复合主键，支持首次全量同步 和 source_filter_sql)
        """
        # --- 检查任务类型 ---
        if task.sync_type != 'db2jdy':
            logger.error(f"task_id:[{task.id}] _insert_jdy_data_with_primary_key failed: Task type is not 'db2jdy'.")
            self._update_task_status(config_session, task, status='error')
            return

        logger.info(f"task_id:[{task.id}] Running _insert_jdy_data_with_primary_key sync...")
        self._update_task_status(config_session, task, status='running')

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            log_sync_error(
                task_config=task,
                error=ValueError(f"Task {task.id} missing department or API key for INCREMENTAL."),
                extra_info=f"task_id:[{task.id}] INCREMENTAL failed."
            )
            self._update_task_status(config_session, task, status='error')
            return
        api_key = task.department.jdy_key_info.api_key

        current_sync_time = datetime.now(TZ_UTC_8)

        try:
            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.id)
            alias_map = mapping_service.get_alias_mapping(config_session, task.id)
            if not payload_map or not alias_map:
                raise ValueError(f"task_id:[{task.id}] Field mapping is empty.")

            # 3. 实例化
            data_api_query = DataApi(api_key, Config.JDY_API_BASE_URL, qps=30)
            data_api_delete = DataApi(api_key, Config.JDY_API_BASE_URL, qps=10)  # 用于去重
            data_api_create = DataApi(api_key, Config.JDY_API_BASE_URL, qps=20)  # Single create
            data_api_update = DataApi(api_key, Config.JDY_API_BASE_URL, qps=20)  # Single update

            with get_dynamic_session(task) as source_session:

                base_query = f"SELECT * FROM `{task.table_name}`"
                params = {}
                if task.source_filter_sql:
                    base_query += f" WHERE {task.source_filter_sql}"

                # --- 性能优化: 使用流式查询 ---
                # 移除: rows = source_session.execute(text(base_query), params).mappings().all()
                # sqlalchemy < 2 版本
                # result_stream = source_session.execution_options(stream_results=True).execute(text(base_query), params)
                # sqlalchemy >= 2 版本
                result_stream = source_session.connection().execution_options(stream_results=True).execute(
                    text(base_query), params)

                # 标记是否处理了任何行
                has_processed_rows = False
                count_new, count_updated = 0, 0

                # 6. 遍历新增/更新
                # for row in rows:
                # 直接遍历迭代器，这会从数据库中逐行（或按小批量）获取数据
                for row in result_stream.mappings():
                    has_processed_rows = True  # 标记已处理
                    row_dict = dict(row)
                    try:
                        self._get_pk_fields_and_values(task, row_dict)
                    except ValueError as e:
                        log_sync_error(task_config=task, error=e,
                                       extra_info=f"task_id:[{task.id}] Row missing PK. Skipping.",
                                       payload=row_dict)
                        continue

                    data_payload = self._transform_row_to_jdy(row_dict, payload_map)
                    if not data_payload:
                        log_sync_error(task_config=task,
                                       payload=row_dict,
                                       extra_info=f"task_id:[{task.id}] Row missing required fields. Skipping.")
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
                            task.app_id, task.entry_id, jdy_id,
                            data_payload, transaction_id=trans_id
                        )
                        update_jdy_id = update_response.get('data', {}).get('_id')

                        if not update_jdy_id:
                            log_sync_error(task_config=task,
                                           payload=data_payload,
                                           error=update_response,
                                           extra_info=f"task_id:[{task.id}] Failed to update data.")
                        else:
                            count_updated += 1
                            logger.debug(f"task_id:[{task.id}] Updated data with _id: {jdy_id}.")

                    else:
                        # 新增
                        create_response = data_api_create.create_single_data(
                            task.app_id, task.entry_id,
                            data_payload, transaction_id=trans_id
                        )
                        new_jdy_id = create_response.get('data', {}).get('_id')
                        if not new_jdy_id:
                            log_sync_error(task_config=task,
                                           payload=data_payload,
                                           error=create_response,
                                           extra_info=f"task_id:[{task.id}] Failed to create data.")
                        else:
                            count_new += 1
                            logger.debug(f"task_id:[{task.id}] Created data with _id: {new_jdy_id}.")
                            # 是否需要回写，有待商榷，实际可不用回写
                            # # 传入 row_dict 以进行复合主键回写
                            # self._writeback_id_to_source(source_session, task, new_jdy_id, row_dict)

                # 检查是否因为没有数据而退出循环
                if not has_processed_rows:
                    logger.info(f"task_id:[{task.id}] No new data where {task.source_filter_sql}.")
                    self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)
                    return

            logger.info(f"task_id:[{task.id}] INCREMENTAL sync completed. New: {count_new}, Updated: {count_updated}.")
            self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)

        except Exception as e:
            config_session.rollback()
            log_sync_error(task_config=task, error=e, extra_info=f"task_id:[{task.id}] INCREMENTAL failed.")
            self._update_task_status(config_session, task, status='error')

    @retry()
    def run_full_sync(self, config_session: Session, task: SyncTask):
        """
        执行全量同步
        (视图检查, 事务 ID, 修复响应逻辑)
        """
        # --- 检查任务类型 ---
        if task.sync_type != 'db2jdy':
            logger.error(f"task_id:[{task.id}] run_full_replace failed: Task type is not 'db2jdy'.")
            self._update_task_status(config_session, task, status='error')
            return

        if not task.is_active:
            logger.info(f"task_id:[{task.id}] run_full_replace is disabled: Task is not active.")
            return

        logger.info(f"task_id:[{task.id}] Running FULL_SYNC sync (Scheduled)...")

        try:
            # 如果首次清空数据
            # 双重确认
            if not task.last_sync_time:
                if task.is_delete_first:
                    # 执行清空操作
                    self._update_task_status(config_session, task, status='running')
                    self._truncate_jdy_data(config_session, task)
                    # 成功, 设置为空闲
                    self._update_task_status(config_session, task, status='idle', last_sync_time=datetime.now(TZ_UTC_8),
                                             is_full_sync_first=True, is_delete_first=False)

            # 执行全量同步
            self._update_task_status(config_session, task, status='running')
            # 如果没有主键
            if not task.business_keys:
                logger.info(
                    f"task_id:[{task.id}] No primary key found. Running _insert_jdy_data_with_no_primary_key sync...")
                self._insert_jdy_data_with_no_primary_key(config_session, task)
            # 如果有主键
            else:
                logger.info(
                    f"task_id:[{task.id}] Primary key found. Running _insert_jdy_data_with_primary_key sync...")
                self._insert_jdy_data_with_primary_key(config_session, task)

            self._update_task_status(config_session, task, status='idle', last_sync_time=datetime.now(TZ_UTC_8))

        except Exception as e:
            config_session.rollback()
            logger.error(f"task_id:[{task.id}] FULL_SYNC failed.")
            log_sync_error(task_config=task, error=e, extra_info="task_id:[{task.id}] FULL_SYNC failed.")
            self._update_task_status(config_session, task, status='error')

    @retry()
    def run_incremental(self, config_session: Session, task: SyncTask):
        """
        执行增量同步 (Upsert)
        (接受会话, 事务 ID, 去重, 复合主键，支持首次全量同步 和 source_filter_sql)
        """
        # --- 检查任务类型 ---
        if task.sync_type != 'db2jdy':
            logger.error(f"task_id:[{task.id}] run_incremental failed: Task type is not 'db2jdy'.")
            self._update_task_status(config_session, task, status='error')
            return

        logger.info(f"task_id:[{task.id}] Running INCREMENTAL sync...")
        self._update_task_status(config_session, task, status='running')

        if not task.department or not task.department.jdy_key_info or not task.department.jdy_key_info.api_key:
            log_sync_error(
                task_config=task,
                error=ValueError(f"Task {task.id} missing department or API key for INCREMENTAL."),
                extra_info=f"task_id:[{task.id}] INCREMENTAL failed."
            )
            self._update_task_status(config_session, task, status='error')
            return
        api_key = task.department.jdy_key_info.api_key

        if not task.incremental_field:
            raise ValueError(f"task_id:[{task.id}] Incremental field (e.g., last_modified) is not configured.")

        if not task.business_keys:
            raise ValueError(f"task_id:[{task.id}] No primary key found. Please configure primary keys.")

        try:
            # 1. 检查是否需要首次全量同步
            if not task.last_sync_time:
                if task.is_full_sync_first:
                    logger.info(f"task_id:[{task.id}] First run: Executing initial FULL SYNC...")
                    try:
                        # 双重确认
                        if task.is_delete_first:
                            # 执行清空操作
                            self._update_task_status(config_session, task, status='running')
                            self._truncate_jdy_data(config_session, task)
                            # 成功后, 更新状态并退出
                            self._update_task_status(config_session, task,
                                                     status='idle',
                                                     last_sync_time=datetime.now(TZ_UTC_8),
                                                     is_delete_first=False)
                        # 调用全量同步
                        self._update_task_status(config_session, task, status='running')
                        self._insert_jdy_data_with_primary_key(config_session, task)
                        # 成功后, 更新状态并退出
                        self._update_task_status(config_session, task,
                                                 status='idle',
                                                 last_sync_time=datetime.now(TZ_UTC_8),
                                                 is_full_sync_first=False,
                                                 is_delete_first=False)

                        logger.info(f"task_id:[{task.id}] Initial full sync complete.")
                        return  # 本次运行结束
                    except Exception as e:
                        # 首次全量同步失败, 保持 is_full_sync_first=True, 设为 error
                        config_session.rollback()
                        self._update_task_status(config_session, task, status='error')
                        return  # 退出

            # 2. 正常增量逻辑
            current_sync_time = datetime.now(TZ_UTC_8)

            mapping_service = FieldMappingService()
            payload_map = mapping_service.get_payload_mapping(config_session, task.id)
            alias_map = mapping_service.get_alias_mapping(config_session, task.id)
            if not payload_map or not alias_map:
                raise ValueError(f"task_id:[{task.id}] Field mapping is empty.")

            # 3. 实例化
            data_api_query = DataApi(api_key, Config.JDY_API_BASE_URL, qps=30)
            data_api_delete = DataApi(api_key, Config.JDY_API_BASE_URL, qps=10)  # 用于去重
            data_api_create = DataApi(api_key, Config.JDY_API_BASE_URL, qps=20)  # Single create
            data_api_update = DataApi(api_key, Config.JDY_API_BASE_URL, qps=20)  # Single update

            # 4. 确定时间戳
            last_sync_time = task.last_sync_time or datetime(1970, 1, 1, tzinfo=TZ_UTC_8)

            # 动态创建 source_session 和 engine
            dynamic_engine = get_dynamic_engine(task)
            with get_dynamic_session(task) as source_session:

                # --- 数据探测逻辑 ---

                # 解析 incremental 字段（有可能是复杂字段）
                raw_field = task.incremental_field.strip() if task.incremental_field else None
                if not raw_field:
                    raise ValueError(f"task_id:[{task.id}] No incremental field specified.")

                raw_field = ','.join([item.strip() for item in raw_field.split(',') if item.strip()])

                field_for_probing = raw_field  # 用第一个字段用于数据类型探测
                incremental_field_for_query = raw_field
                is_complex_field = False

                # 1. 检查字段格式
                if ',' in raw_field and '(' not in raw_field and ')' not in raw_field:
                    # 格式: updated_time,created_time
                    # 转换为 coalesce
                    incremental_field_for_query = f"COALESCE({raw_field})"
                    # 探测字段: updated_time
                    field_for_probing = raw_field.split(',')[0].strip().replace('`', '')
                    is_complex_field = True
                    logger.debug(
                        f"task_id:[{task.id}] Detected comma-separated fields. Query: {incremental_field_for_query}, ProbeField: {field_for_probing}")

                elif 'coalesce(' in raw_field.lower() or 'ifnull(' in raw_field.lower():
                    # 格式: coalesce(updated_time,created_time) 或 IFNULL(...) 或 (其他复杂表达式)
                    incremental_field_for_query = f"({raw_field})"
                    is_complex_field = True

                    # 提取第一个字段用于探测
                    # 查找第一个单词 (可能是函数名) 和随后的字段名
                    # r'[a-zA-Z0-9_]+' 匹配字段名
                    fields = re.findall(r'[a-zA-Z0-9_]+', raw_field.replace('`', ''))

                    if fields:
                        first_word = fields[0].lower()
                        if first_word in ['coalesce', 'ifnull'] and len(fields) > 1:
                            field_for_probing = fields[1]  # e.g., coalesce(THIS_ONE, ...)
                        else:
                            field_for_probing = fields[0]  # e.g., THIS_ONE ...
                    else:
                        field_for_probing = raw_field  # 回退

                    logger.debug(
                        f"task_id:[{task.id}] Detected complex function. Query: {incremental_field_for_query}, ProbeField: {field_for_probing}")

                else:
                    # 简单字段: updated_time
                    incremental_field_for_query = f"`{raw_field}`"
                    field_for_probing = raw_field.replace('`', '')
                    is_complex_field = False
                    logger.debug(
                        f"task_id:[{task.id}] Detected simple field. Query: {incremental_field_for_query}, ProbeField: {field_for_probing}")

                # 2. 检查探测字段 (field_for_probing) 的类型
                inspector = inspect(dynamic_engine)
                col_info = None
                try:
                    columns = inspector.get_columns(task.table_name)
                    # 使用 field_for_probing 查找列信息
                    col_info = next((col for col in columns if col['name'] == field_for_probing), None)
                except NoSuchTableError:
                    raise ValueError(f"task_id:[{task.id}] Incremental field's table '{task.table_name}' not found.")

                # 如果找不到字段
                if not col_info:
                    log_sync_error(task_config=task,
                                   extra_info=f"task_id:[{task.id}] Incremental field (ProbeField) '{field_for_probing}' not found in table '{task.table_name}'.")
                    raise ValueError(
                        f"task_id:[{task.id}] Incremental field (ProbeField) '{field_for_probing}' not found in table '{task.table_name}'.")

                col_type_name = str(col_info['type']).upper()

                last_sync_time_for_query = None

                # 3. 如果是 DATE 类型，总是截断
                if col_type_name == 'DATE':
                    last_sync_time_for_query = last_sync_time.replace(hour=0, minute=0, second=0, microsecond=0)
                    logger.debug(
                        f"task_id:[{task.id}] Detected DATE type. Querying >= {last_sync_time_for_query} (Truncated)")

                # 4. 如果是 DATETIME，执行数据探测
                elif col_type_name.startswith('DATETIME'):
                    logger.debug(f"task_id:[{task.id}] Detected DATETIME type. Probing data ...")
                    is_fake_datetime = False

                    # 探测查询，限制100条
                    probe_query = text(
                        f"SELECT `{field_for_probing}` FROM `{task.table_name}` "
                        f"WHERE `{field_for_probing}` IS NOT NULL LIMIT 100"
                    )
                    probe_results = source_session.execute(probe_query).fetchall()

                    # 没有数据，无法判断。为安全起见，使用截断（防止丢失数据）
                    if not probe_results:
                        logger.debug(f"task_id:[{task.id}] No data found for probing.")
                        is_fake_datetime = True
                    else:
                        min_time = time_obj(0, 0, 0)
                        all_are_midnight = True
                        for row in probe_results:
                            dt_val = row[0]
                            if dt_val is not None and dt_val.time() != min_time:
                                # logger.debug(f"task_id:[{task.id}] Detected DATETIME type is yyyy-MM-dd HH:mm:ss.")
                                all_are_midnight = False
                                break
                        is_fake_datetime = all_are_midnight

                    if is_fake_datetime:
                        logger.debug(
                            f"task_id:[{task.id}] Probe confirms yyyy-MM-dd 00:00:00 DATETIME format. Using truncated timestamp.")
                        last_sync_time_for_query = last_sync_time.replace(hour=0, minute=0, second=0, microsecond=0)
                    else:
                        logger.debug(
                            f"task_id:[{task.id}] Probe found yyyy-MM-dd HH:mm:ss DATETIME format. Using exact timestamp.")
                        last_sync_time_for_query = last_sync_time
                else:
                    # 3. 如果是 TIMESTAMP 或其他类型，使用精确时间
                    last_sync_time_for_query = last_sync_time
                    logger.debug(
                        f"task_id:[{task.id}] Detected {col_type_name} type. Querying >= {last_sync_time_for_query}")

                # 6. 获取源数据 (带 SQL 过滤)
                base_query = (
                    f"SELECT * FROM `{task.table_name}` "
                    f"WHERE {incremental_field_for_query} >= :last_sync_time"
                )
                # 使用动态确定的时间戳
                params = {"last_sync_time": last_sync_time_for_query}
                if task.source_filter_sql:
                    base_query += f" AND ({task.source_filter_sql})"

                # --- 2. 性能优化: 流式查询 ---
                # 移除: rows = source_session.execute(text(base_query), params).mappings().all()

                # if not rows:
                #     logger.info(f"task_id:[{task.id}] No new data found since {last_sync_time_for_query}.")
                #     self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)
                #     return

                # 1. 不要使用 .all()，而是获取结果迭代器
                # 2. 使用 stream_results=True 启用服务器端游标，防止数据库连接因长时间处理而超时
                # sqlalchemy < 2 版本
                # result_stream = source_session.execution_options(stream_results=True).execute(text(base_query), params)
                # sqlalchemy >= 2 版本
                result_stream = source_session.connection().execution_options(stream_results=True).execute(
                    text(base_query), params)

                # 标记是否处理了任何行
                has_processed_rows = False
                count_new, count_updated = 0, 0

                # 6. 遍历新增/更新
                # for row in rows:
                # 直接遍历迭代器，这会从数据库中逐行（或按小批量）获取数据
                for row in result_stream.mappings():
                    has_processed_rows = True  # 标记已处理
                    row_dict = dict(row)
                    try:
                        self._get_pk_fields_and_values(task, row_dict)
                    except ValueError as e:
                        log_sync_error(task_config=task, error=e,
                                       extra_info=f"task_id:[{task.id}] Row missing PK. Skipping.",
                                       payload=row_dict)
                        continue

                    data_payload = self._transform_row_to_jdy(row_dict, payload_map)
                    if not data_payload:
                        log_sync_error(task_config=task,
                                       payload=row_dict,
                                       extra_info=f"task_id:[{task.id}] Row missing required fields. Skipping.")
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
                            task.app_id, task.entry_id, jdy_id,
                            data_payload, transaction_id=trans_id
                        )
                        update_jdy_id = update_response.get('data', {}).get('_id')

                        if not update_jdy_id:
                            log_sync_error(task_config=task,
                                           payload=data_payload,
                                           error=update_response,
                                           extra_info=f"task_id:[{task.id}] Failed to update data.")
                        else:
                            count_updated += 1
                            logger.debug(f"task_id:[{task.id}] Updated data with _id: {jdy_id}.")

                    else:
                        # 新增
                        create_response = data_api_create.create_single_data(
                            task.app_id, task.entry_id,
                            data_payload, transaction_id=trans_id
                        )
                        new_jdy_id = create_response.get('data', {}).get('_id')
                        if not new_jdy_id:
                            log_sync_error(task_config=task,
                                           payload=data_payload,
                                           error=create_response,
                                           extra_info=f"task_id:[{task.id}] Failed to create data.")
                        else:
                            count_new += 1
                            logger.debug(f"task_id:[{task.id}] Created data with _id: {new_jdy_id}.")
                            # 是否需要回写，有待商榷，实际可不用回写
                            # # 传入 row_dict 以进行复合主键回写
                            # self._writeback_id_to_source(source_session, task, new_jdy_id, row_dict)

                # 检查是否因为没有数据而退出循环
                if not has_processed_rows:
                    logger.info(f"task_id:[{task.id}] No new data found since {last_sync_time_for_query}.")
                    self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)
                    return

            logger.info(f"task_id:[{task.id}] INCREMENTAL sync completed. New: {count_new}, Updated: {count_updated}.")
            self._update_task_status(config_session, task, status='idle', last_sync_time=current_sync_time)

        except Exception as e:
            config_session.rollback()
            log_sync_error(task_config=task, error=e, extra_info=f"task_id:[{task.id}] INCREMENTAL failed.")
            self._update_task_status(config_session, task, status='error')

    @retry()
    def run_binlog_listener(self, task: SyncTask):
        """
        运行一个长连接的 Binlog 监听器
        (独立创建会话, 事务 ID, 去重, 复合主键，支持首次全量同步)
        """

        # 1. 尽快安全地获取 task_id
        # 'task' 对象是从另一个线程传入的, 处于游离状态。
        # 访问除 .id 之外的任何属性都可能导致 DetachedInstanceError。
        try:
            task_id_safe = task.id
        except Exception as e:
            # 如果连 task.id 都无法访问, 记录日志并退出
            logger.error(f"[BinlogListener-??] CRITICAL: Failed to get task.id from initial task object: {e}")
            return

        thread_name = f"BinlogListener-{task_id_safe}"
        current_thread().name = thread_name
        logger.info(f"[{thread_name}] Starting...")

        # 检查任务类型
        if task.sync_type != 'db2jdy':
            logger.error(f"task_id:[{task_id_safe}] run_binlog_listener 失败：任务类型不是 'db2jdy'。")
            return

        # 2. 定义所有需要的变量, 它们将在下面的 'with' 块中被填充
        api_key = None
        payload_map = None
        alias_map = None
        dynamic_binlog_settings = {}
        server_id = 100 + task_id_safe
        table_name = None
        db_name = None
        log_file = None
        log_pos = None
        last_sync_time = None
        app_id = None
        entry_id = None

        stream = None

        try:
            # 3. 创建一个 *单一的* 会话来获取所有需要的配置
            # 这样可以确保所有 SQLAlchemy 对象在关闭会话之前
            # 其所有属性都被访问并存储在局部变量中
            with ConfigSession() as config_session:

                # 3a. 从新会话中获取 'live' 的 task 实例
                session_task = config_session.query(SyncTask).options(
                    joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                    joinedload(SyncTask.database)
                ).get(task_id_safe)

                if not session_task:
                    # 使用原始的 'task' 对象进行最后一次日志记录
                    log_sync_error(task_config=task, extra_info=f"[{thread_name}] Task not found in DB. Stopping.")
                    return  # 致命错误

                if self._is_view(session_task):
                    log_sync_error(task_config=session_task,
                                   extra_info=f"task_id:[{task_id_safe}] BINLOG mode is not allowed for VIEWS. Stopping listener.")
                    return

                # 3b. 提取 API 密钥
                if not session_task.department or not session_task.department.jdy_key_info or not session_task.department.jdy_key_info.api_key:
                    log_sync_error(
                        task_config=session_task,
                        error=ValueError(f"Task {task_id_safe} missing department or API key for BINLOG."),
                        extra_info=f"task_id:[{task_id_safe}] BINLOG listener stopped."
                    )
                    return
                api_key = session_task.department.jdy_key_info.api_key  # 存储为局部变量

                # 3c. 提取数据库信息
                if not session_task.database:
                    log_sync_error(task_config=session_task,
                                   error=ValueError(f"Task {task_id_safe} missing database link."),
                                   extra_info=f"task_id:[{task_id_safe}] BINLOG listener stopped.")
                    return

                # 3d. 提取 Binlog 动态设置 (标量)
                dynamic_binlog_settings = {
                    "host": session_task.database.db_host,
                    "port": session_task.database.db_port,
                    "user": session_task.database.db_user,
                    "passwd": session_task.database.db_password
                }
                table_name = session_task.table_name  # 存储为局部变量
                db_name = session_task.database.db_name  # 存储为局部变量

                # 3e. 检查是否需要首次全量同步
                if not session_task.last_sync_time:
                    if session_task.is_full_sync_first:
                        logger.info(f"task_id:[{task.id}] First run: Executing initial FULL SYNC...")
                        try:
                            # 双重确认
                            if session_task.is_delete_first:
                                # 执行清空操作
                                self._update_task_status(config_session, session_task, status='running')
                                self._truncate_jdy_data(config_session, session_task)
                                # 成功后, 更新状态并退出
                                self._update_task_status(config_session, session_task,
                                                         status='idle',
                                                         last_sync_time=datetime.now(TZ_UTC_8),
                                                         is_delete_first=False)
                            # 调用全量同步
                            self._update_task_status(config_session, task, status='running')
                            self._insert_jdy_data_with_primary_key(config_session, session_task)
                            # 成功后, 更新状态并退出
                            self._update_task_status(config_session, task,
                                                     status='idle',
                                                     last_sync_time=datetime.now(TZ_UTC_8),
                                                     is_full_sync_first=False,
                                                     is_delete_first=False)

                            logger.info(f"[{thread_name}] Initial full sync complete. Proceeding to binlog...")
                        except Exception as e:
                            log_sync_error(task_config=session_task, error=e,
                                           extra_info=f"[{thread_name}] Initial full sync failed. Stopping binlog listener.")
                            # 确保错误状态被提交
                            self._update_task_status(config_session, session_task, status='error')
                            return  # 退出线程
                else:
                    # 仅在非首次运行时更新状态
                    self._update_task_status(config_session, session_task, status='running')

                # 3f. 提取剩余的标量值
                log_file = session_task.last_binlog_file
                log_pos = session_task.last_binlog_pos
                last_sync_time = session_task.last_sync_time
                app_id = session_task.app_id
                entry_id = session_task.entry_id

                # --- 检查并获取初始 Binlog 位置 ---
                if not log_file or not log_pos:
                    logger.info(f"[{thread_name}] Binlog position not found. Fetching current master status...")
                    try:
                        # 必须使用源数据库引擎
                        dynamic_engine = get_dynamic_engine(session_task)
                        with dynamic_engine.connect() as connection:
                            result = connection.execute(text("SHOW MASTER STATUS")).fetchone()
                            # 确保 result 不是 None 并且至少有2个元素 (File, Position)
                            if result and len(result) >= 2:
                                file, pos = result[0], result[1]
                                logger.info(f"[{thread_name}] Fetched master status: {file}:{pos}")
                                log_file = file
                                log_pos = pos
                                # 立即保存此位置 (使用 config_session)
                                self._update_task_status(config_session, session_task, 'running',
                                                         binlog_file=log_file, binlog_pos=log_pos,
                                                         last_sync_time=datetime.now(TZ_UTC_8))
                            else:
                                log_sync_error(task_config=session_task,
                                               extra_info=f"[{thread_name}] Failed to get master status (no result). Stopping listener.")
                                self._update_task_status(config_session, session_task, status='error',
                                                         last_sync_time=datetime.now(TZ_UTC_8))
                                return  # 退出
                    except Exception as e:
                        log_sync_error(task_config=session_task, error=e,
                                       extra_info=f"[{thread_name}] Failed to get master status (exception). Stopping listener.")
                        self._update_task_status(config_session, session_task, status='error',
                                                 last_sync_time=datetime.now(TZ_UTC_8))
                        return  # 退出

                # 3g. 提取映射
                mapping_service = FieldMappingService()
                payload_map = mapping_service.get_payload_mapping(config_session, task_id_safe)
                alias_map = mapping_service.get_alias_mapping(config_session, task_id_safe)

            # --- config_session 在此结束 ---
            # 'session_task' 实例现在已分离, 但我们已将所有
            # 需要的值存储在局部变量中 (api_key, payload_map, table_name 等)

            # 4. 检查映射
            if not payload_map or not alias_map:
                raise ValueError(f"[{thread_name}] Field mapping is empty. Stopping.")

            # 5. 实例化 API (在会话外)
            data_api_query = DataApi(api_key, Config.JDY_API_BASE_URL, qps=30)
            data_api_delete = DataApi(api_key, Config.JDY_API_BASE_URL, qps=10)
            data_api_create = DataApi(api_key, Config.JDY_API_BASE_URL, qps=20)
            data_api_update = DataApi(api_key, Config.JDY_API_BASE_URL, qps=20)

            # 6. 实例化 Binlog 读取器 (在会话外, 使用局部变量)
            stream = BinLogStreamReader(
                connection_settings=dynamic_binlog_settings,
                server_id=server_id,
                # 确保我们监听心跳事件
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, HeartbeatLogEvent],
                only_tables=[table_name],
                only_schemas=[db_name],
                log_file=log_file,
                log_pos=log_pos,
                resume_stream=True,
                blocking=True,
                slave_heartbeat=5,  # 请求每 5 秒一次心跳
                skip_to_timestamp=last_sync_time.timestamp() if last_sync_time else None
            )

            logger.info(f"[{thread_name}] Listening for binlog events...")

            # 7. 开始循环 (在循环内部使用短暂的会话)
            for binlog_event in stream:

                # 7a. 每次循环都检查任务是否被禁用 (会因心跳而每 5 秒运行一次)
                try:
                    with ConfigSession() as check_session:
                        current_task_state = check_session.query(SyncTask).get(task_id_safe)
                        if not current_task_state or not current_task_state.is_active:
                            logger.info(f"[{thread_name}] Task disabled. Stopping listener.")
                            break  # 退出 'for binlog_event in stream:' 循环
                except Exception as check_e:
                    # 如果无法检查数据库，这是一个严重问题，最好停止监听器
                    log_sync_error(task_config=task, error=check_e,
                                   extra_info=f"[{thread_name}] Failed to check task status. Stopping listener.")
                    break  # 退出循环

                # 7b. 如果 'timeout_seconds' (slave_heartbeat) 到了, event 可能是 None
                if binlog_event is None:
                    continue  # 没有事件，继续等待

                # 7c. 立即从流中获取当前位置。
                current_log_file = stream.log_file
                current_log_pos = stream.log_pos

                # 7d. 如果是心跳事件, 我们只更新位置 (保存我们的位置) 然后继续
                if isinstance(binlog_event, HeartbeatLogEvent):
                    logger.debug(f"[{thread_name}] Received heartbeat. Updating position.")
                    try:
                        with ConfigSession() as heartbeat_session:
                            # 获取 'live' task 对象以更新
                            task_to_update = heartbeat_session.query(SyncTask).get(task_id_safe)
                            if task_to_update:
                                self._update_task_status(heartbeat_session, task_to_update, 'running',
                                                         current_log_file, current_log_pos)
                    except Exception as e:
                        # 记录更新位置时的错误, 但不要停止监听器
                        log_sync_error(task_config=task, error=e,
                                       extra_info=f"[{thread_name}] Failed to update position on heartbeat.")
                    continue  # 继续等待下一个事件

                # 7e. 这是一个真实的事件 (Write/Update/Delete), 处理它
                try:
                    # 在循环内部为 *每个事件* 创建短暂的会话
                    # 'task' 在这里是原始的游离对象, 我们只使用它来打开 get_dynamic_session
                    with ConfigSession() as loop_session, get_dynamic_session(task) as source_session:

                        task_in_loop = loop_session.query(SyncTask).get(task_id_safe)
                        if not task_in_loop:  # 如果任务在两次检查之间被删除了
                            logger.warning(f"[{thread_name}] Task was deleted during event processing. Stopping.")
                            break  # 退出循环

                        for row in binlog_event.rows:
                            trans_id = str(uuid.uuid4())  # 每个 row 操作都是一个事务

                            if isinstance(binlog_event, WriteRowsEvent):
                                # 1. 查找 ID, 使用 row['values']
                                jdy_id = row['values'].get('_id') or self._find_jdy_id_by_pk(
                                    task_in_loop, row['values'],
                                    data_api_query, data_api_delete, alias_map
                                )

                                # 2. 准备 payload, 使用 row['values']
                                data_payload = self._transform_row_to_jdy(row['values'], payload_map)

                                if not data_payload:
                                    log_sync_error(task_config=task_in_loop,
                                                   payload=row['values'],
                                                   extra_info=f"task_id:[{task_id_safe}] Row missing required fields. Skipping.")
                                    continue

                                if jdy_id:
                                    # 3. 如果找到了 (罕见, 但为了数据一致性), 更新
                                    logger.debug(
                                        f"task_id:[{task_id_safe}] WriteEvent: ID {jdy_id} found. Updating...")
                                    update_response = data_api_update.update_single_data(
                                        app_id, entry_id, jdy_id,  # 使用局部变量
                                        data_payload, transaction_id=trans_id
                                    )
                                    update_jdy_id = update_response.get('data', {}).get('_id')
                                    if not update_jdy_id:
                                        log_sync_error(task_config=task_in_loop,
                                                       payload=data_payload,
                                                       error=update_response,
                                                       extra_info=f"task_id:[{task_id_safe}] WriteEvent: Failed to update data (ID found).")
                                    else:
                                        logger.debug(
                                            f"task_id:[{task_id_safe}] WriteEvent: Updated data with _id: {update_jdy_id} (ID found).")

                                else:
                                    # 4. 如果没找到 (正常情况), 创建
                                    logger.debug(f"task_id:[{task_id_safe}] WriteEvent: ID not found. Creating...")
                                    create_response = data_api_create.create_single_data(
                                        app_id, entry_id,  # 使用局部变量
                                        data_payload, transaction_id=trans_id
                                    )
                                    new_jdy_id = create_response.get('data', {}).get('_id')
                                    if not new_jdy_id:
                                        log_sync_error(task_config=task_in_loop,
                                                       payload=data_payload,
                                                       error=create_response,
                                                       extra_info=f"[{thread_name}] Failed to create data.")
                                    else:
                                        logger.debug(
                                            f"task_id:[{task_id_safe}] Created data with _id: {new_jdy_id}")


                            elif isinstance(binlog_event, UpdateRowsEvent):
                                jdy_id = row['after_values'].get('_id') or self._find_jdy_id_by_pk(
                                    task_in_loop, row['after_values'],  # 使用 task_in_loop
                                    data_api_query, data_api_delete, alias_map
                                )

                                if jdy_id:
                                    data_payload = self._transform_row_to_jdy(row['after_values'], payload_map)

                                    if not data_payload:
                                        log_sync_error(task_config=task_in_loop,
                                                       payload=row['after_values'],
                                                       extra_info=f"task_id:[{task_id_safe}] Row missing required fields. Skipping.")
                                        continue

                                    logger.debug(
                                        f"task_id:[{task_id_safe}] UpdateEvent: ID {jdy_id} found. Updating...")
                                    update_response = data_api_update.update_single_data(
                                        app_id, entry_id, jdy_id,  # 使用局部变量
                                        data_payload, transaction_id=trans_id
                                    )
                                    update_jdy_id = update_response.get('data', {}).get('_id')
                                    if not update_jdy_id:
                                        log_sync_error(task_config=task_in_loop,
                                                       payload=data_payload,
                                                       error=update_response,
                                                       extra_info=f"task_id:[{task_id_safe}] Failed to update data.")
                                    else:
                                        logger.debug(f"task_id:[{task_id_safe}] Updated data with _id: {update_jdy_id}")

                                else:
                                    # 简道云中没有，则新增
                                    logger.debug(f"task_id:[{task_id_safe}] UpdateEvent: ID not found. Creating...")
                                    data_payload = self._transform_row_to_jdy(row['after_values'], payload_map)
                                    if not data_payload:
                                        log_sync_error(task_config=task_in_loop,
                                                       payload=row['after_values'],
                                                       extra_info=f"task_id:[{task_id_safe}] Row missing required fields. Skipping.")
                                        continue

                                    create_response = data_api_create.create_single_data(
                                        app_id, entry_id,  # 使用局部变量
                                        data_payload, transaction_id=trans_id
                                    )
                                    new_jdy_id = create_response.get('data', {}).get('_id')
                                    if not new_jdy_id:
                                        log_sync_error(task_config=task_in_loop,
                                                       payload=data_payload,
                                                       error=create_response,
                                                       extra_info=f"[{thread_name}] Failed to create data.")
                                    else:
                                        logger.debug(
                                            f"task_id:[{task_id_safe}] Update event: Jdy ID not found, Created data with _id: {new_jdy_id}")

                            elif isinstance(binlog_event, DeleteRowsEvent):
                                jdy_id = row['values'].get('_id') or self._find_jdy_id_by_pk(
                                    task_in_loop, row['values'],  # 使用 task_in_loop
                                    data_api_query, data_api_delete, alias_map
                                )

                                if jdy_id:
                                    logger.debug(
                                        f"task_id:[{task_id_safe}] DeleteEvent: ID {jdy_id} found. Deleting...")
                                    delete_response = data_api_delete.delete_single_data(
                                        app_id, entry_id, jdy_id  # 使用局部变量
                                    )
                                    success = delete_response.get('status')
                                    if not success:
                                        log_sync_error(task_config=task_in_loop,
                                                       payload=row['values'],
                                                       error=delete_response,
                                                       extra_info=f"[{thread_name}] Failed to delete data.")
                                    else:
                                        logger.debug(f"task_id:[{task_id_safe}] Deleted data with _id: {jdy_id}")
                                else:
                                    log_sync_error(task_config=task_in_loop,
                                                   extra_info=f"[{thread_name}] Delete event skipped: Jdy ID not found.",
                                                   payload=row['values'])

                        # 8. 事件处理完成后, 在会话内更新位置
                        #    使用我们在此循环开始时捕获的位置
                        self._update_task_status(loop_session, task_in_loop, 'running',
                                                 current_log_file, current_log_pos)

                except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as api_err:
                    # API 错误, 记录日志但不停止监听器
                    # 在 except 块中创建一个新会话来获取 'live' 的 task 对象进行日志记录
                    with ConfigSession() as error_session:
                        error_task = error_session.query(SyncTask).get(task_id_safe)
                        log_sync_error(task_config=error_task or task, error=api_err,
                                       extra_info=f"[{thread_name}] API error during binlog event processing (will retry).")
                    time.sleep(10)  # 发生 API 错误时暂停
                except OperationalError as db_err:
                    # 数据库连接错误, 记录日志但不停止监听器
                    with ConfigSession() as error_session:
                        error_task = error_session.query(SyncTask).get(task_id_safe)
                        log_sync_error(task_config=error_task or task, error=db_err,
                                       extra_info=f"[{thread_name}] DB OperationalError (will retry).")
                    time.sleep(10)  # 发生 DB 错误时暂停
                except Exception as event_err:
                    # 其他事件处理错误, 记录日志并跳过此事件
                    with ConfigSession() as error_session:
                        error_task = error_session.query(SyncTask).get(task_id_safe)
                        log_sync_error(task_config=error_task or task, error=event_err,
                                       extra_info=f"[{thread_name}] Error processing binlog event (skipping).")
                        # 仍然在会话内更新位置, 以跳过错误事件
                        if error_task:
                            self._update_task_status(error_session, error_task, 'running',
                                                     current_log_file, current_log_pos)


        except Exception as e:
            # 这是启动监听器时的严重错误 (例如连接失败)
            with ConfigSession() as error_session:
                # 尝试获取 'live' 的 task 对象进行日志记录
                error_task = error_session.query(SyncTask).get(task_id_safe)
                log_sync_error(task_config=error_task or task, error=e,
                               extra_info=f"[{thread_name}] CRITICAL error. Listener stopped.")
                # 确保状态被更新
                if error_task:
                    self._update_task_status(error_session, error_task, status='error')

        finally:
            if stream:
                stream.close()
            logger.warning(f"[{thread_name}] Listener shut down.")
            with ConfigSession() as session:
                task_status = session.query(SyncTask).get(task_id_safe)
                if task_status and task_status.sync_status == 'running':
                    self._update_task_status(session, task_status, status='idle')  # 正常关闭
