import datetime
import time

from flask import current_app
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from requests import RequestException, HTTPError
from sqlalchemy import text

from app.jdy_api import DataApi, FormApi
from app.models import db, SyncTask, FormFieldMapping
from app.utils import log_sync_error, send_wecom_notification, json_serializer, retry


class FieldMappingService:
    """
    处理字段映射的服务
    """

    @retry(retries=3, delay=5)  # 为字段映射添加服务层重试
    def _get_mappings(self, task_id):
        """
        从数据库缓存中获取所有字段映射
        如果缓存为空，则从 API 更新
        """
        mappings = FormFieldMapping.query.filter_by(task_id=task_id).all()
        if not mappings:
            current_app.logger.warning(f"[Task {task_id}] No field mappings found in cache. Attempting to update.")
            task = SyncTask.query.get(task_id)
            if task:
                self.update_form_fields_mapping(task)
                mappings = FormFieldMapping.query.filter_by(task_id=task_id).all()
            else:
                current_app.logger.error(f"[Task {task_id}] Task not found, cannot update mappings.")
                return []
        return mappings

    def get_payload_mapping(self, task_id):
        """
        获取用于构建 API 负载 (data) 的映射
        :return: dict {widget_alias: widget_name} (e.g., {'dept_name': '_widget_123'})
        """
        mappings = self._get_mappings(task_id)
        return {m.widget_alias: m.widget_name for m in mappings}

    # def get_alias_mapping(self, task_id):
    #     """
    #     获取用于 API 查询 (filter) 和 PK 验证的映射
    #     :return: dict {widget_alias: widget_alias} (e.g., {'dept_name': 'dept_name'})
    #     """
    #     mappings = self._get_mappings(task_id)
    #     return {m.widget_alias: m.widget_alias for m in mappings}

    @retry(retries=3, delay=5)
    def update_form_fields_mapping(self, task: SyncTask):
        """
        从简道云 API 获取最新字段，并更新到数据库缓存
        """
        current_app.logger.info(f"[Task {task.task_id}] Updating field mappings from JDY API...")
        try:
            api_key = task.jdy_api_key
            host = current_app.config['JDY_API_HOST']
            if not api_key:
                raise Exception(f"Task {task.task_id} missing jdy_api_key")

            form_api = FormApi(api_key=api_key, host=host)

            # 调用 get_form_widgets
            response = form_api.get_form_widgets(task.jdy_app_id, task.jdy_entry_id)

            # 解析响应
            jdy_fields = response.get('widgets', response.get('data', response))
            if not isinstance(jdy_fields, list):
                raise Exception(f"Failed to parse widgets list from API response: {response}")

            if not jdy_fields:
                current_app.logger.warning(f"[Task {task.task_id}] JDY API returned no fields.")
                return

            # 1. 删除旧映射
            FormFieldMapping.query.filter_by(task_id=task.task_id).delete()

            # 2. 插入新映射
            new_mappings = []
            for field in jdy_fields:
                # 简道云widget别名 (e.g., 'name', 'department',  '部门')
                widget_alias = field.get('name')
                # 简道云widget字段ID (e.g., '_widget_12345')
                widget_name = field.get('widgetName')
                # 简道云表单字段名
                widget_label = field.get('label')
                # 字段类型 (e.g., 'text', 'select')
                widget_type = field.get('type')

                # # 如果 API 响应中没有 widget_name，我们回退到使用 name
                # if not widget_name:
                #     widget_name = widget_alias
                #     current_app.logger.warning(
                #         f"[Task {task.task_id}] Field '{widget_alias}' missing 'widget_name' key, falling back to 'name'.")

                # 核心逻辑：MySQL 列名 (column_name) 与 简道云后端别名 (widget_alias/name) 一致

                new_map = FormFieldMapping(
                    task_id=task.task_id,
                    form_name=None,
                    widget_name=widget_name,
                    widget_alias=widget_alias,
                    label=widget_label,
                    widget_type=widget_type
                )
                new_mappings.append(new_map)

            db.session.bulk_save_objects(new_mappings)
            db.session.commit()
            current_app.logger.info(f"[Task {task.task_id}] Updated {len(new_mappings)} field mappings.")

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"[Task {task.task_id}] Failed to update field mappings: {e}")
            log_sync_error(task.task_id, f"更新字段映射失败: {e}")
            raise


class SyncService:
    """
    核心同步逻辑服务
    """

    def __init__(self):
        self.mapping_service = FieldMappingService()
        self.view_cache = {}  # 用于缓存表是否为视图

    def get_task(self, task_id):
        return SyncTask.query.get(task_id)

    def _get_source_db_engine(self):
        """获取源数据库的 bind engine"""
        return db.session.get_bind('source_db')

    def _is_view(self, task: SyncTask):
        """
        检查源表是否为视图
        """
        if task.task_id in self.view_cache:
            return self.view_cache[task.task_id]

        is_view = False
        try:
            db_name = current_app.config['SOURCE_DB_NAME']
            table_name = task.source_table
            engine = self._get_source_db_engine()
            with engine.connect() as conn:
                query = text(
                    "SELECT table_type FROM information_schema.tables "
                    "WHERE table_schema = :db_name AND table_name = :table_name"
                )
                result = conn.execute(query, {"db_name": db_name, "table_name": table_name}).fetchone()

                if result and result[0] == 'VIEW':
                    is_view = True

            self.view_cache[task.task_id] = is_view
            return is_view

        except Exception as e:
            current_app.logger.error(f"[Task {task.task_id}] Failed to check if table is view: {e}")
            # 出错时，为安全起见假设它不是视图
            return False

    def _transform_row_to_jdy(self, row, payload_map):
        """
        将数据库行 (RowProxy 或 dict) 转换为简道云 data 字典
        """
        jdy_data = {}
        row_dict = dict(row)  # 转换为字典

        # 遍历 MySQL 行的 {列名: 值}
        for mysql_col, value in row_dict.items():
            # 根据列名 (column_name) 查找对应的 API 提交键 (widget_name)
            widget_name = payload_map.get(mysql_col)

            # 如果找到了映射关系
            if widget_name and value is not None:
                try:
                    # 主动序列化 Decimal, datetime 等
                    serialized_value = json_serializer(value)
                except TypeError:
                    # 对于其他类型，转为字符串
                    serialized_value = str(value)

                # jdy_data['_widget_123'] = {"value": ...}
                jdy_data[widget_name] = {"value": serialized_value}
        return jdy_data

    def _update_task_status(self, task_id, status, message=None, last_sync_time=None, binlog_file=None,
                            binlog_pos=None):
        """
        更新任务状态和日志 (在 app 上下文中调用)
        """
        try:
            task = SyncTask.query.get(task_id)
            if not task:
                current_app.logger.error(f"Task {task_id} not found during status update.")
                return

            task.status = status
            if message:
                task.last_error_message = message
            if last_sync_time:
                task.last_sync_time = last_sync_time
            if binlog_file:
                task.last_binlog_file = binlog_file
            if binlog_pos:
                task.last_binlog_pos = binlog_pos

            db.session.commit()
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"[Task {task_id}] CRITICAL: Failed to update task status: {e}")

    def _find_jdy_id_by_pk(self, task: SyncTask, pk_field_alias, pk_value):
        """
        通过业务主键在简道云中查找对应的 _id
        (使用 pk_field_alias, 即 'name' 字段)
        """
        try:
            host = current_app.config['JDY_API_HOST']
            # 实例化用于 'list' (QPS 30) 的客户端
            list_api = DataApi(api_key=task.jdy_api_key, host=host, qps=30)

            data_filter = {
                "rel": "and",
                "cond": [
                    {"field": pk_field_alias, "type": "text", "method": "eq", "value": pk_value}
                ]
            }
            # query_list_data 返回的是 data 列表
            result_list = list_api.query_list_data(
                app_id=task.jdy_app_id,
                entry_id=task.jdy_entry_id,
                fields=[pk_field_alias],
                limit=1,
                filter=data_filter
            )
            if result_list:
                return result_list[0]['_id']

        except Exception as e:
            current_app.logger.error(f"Failed to find JDY ID by PK ({pk_field_alias}={pk_value}): {e}")
        return None

    def _writeback_id_to_source(self, task: SyncTask, pk_value, jdy_id):
        """
        将简道云 _id 回写到源数据库
        (此操作在视图上会失败，这是符合预期的)
        """
        # 1. 检查是否为视图
        if self._is_view(task):
            current_app.logger.debug(
                f"[Task {task.task_id}] Source is a VIEW. Skipping _id writeback for PK={pk_value}.")
            return

        # 2. 正常回写
        id_field_in_source = '_id'
        try:
            engine = self._get_source_db_engine()
            with engine.connect() as conn:
                stmt = text(
                    f"UPDATE {task.source_table} "
                    f"SET {id_field_in_source} = :jdy_id "
                    f"WHERE {task.pk_field_name} = :pk_value"
                )
                conn.execute(stmt, {"jdy_id": jdy_id, "pk_value": pk_value})
                conn.commit()
            current_app.logger.info(f"[Task {task.task_id}] Writeback _id={jdy_id} for PK={pk_value} success.")
        except Exception as e:
            current_app.logger.warning(f"[Task {task.task_id}] Failed to writeback _id to source table: {e}")
            log_sync_error(task.task_id, f"回写 _id 失败 (PK: {pk_value}): {e}")

    # --- 三种同步模式的实现 ---

    def run_full_replace(self, task: SyncTask):
        """
        执行 FULL_REPLACE 同步
        """
        current_app.logger.info(f"[Task {task.task_id}] Starting FULL_REPLACE...")

        # 1. 检查是否为视图
        if self._is_view(task):
            msg = "FULL_REPLACE mode is not allowed for VIEWs."
            current_app.logger.error(f"[Task {task.task_id}] {msg}")
            self._update_task_status(task.task_id, "error", message=msg)
            return

        try:
            host = current_app.config['JDY_API_HOST']
            # 按需实例化 API 客户端
            list_api = DataApi(task.jdy_api_key, host, qps=30)
            batch_delete_api = DataApi(task.jdy_api_key, host, qps=10)
            batch_create_api = DataApi(task.jdy_api_key, host, qps=10)

            payload_map = self.mapping_service.get_payload_mapping(task.task_id)
            if not payload_map:
                raise Exception("获取字段映射失败，任务终止")

            # 1. 获取简道云全量数据 ID
            current_app.logger.info(f"[Task {task.task_id}] Fetching all data from JDY...")
            jdy_data = list_api.query_list_data(task.jdy_app_id, task.jdy_entry_id, fields=['_id'])
            jdy_ids = [d['_id'] for d in jdy_data]
            current_app.logger.info(f"[Task {task.task_id}] Found {len(jdy_ids)} existing entries in JDY.")

            # 2. 删除简道云全量数据
            if jdy_ids:
                # 批量删除
                chunk_size = 100
                for i in range(0, len(jdy_ids), chunk_size):
                    chunk = jdy_ids[i:i + chunk_size]
                    deleted_count = batch_delete_api.delete_batch_data(task.jdy_app_id, task.jdy_entry_id, chunk)
                    current_app.logger.info(f"[Task {task.task_id}] Deleted {deleted_count} entries...")
                    time.sleep(1)

            # 3. 获取源数据库全量数据
            current_app.logger.info(f"[Task {task.task_id}] Fetching all data from source DB...")
            engine = self._get_source_db_engine()
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {task.source_table}"))
                source_rows = result.fetchall()

            # 4. 转换并分批插入简道云
            jdy_data_list = []
            for row in source_rows:
                # 使用新的转换方法
                jdy_data = self._transform_row_to_jdy(row, payload_map)
                if jdy_data:
                    jdy_data_list.append(jdy_data)

            current_app.logger.info(f"[Task {task.task_id}] Inserting {len(jdy_data_list)} new entries to JDY...")
            total_success = 0
            chunk_size = 100
            for i in range(0, len(jdy_data_list), chunk_size):
                chunk = jdy_data_list[i:i + chunk_size]
                # create_batch_data 返回 {"success_count": N, "fail_list": [...]}
                response_dict = batch_create_api.create_batch_data(task.jdy_app_id, task.jdy_entry_id, chunk)
                success_count = response_dict.get('success_count', 0)
                fail_list = response_dict.get('fail_list', [])

                total_success += success_count
                if fail_list:
                    current_app.logger.error(f"[Task {task.task_id}] Batch create failed for {len(fail_list)} items.")
                    for fail in fail_list:
                        log_sync_error(task.task_id, f"批量创建失败: {fail.get('error_msg') or fail.get('msg')}",
                                       fail.get('data'))
                time.sleep(1)

            self._update_task_status(task.task_id, "running", message="Full replace completed.",
                                     last_sync_time=datetime.datetime.utcnow())
            current_app.logger.info(
                f"[Task {task.task_id}] FULL_REPLACE finished. Total success: {total_success}/{len(jdy_data_list)}")
            send_wecom_notification(task.wecom_bot_key, f"同步完成: {task.task_name}",
                                    f"模式: 全量替换\n成功: {total_success}/{len(jdy_data_list)}")

        except Exception as e:
            current_app.logger.error(f"[Task {task.task_id}] FULL_REPLACE failed: {e}")
            self._update_task_status(task.task_id, "error", message=str(e))
            log_sync_error(task.task_id, f"FULL_REPLACE 失败: {e}")
            send_wecom_notification(task.wecom_bot_key, f"同步失败: {task.task_name}", f"错误: {e}")

    def run_incremental(self, task: SyncTask):
        """
        执行 INCREMENTAL 同步
        """
        current_app.logger.info(f"[Task {task.task_id}] Starting INCREMENTAL...")

        if not task.incremental_field or not task.last_sync_time:
            current_app.logger.error(f"[Task {task.task_id}] 增量字段或上次同步时间未配置，任务终止。")
            self._update_task_status(task.task_id, "error", message="增量字段或上次同步时间未配置")
            return

        try:
            host = current_app.config['JDY_API_HOST']
            # 实例化 API 客户端
            single_update_api = DataApi(task.jdy_api_key, host, qps=20)
            single_create_api = DataApi(task.jdy_api_key, host, qps=20)

            payload_map = self.mapping_service.get_payload_mapping(task.task_id)
            # alias_map = self.mapping_service.get_alias_mapping(task.task_id)

            # if not payload_map or not alias_map:
            if not payload_map:
                raise Exception("获取字段映射失败，任务终止")

            ## task.pk_field_name (e.g., 'dept_name')
            ## alias_map 是 {'dept_name': 'dept_name'}
            # if task.pk_field_name not in alias_map:
            #    raise Exception(f"主键 '{task.pk_field_name}' (widget_alias) 未在字段映射中，无法执行增量同步。")

            # jdy_pk_alias = alias_map[task.pk_field_name]
            jdy_pk_alias = task.pk_field_name
            id_field_in_source = '_id'

            start_time = task.last_sync_time
            current_sync_time = datetime.datetime.utcnow()

            # 1. 查询源数据库中更新的数据
            engine = self._get_source_db_engine()
            with engine.connect() as conn:
                query = text(
                    f"SELECT * FROM {task.source_table} "
                    f"WHERE {task.incremental_field} >= :start_time"
                )
                result = conn.execute(query, {"start_time": start_time})
                updated_rows = result.fetchall()

            current_app.logger.info(f"[Task {task.task_id}] Found {len(updated_rows)} updated rows since {start_time}.")
            if not updated_rows:
                self._update_task_status(task.task_id, "running", last_sync_time=current_sync_time)
                current_app.logger.info(f"[Task {task.task_id}] No updates found. Task finished.")
                return

            # 2. 逐条处理 (Upsert 逻辑)
            create_count = 0
            update_count = 0
            fail_count = 0

            for row in updated_rows:
                row_dict = dict(row)
                # 使用 payload_map (column -> widget_name) 转换
                jdy_data = self._transform_row_to_jdy(row, payload_map)
                pk_value = row_dict[task.pk_field_name]
                existing_jdy_id = row_dict.get(id_field_in_source)

                try:
                    target_jdy_id = existing_jdy_id
                    # 如果源表中没有 _id (例如，是视图，或回写失败)
                    if not target_jdy_id:
                        # (视图) 或 (非视图但回写失败) -> 尝试通过 PK 查找
                        target_jdy_id = self._find_jdy_id_by_pk(task, jdy_pk_alias, pk_value)

                    if target_jdy_id:
                        # 更新
                        single_update_api.update_single_data(task.jdy_app_id, task.jdy_entry_id, target_jdy_id,
                                                             jdy_data)
                        update_count += 1
                        # 如果源表中没有，尝试回写 (对视图会无效)
                        if not existing_jdy_id:
                            self._writeback_id_to_source(task, pk_value, target_jdy_id)
                    else:
                        # 创建
                        created_data = single_create_api.create_single_data(task.jdy_app_id, task.jdy_entry_id,
                                                                            jdy_data)
                        new_jdy_id = created_data.get('data', {}).get('_id')  # 返回 _id 在 data 嵌套中
                        if new_jdy_id:
                            create_count += 1
                            # 尝试回写 (对视图会无效)
                            self._writeback_id_to_source(task, pk_value, new_jdy_id)
                        else:
                            raise Exception(f"创建数据失败，未返回 _id: {created_data}")

                except Exception as e:
                    fail_count += 1
                    current_app.logger.error(f"[Task {task.task_id}] Failed to process row (PK: {pk_value}): {e}")
                    log_sync_error(task.task_id, f"增量同步失败 (PK: {pk_value}): {e}", row_dict)

            # 3. 更新任务状态
            self._update_task_status(task.task_id, "running", last_sync_time=current_sync_time)
            msg = f"增量同步完成。\n创建: {create_count}, 更新: {update_count}, 失败: {fail_count}"
            current_app.logger.info(f"[Task {task.task_id}] {msg}")
            send_wecom_notification(task.wecom_bot_key, f"同步完成: {task.task_name}", msg)

        except Exception as e:
            current_app.logger.error(f"[Task {task.task_id}] INCREMENTAL failed: {e}")
            self._update_task_status(task.task_id, "error", message=str(e))
            log_sync_error(task.task_id, f"INCREMENTAL 失败: {e}")
            send_wecom_notification(task.wecom_bot_key, f"同步失败: {task.task_name}", f"错误: {e}")

    def run_binlog_listener(self, task: SyncTask, app):
        """
        执行 BINLOG 监听
        """
        current_app.logger.info(f"[Task {task.task_id}] Starting BINLOG listener thread...")

        # 1. 检查是否为视图
        with app.app_context():
            if self._is_view(task):
                msg = "BINLOG mode is not allowed for VIEWs."
                current_app.logger.error(f"[Task {task.task_id}] {msg}")
                self._update_task_status(task.task_id, "error", message=msg)
                return

        mysql_settings = app.config['BINLOG_MYSQL_SETTINGS']
        start_file = task.last_binlog_file
        start_pos = task.last_binlog_pos

        stream = None
        try:
            stream = BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=100 + task.task_id,  # 确保 server_id 唯一
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                only_schemas=[app.config['SOURCE_DB_NAME']],
                only_tables=[task.source_table],
                log_file=start_file,
                log_pos=start_pos,
                resume_stream=True,
                blocking=True
            )

            host = app.config['JDY_API_HOST']
            # 实例化 API 客户端 (QPS 20)
            single_create_api = DataApi(task.jdy_api_key, host, qps=20)
            single_update_api = DataApi(task.jdy_api_key, host, qps=20)
            single_delete_api = DataApi(task.jdy_api_key, host, qps=20)

            # 在 app 上下文中获取字段映射
            payload_map = {}
            alias_map = {}
            jdy_pk_alias = None
            with app.app_context():
                payload_map = self.mapping_service.get_payload_mapping(task.task_id)
                # alias_map = self.mapping_service.get_alias_mapping(task.task_id)
                if not payload_map or not alias_map:
                    raise Exception("获取字段映射失败，Binlog 任务终止")

                # jdy_pk_alias = alias_map.get(task.pk_field_name)
                jdy_pk_alias = task.pk_field_name
                if not jdy_pk_alias:
                    raise Exception(f"主键 '{task.pk_field_name}' (widget_alias) 未在字段映射中，无法执行 Binlog 同步。")

            id_field_in_source = '_id'

            current_app.logger.info(f"[Task {task.task_id}] Binlog stream started at {start_file}:{start_pos}")

            for binlogevent in stream:
                # 核心：在循环内部的 *每次* 操作都使用 app_context
                with app.app_context():
                    try:
                        current_file = stream.log_file
                        current_pos = stream.log_pos

                        for row in binlogevent.rows:
                            if isinstance(binlogevent, WriteRowsEvent):
                                # 插入
                                values = row['values']
                                # 使用 payload_map (column -> widget_name) 转换
                                jdy_data = self._transform_row_to_jdy(values, payload_map)
                                pk_value = values.get(task.pk_field_name)

                                created_data = single_create_api.create_single_data(task.jdy_app_id, task.jdy_entry_id,
                                                                                    jdy_data)
                                new_jdy_id = created_data.get('data', {}).get('_id')
                                if new_jdy_id:
                                    # 尝试回写
                                    self._writeback_id_to_source(task, pk_value, new_jdy_id)
                                current_app.logger.debug(f"[Task {task.task_id}] BINLOG Create: {pk_value}")

                            elif isinstance(binlogevent, UpdateRowsEvent):
                                # 更新
                                after_values = row['after_values']
                                # 使用 payload_map (column -> widget_name) 转换
                                jdy_data = self._transform_row_to_jdy(after_values, payload_map)
                                pk_value = after_values.get(task.pk_field_name)
                                jdy_id = after_values.get(id_field_in_source)

                                if not jdy_id:
                                    jdy_id = self._find_jdy_id_by_pk(task, jdy_pk_alias, pk_value)

                                if jdy_id:
                                    single_update_api.update_single_data(task.jdy_app_id, task.jdy_entry_id, jdy_id,
                                                                         jdy_data)
                                    current_app.logger.debug(f"[Task {task.task_id}] BINLOG Update: {pk_value}")
                                else:
                                    created_data = single_create_api.create_single_data(task.jdy_app_id,
                                                                                        task.jdy_entry_id, jdy_data)
                                    new_jdy_id = created_data.get('data', {}).get('_id')
                                    if new_jdy_id:
                                        self._writeback_id_to_source(task, pk_value, new_jdy_id)
                                    current_app.logger.warning(
                                        f"[Task {task.task_id}] BINLOG Update-to-Create: {pk_value}")

                            elif isinstance(binlogevent, DeleteRowsEvent):
                                # 删除
                                values = row['values']
                                pk_value = values.get(task.pk_field_name)
                                jdy_id = values.get(id_field_in_source)

                                if not jdy_id:
                                    jdy_id = self._find_jdy_id_by_pk(task, jdy_pk_alias, pk_value)

                                if jdy_id:
                                    single_delete_api.delete_single_data(task.jdy_app_id, task.jdy_entry_id, jdy_id)
                                    current_app.logger.debug(f"[Task {task.task_id}] BINLOG Delete: {pk_value}")
                                else:
                                    current_app.logger.warning(
                                        f"[Task {task.task_id}] BINLOG Delete skipped (JDY ID not found): {pk_value}")

                        # 处理完一个 event，更新位置
                        self._update_task_status(task.task_id, "running", binlog_file=current_file,
                                                 binlog_pos=current_pos)

                    # 3. 捕获 requests 异常
                    except (RequestException, HTTPError) as e:
                        current_app.logger.error(f"[Task {task.task_id}] BINLOG API Error: {e}")
                        log_sync_error(task.task_id, f"BINLOG API 错误: {e}")
                        time.sleep(5)
                    except Exception as e:
                        current_app.logger.error(f"[Task {task.task_id}] BINLOG processing error: {e}")
                        log_sync_error(task.task_id, f"BINLOG 处理失败: {e}")
                        # 其他错误可能较严重，也继续尝试
                        time.sleep(5)

        except Exception as e:
            # stream 启动失败或严重错误
            with app.app_context():
                current_app.logger.error(f"[Task {task.task_id}] BINLOG listener thread CRASHED: {e}")
                self._update_task_status(task.task_id, "error", message=f"Binlog 监听器崩溃: {e}")
                log_sync_error(task.task_id, f"Binlog 监听器崩溃: {e}")
                send_wecom_notification(task.wecom_bot_key, f"同步失败: {task.task_name}", f"Binlog 监听器崩溃: {e}")

        finally:
            if stream:
                stream.close()
            with app.app_context():
                current_app.logger.info(f"[Task {task.task_id}] BINLOG listener thread stopped.")

    def update_id_from_webhook(self, task_id, business_pk_value, jdy_id):
        """
        Webhook 调用的回写服务
        """
        current_app.logger.info(f"[Webhook Task {task_id}] Received _id={jdy_id} for PK={business_pk_value}")
        task = self.get_task(task_id)
        if not task:
            current_app.logger.error(f"[Webhook Task {task_id}] Task not found.")
            return

        if task.pk_field_name is None:
            current_app.logger.error(f"[Webhook Task {task_id}] Task PK field not configured.")
            return

        try:
            # 此方法内部有视图检查
            self._writeback_id_to_source(task, business_pk_value, jdy_id)
        except Exception as e:
            current_app.logger.error(f"[Webhook Task {task_id}] Failed to writeback from webhook: {e}")
            log_sync_error(task_id, f"Webhook 回写失败: {e}")
