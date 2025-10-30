import datetime
import time

from flask import current_app
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from sqlalchemy import text

from app.jdy_api import DataApi, FormApi, JdyApiError
from app.models import db, SyncTask, FormFieldMapping
from app.utils import log_sync_error, send_wecom_notification


class FieldMappingService:
    """
    处理字段映射的服务
    """

    def get_field_map(self, task_id):
        """
        从数据库缓存中获取字段映射
        :return: dict {source_field: jdy_field}
        """
        mappings = FormFieldMapping.query.filter_by(task_id=task_id).all()
        if not mappings:
            current_app.logger.warning(f"[Task {task_id}] No field mappings found in cache. Attempting to update.")
            task = SyncTask.query.get(task_id)
            if task:
                try:
                    self.update_form_fields_mapping(task)
                    mappings = FormFieldMapping.query.filter_by(task_id=task_id).all()
                except Exception as e:
                    current_app.logger.error(f"[Task {task_id}] Failed to update field mappings: {e}")
                    return {}
            else:
                current_app.logger.error(f"[Task {task_id}] Task not found, cannot update mappings.")
                return {}

        return {m.source_field: m.jdy_field for m in mappings}

    def update_form_fields_mapping(self, task: SyncTask):
        """
        从简道云 API 获取最新字段，并更新到数据库缓存
        """
        current_app.logger.info(f"[Task {task.task_id}] Updating field mappings from JDY API...")
        try:
            api_key = task.jdy_api_key
            if not api_key:
                raise Exception(f"Task {task.task_id} missing jdy_api_key")

            form_api = FormApi(api_key=api_key, app_id=task.jdy_app_id)

            jdy_fields = form_api.get_form_fields(task.jdy_entry_id)

            if not jdy_fields:
                current_app.logger.warning(f"[Task {task.task_id}] JDY API returned no fields.")
                return

            # 1. 删除旧映射
            FormFieldMapping.query.filter_by(task_id=task.task_id).delete()

            # 2. 插入新映射
            new_mappings = []
            for field in jdy_fields:
                jdy_api_name = field.get('name')  # 'name' 是 API 字段名
                # 假设源表中的字段名与简道云的 API 字段名一致（如果不是，需要更复杂的映射逻辑）
                # 或者，我们只映射源表中存在的字段

                # 简单起见，我们假设 API 字段名 (name) 对应 source_field
                # 在实际场景中，可能需要一个映射规则
                source_field_name = jdy_api_name

                new_map = FormFieldMapping(
                    task_id=task.task_id,
                    source_field=source_field_name,
                    jdy_field=jdy_api_name,
                    jdy_field_type=field.get('type')
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

    def get_task(self, task_id):
        return SyncTask.query.get(task_id)

    def _get_source_db_engine(self):
        """获取源数据库的 bind engine"""
        return db.session.get_bind('source_db')

    def _transform_row_to_jdy(self, row, field_map):
        """
        将数据库行 (RowProxy 或 dict) 转换为简道云 data 字典
        """
        jdy_data = {}
        row_dict = dict(row)  # 转换为字典

        for source_field, jdy_field in field_map.items():
            if source_field in row_dict:
                value = row_dict[source_field]
                # 简道云 API 不接受 None，但接受空字符串或 {}
                if value is not None:
                    jdy_data[jdy_field] = {"value": value}
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

    def _find_jdy_id_by_pk(self, data_api: DataApi, pk_field_jdy, pk_value):
        """
        通过业务主键在简道云中查找对应的 _id
        """
        data_filter = {
            "rel": "and",
            "cond": [
                {"field": pk_field_jdy, "type": "text", "method": "eq", "value": pk_value}
            ]
        }
        try:
            result = data_api.query_list_data(fields=[pk_field_jdy], limit=1, data_filter=data_filter)
            if result:
                return result[0]['_id']
        except Exception as e:
            current_app.logger.error(f"Failed to find JDY ID by PK ({pk_field_jdy}={pk_value}): {e}")
        return None

    def _writeback_id_to_source(self, task: SyncTask, pk_value, jdy_id):
        """
        将简道云 _id 回写到源数据库
        """
        # 假设源表中用于存储 _id 的字段固定为 'jdy_id'
        # 这是一个强假设，实际中可能需要配置
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
            current_app.logger.error(f"[Task {task.task_id}] Failed to writeback _id to source table: {e}")
            log_sync_error(task.task_id, f"回写 _id 失败 (PK: {pk_value}): {e}")

    # --- 三种同步模式的实现 ---

    def run_full_replace(self, task: SyncTask):
        """
        执行 FULL_REPLACE 同步
        """
        current_app.logger.info(f"[Task {task.task_id}] Starting FULL_REPLACE...")

        try:
            data_api = DataApi(task.jdy_api_key, task.jdy_app_id, task.jdy_entry_id)
            field_map = self.mapping_service.get_field_map(task.task_id)
            if not field_map:
                raise Exception("获取字段映射失败，任务终止")

            # 1. 获取简道云全量数据 ID
            current_app.logger.info(f"[Task {task.task_id}] Fetching all data from JDY...")
            jdy_data = data_api.query_list_data(fields=['_id'])
            jdy_ids = [d['_id'] for d in jdy_data]
            current_app.logger.info(f"[Task {task.task_id}] Found {len(jdy_ids)} existing entries in JDY.")

            # 2. 删除简道云全量数据
            if jdy_ids:
                # 批量删除
                chunk_size = 100
                for i in range(0, len(jdy_ids), chunk_size):
                    chunk = jdy_ids[i:i + chunk_size]
                    deleted_count = data_api.delete_batch_data(chunk)
                    current_app.logger.info(f"[Task {task.task_id}] Deleted {deleted_count} entries...")
                    time.sleep(0.5)  # 避免速率限制

            # 3. 获取源数据库全量数据
            current_app.logger.info(f"[Task {task.task_id}] Fetching all data from source DB...")
            engine = self._get_source_db_engine()
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {task.source_table}"))
                source_rows = result.fetchall()

            # 4. 转换并分批插入简道云
            jdy_data_list = []
            for row in source_rows:
                jdy_data = self._transform_row_to_jdy(row, field_map)
                if jdy_data:
                    jdy_data_list.append(jdy_data)

            current_app.logger.info(f"[Task {task.task_id}] Inserting {len(jdy_data_list)} new entries to JDY...")
            total_success = 0
            chunk_size = 100  # 批量创建 API 上限
            for i in range(0, len(jdy_data_list), chunk_size):
                chunk = jdy_data_list[i:i + chunk_size]
                success_count, fail_list = data_api.create_batch_data(chunk)
                total_success += success_count
                if fail_list:
                    current_app.logger.error(f"[Task {task.task_id}] Batch create failed for {len(fail_list)} items.")
                    for fail in fail_list:
                        log_sync_error(task.task_id, f"批量创建失败: {fail.get('error_msg')}", fail.get('data'))

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
            data_api = DataApi(task.jdy_api_key, task.jdy_app_id, task.jdy_entry_id)
            field_map = self.mapping_service.get_field_map(task.task_id)
            if not field_map:
                raise Exception("获取字段映射失败，任务终止")

            if task.pk_field_name not in field_map:
                raise Exception(f"主键 '{task.pk_field_name}' 未在字段映射中，无法执行增量同步。")

            jdy_pk_field = field_map[task.pk_field_name]
            id_field_in_source = '_id'  # 假设源表中存储 _id 的字段叫 _id

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

            # 2. 逐条处理
            create_count = 0
            update_count = 0
            fail_count = 0

            for row in updated_rows:
                row_dict = dict(row)
                jdy_data = self._transform_row_to_jdy(row, field_map)
                pk_value = row_dict[task.pk_field_name]
                existing_jdy_id = row_dict.get(id_field_in_source)

                try:
                    target_jdy_id = existing_jdy_id
                    # 如果源表中没有 _id，尝试通过 PK 去简道云查找
                    if not target_jdy_id:
                        target_jdy_id = self._find_jdy_id_by_pk(data_api, jdy_pk_field, pk_value)

                    if target_jdy_id:
                        # 更新
                        data_api.update_single_data(target_jdy_id, jdy_data)
                        update_count += 1
                        # 如果源表中没有，回写
                        if not existing_jdy_id:
                            self._writeback_id_to_source(task, pk_value, target_jdy_id)
                    else:
                        # 创建
                        created_data = data_api.create_single_data(jdy_data)
                        new_jdy_id = created_data.get('_id')
                        if new_jdy_id:
                            create_count += 1
                            # 回写
                            self._writeback_id_to_source(task, pk_value, new_jdy_id)
                        else:
                            raise Exception("创建数据失败，未返回 _id")

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
        执行 BINLOG 监听 (此函数在一个单独的线程中运行)
        ** 关键：必须传入 app 对象以创建 app_context **
        """
        current_app.logger.info(f"[Task {task.task_id}] Starting BINLOG listener thread...")

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

            data_api = DataApi(task.jdy_api_key, task.jdy_app_id, task.jdy_entry_id)

            # 在 app 上下文中获取字段映射
            with app.app_context():
                field_map = self.mapping_service.get_field_map(task.task_id)
                if not field_map:
                    raise Exception("获取字段映射失败，Binlog 任务终止")

                jdy_pk_field = field_map.get(task.pk_field_name)
                if not jdy_pk_field:
                    raise Exception(f"主键 '{task.pk_field_name}' 未在字段映射中，无法执行 Binlog 同步。")

            id_field_in_source = '_id'  # 假设

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
                                jdy_data = self._transform_row_to_jdy(values, field_map)
                                pk_value = values.get(task.pk_field_name)

                                created_data = data_api.create_single_data(jdy_data)
                                new_jdy_id = created_data.get('_id')
                                if new_jdy_id:
                                    self._writeback_id_to_source(task, pk_value, new_jdy_id)
                                current_app.logger.debug(f"[Task {task.task_id}] BINLOG Create: {pk_value}")

                            elif isinstance(binlogevent, UpdateRowsEvent):
                                # 更新
                                before_values = row['before_values']
                                after_values = row['after_values']
                                jdy_data = self._transform_row_to_jdy(after_values, field_map)
                                pk_value = after_values.get(task.pk_field_name)
                                jdy_id = after_values.get(id_field_in_source)

                                if not jdy_id:
                                    jdy_id = self._find_jdy_id_by_pk(data_api, jdy_pk_field, pk_value)

                                if jdy_id:
                                    data_api.update_single_data(jdy_id, jdy_data)
                                    current_app.logger.debug(f"[Task {task.task_id}] BINLOG Update: {pk_value}")
                                else:
                                    # 如果找不到，转为创建
                                    created_data = data_api.create_single_data(jdy_data)
                                    new_jdy_id = created_data.get('_id')
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
                                    jdy_id = self._find_jdy_id_by_pk(data_api, jdy_pk_field, pk_value)

                                if jdy_id:
                                    data_api.delete_single_data(jdy_id)
                                    current_app.logger.debug(f"[Task {task.task_id}] BINLOG Delete: {pk_value}")
                                else:
                                    current_app.logger.warning(
                                        f"[Task {task.task_id}] BINLOG Delete skipped (JDY ID not found): {pk_value}")

                        # 处理完一个 event，更新位置
                        self._update_task_status(task.task_id, "running", binlog_file=current_file,
                                                 binlog_pos=current_pos)

                    except JdyApiError as e:
                        current_app.logger.error(f"[Task {task.task_id}] BINLOG API Error: {e}")
                        log_sync_error(task.task_id, f"BINLOG API 错误: {e}")
                        # API 错误通常可重试，继续监听
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
        (新功能) Webhook 调用的回写服务
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
            self._writeback_id_to_source(task, business_pk_value, jdy_id)
        except Exception as e:
            current_app.logger.error(f"[Webhook Task {task_id}] Failed to writeback from webhook: {e}")
            log_sync_error(task_id, f"Webhook 回写失败: {e}")
