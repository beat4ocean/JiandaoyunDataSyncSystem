# -*- coding: utf-8 -*-
import json
import logging
import re
import threading
from datetime import datetime, time
from urllib.parse import quote

from sqlalchemy import inspect, Table, Column, String, text, JSON, DateTime, Text, BigInteger, Float
from sqlalchemy.dialects.mysql import insert, LONGTEXT
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql.sqltypes import TypeEngine, Integer, Boolean, Time

from app import Config
from app.database import get_dynamic_engine, get_dynamic_metadata, get_dynamic_session
from app.jdy_api import FormApi, DataApi
from app.models import (SyncTask, FormFieldMapping, ConfigSession, Department)
from app.utils import (retry, convert_to_pinyin, log_sync_error, TZ_UTC_8)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXTRACT_SCHEMA_FROM_DATA = False


class FieldMappingService:
    """
    处理简道云->数据库同步的字段映射
    """

    # @retry()
    # def get_table_name(payload: dict) -> (str, str):
    #     """从数据中生成表名，并将中文转换为拼音，确保表名合法。"""
    #     data = payload.get('data')
    #     op = payload.get('op')
    #
    #     # 优先从 data 中获取 name 或 formName
    #     form_name = None
    #     if isinstance(data, dict):
    #         # form_update 事件使用 'name'
    #         if op == 'form_update':
    #             form_name = data.get('name')
    #         # 其他事件通常使用 'formName'
    #         if not form_name:
    #             form_name = data.get('formName')
    #
    #     # # 如果 data 中没有，尝试从 payload 顶层获取
    #     # if not form_name:
    #     #     form_name = payload.get('formName')
    #     #
    #     # # 如果都没有，提供一个基于 entryId 的默认值
    #     # if not form_name:
    #     #     entry_id_from_data = data.get('entryId') if isinstance(data, dict) else None
    #     #     entry_id_from_payload = payload.get('entry_id')
    #     #     entry_id = entry_id_from_data or entry_id_from_payload
    #     #     form_name = f"unknown_form_{entry_id}" if entry_id else "unknown_form"
    #     #     logger.warning(f"Unable to determine form name from payload; using default value: {form_name}")
    #
    #     form_pinyin_name = convert_to_pinyin(form_name)
    #     # 返回原始名称和转换后的名称
    #     return form_pinyin_name

    @retry()
    def get_column_name(self, widget: dict, use_label_pinyin: bool) -> str:
        """
        根据动态配置获取数据库列名。
        1. 字段别名 (name)，如果不是默认的 _widget_xxx
        2. 字段标题 (label)，根据配置决定是否转拼音
        3. 字段ID (widgetName) 作为备用
        :param widget: 简道云的字段对象
        :param use_label_pinyin: 是否将 Label 转为拼音
        :return: 数据库列名
        """
        widget_name = widget.get('widgetName')  # 这是字段 ID，通常是 _widget_xxx
        widget_alias = widget.get('name')
        label = widget.get('label')

        final_name = None

        # 1. 优先使用别名 (如果已设置且非默认)
        if widget_alias and isinstance(widget_alias, str) and not widget_alias.startswith('_widget_'):
            # 别名通常是用户自定义的，可能包含非法字符，需要清理
            final_name = widget_alias
            # logger.debug(f"使用别名: {widget_alias} -> {final_name}")

        # 2. 其次使用 label
        if not final_name and label and isinstance(label, str):
            if use_label_pinyin:
                final_name = convert_to_pinyin(label)
                # logger.debug(f"使用标签 (拼音): {label} -> {final_name}")
            else:
                final_name = label
                # logger.debug(f"使用标签 (原文清理): {label} -> {final_name}")

        # 3. 最后备用 widgetName (字段 ID)
        if not final_name and widget_name and isinstance(widget_name, str):
            # widgetName 通常是 _widget_xxx
            final_name = widget_name
            # logger.debug(f"使用 Widget Name: {widget_name} -> {final_name}")

        # 4. 清理 final_name 中的非法字符
        if final_name:
            # # 替换所有非字母、数字、下划线的字符为空字符串
            # final_name = re.sub(r'[^a-zA-Z0-9_]', '', final_name)
            # 保留中文、英文、数字、下划线
            final_name = re.sub(r'[^a-zA-Z0-9_\u4e00-\u9fff]', '', final_name)
            # 确保不以数字开头 (如果数据库有此限制)
            if final_name and final_name[0].isdigit():
                final_name = '_' + final_name

        # 添加一个最终的非空检查
        if not final_name:
            logger.warning(f"Unable to generate valid column names for widget {widget}.")
            final_name = None

        return final_name

    @retry()
    def get_sql_type(self, type: str | None, data_value: any) -> TypeEngine:
        """
        根据简道云的字段类型（优先）或值的Python类型推断出合适的 SQLAlchemy 数据类型。
        :param type: 从 `form_fields_mapping` 表中获取的简道云字段类型字符串。
        :param data_value: 字段的实际值，用于备用推断。
        :return: SQLAlchemy 类型实例 (e.g., Text(), Float(), JSON())。
        """
        # 简道云字段类型到SQLAlchemy类型的映射 (返回类型类)
        JDY_TYPE_TO_SQLALCHEMY_CLASS = {
            'text': Text, 'textarea': Text, 'serial_number': String,  # Use String for serial
            'radiogroup': String, 'combo': String, 'calculation': Text,  # Calculation might be long
            'number': Float, 'money': Float,  # Add money type
            'datetime': DateTime, 'date': DateTime, 'time': Time,  # Add date and time
            'address': JSON, 'location': JSON, 'signature': JSON,
            'user': JSON, 'dept': JSON, 'phone': JSON, 'member': JSON,  # member is alias for user/dept
            'lookup': JSON, 'linkdata': JSON, 'formula': JSON,  # Treat formula results as potentially complex
            'checkboxgroup': JSON, 'combocheck': JSON, 'image': JSON,
            'upload': JSON, 'subform': JSON, 'widget_relation': JSON,  # Relation widget
            'usergroup': JSON, 'deptgroup': JSON,
            'cascader': JSON,  # Add cascader
            'rate': Float,  # Add rate
            'progress': Integer,  # Add progress
            'autonumber': BigInteger,  # Add autonumber (treat as big int)
            'flowstate': BigInteger,  # Keep as BigInteger
            'boolean': Boolean  # Add boolean explicitly
        }

        sql_type_class = None

        # 1. 优先根据简道云的字段类型进行映射
        if type and type in JDY_TYPE_TO_SQLALCHEMY_CLASS:
            sql_type_class = JDY_TYPE_TO_SQLALCHEMY_CLASS[type]
            # logger.debug(f"JDY Type '{jdy_type}' mapped to {sql_type_class.__name__}")

        # 2. 如果类型映射成功，但需要根据值调整 (例如 String vs Text)
        if sql_type_class:
            if sql_type_class in (Text, String) and isinstance(data_value, str):
                if len(data_value) > 65535:
                    # logger.debug(f"Value length > 65535, promoting to LONGTEXT")
                    return LONGTEXT()
                elif len(data_value) > 1024:
                    # logger.debug(f"Value length > 1024, promoting to TEXT")
                    return Text()
                else:
                    # logger.debug(f"Value fits in String(1024)")
                    # For serial_number, radiogroup, combo, allow longer String if needed, e.g., String(255)
                    # Adjust based on expected max length for these types
                    return String(1024)

            elif sql_type_class is Float and isinstance(data_value, int):
                # Allow integers to be stored in Float columns
                # logger.debug("Integer value for Float type, allowed.")
                return Float()

            elif sql_type_class is BigInteger and isinstance(data_value, (int, str)):
                # Allow potential string representation of big integers
                try:
                    int(data_value)  # Check if convertible
                    # logger.debug("String/Int value for BigInteger type, allowed.")
                    return BigInteger()
                except (ValueError, TypeError):
                    logger.warning(
                        f"Value '{data_value}' cannot be converted to BigInteger; Text will be used instead.")
                    return Text()  # Fallback if value cannot be converted
            # If type is mapped and doesn't need value adjustment, return instance
            # logger.debug(f"Returning instance: {sql_type_class.__name__}()")

            return sql_type_class()  # 修正：返回实例

        # 3. 如果没有提供简道云类型或类型未知，则回退到基于值的推断
        # logger.debug(f"No JDY type or unknown type '{jdy_type}', inferring from value type: {type(data_value).__name__}")
        if isinstance(data_value, bool):
            return Boolean()
        if isinstance(data_value, int):
            # Consider magnitude for Int vs BigInt if necessary
            return BigInteger()  # Default to BigInteger for safety
        if isinstance(data_value, float):
            return Float()
        if isinstance(data_value, (dict, list)):
            return JSON()
        if isinstance(data_value, str):
            # 尝试检查是否为日期时间格式
            try:
                # Add more robust date/time checking if needed
                if len(data_value) >= 10:  # Basic check
                    # Try parsing common formats
                    datetime.fromisoformat(data_value.replace('Z', '+00:00'))
                    return DateTime()
            except (ValueError, TypeError):
                pass  # Not a standard ISO datetime

            # 检查是否像时间 "HH:MM:SS"
            if re.match(r'^\d{2}:\d{2}:\d{2}$', data_value):
                try:
                    time.fromisoformat(data_value)
                    return Time()
                except ValueError:
                    pass  # Not a valid time string

            # 根据长度决定使用 String/TEXT/LONGTEXT
            if len(data_value) > 65535:
                return LONGTEXT()
            elif len(data_value) > 1024:
                return Text()
            return String(1024)  # Default string length

        # Default fallback for unknown types
        # logger.info("Unknown value type, falling back to Text()")
        return Text()

    @retry()
    def create_or_update_form_fields_mapping(self, config_session: Session, task_config: SyncTask,
                                             api_client: FormApi, form_name: str):
        """
        同步字段映射：(用于数据同步时的首次创建)
        1. 从 API 拉取最新的字段列表。
        2. 与数据库中已有的映射进行比对。
        3. 更新(Update)已存在的字段信息。
        4. 插入(Insert)新增的字段。
        5. 删除(Delete)在 API 响应中已不存在的字段。
        """
        app_id = task_config.app_id
        entry_id = task_config.entry_id
        task_id = task_config.id  # (新增) 使用 task_id

        if not all([app_id, entry_id]):
            logger.error(
                f"task_id:[{task_id}] is missing App ID or Entry ID, cannot sync field mappings.")
            return

        # # api_client 可能为 None (如果调用者是 update_table_schema_from_form)
        # if not api_client:
        #     if not task_config.department or not task_config.department.jdy_key_info:
        #         raise ValueError(f"task_id:[{task_id}] Missing department/key info for mapping update")
        #     api_client = FormApi(
        #         api_key=task_config.department.jdy_key_info.api_key,
        #         host=Config.JDY_API_BASE_URL
        #     )
        #     logger.debug(
        #         f"task_id:[{task_id}] create_or_update_form_fields_mapping: api_client was None, created new one.")

        logger.info(
            f"task_id:[{task_id}] Received data for {app_id}/{entry_id}, syncing field mappings...")

        try:
            # 1. 从 API 获取最新的“目标”字段列表
            resp = api_client.get_form_widgets(app_id, entry_id)
            api_widgets = resp.get('widgets', [])

            # 如果 form_name 未传入（来自 update_table_schema_from_form），则从 API 获取
            if not form_name:
                form_name = resp.get('name') or resp.get('formName', '')

            if not api_widgets:
                logger.warning(
                    f"task_id:[{task_id}] API returned no field information, skipping.")
                # 根据需求，可删除所有现有映射
                # config_session.query(FormFieldMapping).filter_by(app_id=app_id, entry_id=entry_id).delete()
                # config_session.commit()
                return

            # 2. 获取数据库中已存在的“当前”映射
            existing_mappings_query = config_session.query(FormFieldMapping).filter_by(task_id=task_id)

            # 3. 将已存在映射转为 {widget_name: mapping_object} 的字典
            existing_map = {m.widget_name: m for m in existing_mappings_query.all()}

            mappings_to_add = []
            mappings_to_delete = []

            # 4. 遍历 API 传来的新字段，执行 Upsert
            api_widget_names = set()  # 用于跟踪 API 返回的 widget names
            for widget in api_widgets:
                widget_name = widget.get('widgetName')
                if not widget_name:
                    logger.warning(f"task_id:[{task_id}] Widget returned from API is missing widgetName: {widget}")
                    continue
                api_widget_names.add(widget_name)

                # # 统一计算列名
                # column_name = self.get_column_name(widget, task_config.label_to_pinyin)

                # 检查这个字段是否已存在于数据库
                mapping_to_update = existing_map.get(widget_name)

                if mapping_to_update:
                    # --- Case 1: 存在，检查是否需要更新 ---
                    # 标记已处理
                    existing_map.pop(widget_name)
                    # 检查字段属性是否有变化
                    if (mapping_to_update.form_name != form_name or
                            mapping_to_update.widget_alias != widget.get('name') or
                            mapping_to_update.label != widget.get('label') or
                            mapping_to_update.type != widget.get('type')):
                        mapping_to_update.form_name = form_name
                        mapping_to_update.widget_alias = widget.get('name')
                        mapping_to_update.label = widget.get('label')
                        mapping_to_update.type = widget.get('type')
                        # Session 会自动跟踪变更
                else:
                    # --- Case 2: 不存在，执行插入 ---
                    new_mapping = FormFieldMapping(
                        task_id=task_id,
                        # app_id=app_id,
                        # entry_id=entry_id,
                        form_name=form_name,
                        widget_name=widget_name,
                        widget_alias=widget.get('name'),
                        label=widget.get('label'),
                        type=widget.get('type'),
                    )
                    mappings_to_add.append(new_mapping)

            # 5. 处理已删除的字段 (存在于 existing_map 中，但未在 api_widget_names 中)
            mappings_to_delete = list(existing_map.values())

            # 6. 提交所有变更
            if mappings_to_add:
                config_session.add_all(mappings_to_add)
                logger.info(f"task_id:[{task_id}] Adding {len(mappings_to_add)} new field mappings.")
            if mappings_to_delete:
                logger.info(
                    f"task_id:[{task_id}] Detected {len(mappings_to_delete)} deleted fields, removing from mapping.")
                for m in mappings_to_delete:
                    config_session.delete(m)

            # 只有在有实际变更时才 commit
            if config_session.new or config_session.dirty or config_session.deleted:
                config_session.commit()
                logger.info(f"task_id:[{task_id}] Field mapping sync complete.")
            else:
                logger.info(f"task_id:[{task_id}] Field mappings are up-to-date.")


        except SQLAlchemyError as db_err:
            config_session.rollback()
            logger.error(f"task_id:[{task_id}] Database error during field mapping sync: {db_err}",
                         exc_info=True)
            log_sync_error(
                task_config=task_config,
                error=db_err,
                extra_info=f"Database error during form fields mapping sync"
            )
        except Exception as e:
            config_session.rollback()
            logger.error(f"task_id:[{task_id}] Failed to fetch or update field mappings: {e}",
                         exc_info=True)
            log_sync_error(
                task_config=task_config,
                error=e,
                extra_info=f"task_id:[{task_id}]Failed to create or update form fields mapping. "
            )


class Jdy2DbSyncService:
    """
    处理 简道云 -> 数据库 的核心同步逻辑 (Webhook, DDL, DML, 历史数据)
    """

    def __init__(self):
        # Cache for inspected table definitions, keyed by engine URL string
        # { "engine_url_key": { "table_name": TableObject } }
        self.inspected_tables_cache = {}
        # Instantiate the mapping service
        self.mapping_service = FieldMappingService()
        logger.info("JdyDBSyncService initialized with new cache.")

    # --- 核心 Webhook 处理器 ---

    @retry()
    def handle_webhook_data(self, config_session: Session, payload: dict, task_config: SyncTask, api_client: FormApi,
                            table_param: str = None):
        """
        处理 Webhook 推送的数据。
        :param config_session: 配置数据库会话
        :param payload: Webhook 的 JSON 数据
        :param task_config: 从数据库加载的动态任务配置
        :param api_client: 动态实例化的 FormApi 客户端
        :param table_param: 从 ?table= 获取的动态表名（可选）
        """
        op = payload.get('op')
        data = payload.get('data')
        # 提取表单名称，用于第三优先级
        form_name = data.get('name') or data.get('formName')

        if not data:
            logger.warning("No data in webhook payload, ignored.")
            return

        # --- 3. 获取动态引擎和元数据 ---
        try:
            if not task_config.database:
                task_config = config_session.query(SyncTask).options(joinedload(SyncTask.database)).get(task_config.id)
                if not task_config.database:
                    raise ValueError(f"task_id:[{task_config.id}] Missing associated database configuration!")

            dynamic_engine = get_dynamic_engine(task_config)
            dynamic_metadata = get_dynamic_metadata(dynamic_engine)
            engine_url_key = str(dynamic_engine.url)

            # 初始化此引擎的表缓存
            if engine_url_key not in self.inspected_tables_cache:
                self.inspected_tables_cache[engine_url_key] = {}

        except Exception as e:
            log_sync_error(
                task_config=task_config,
                error=e,
                payload=payload,
                extra_info="无法初始化动态数据库引擎"
            )
            logger.error(
                f"task_id:[{task_config.id}] Error: Failed to initialize dynamic database engine for task {task_config.id}: {e}")
            return

        app_id = task_config.app_id
        entry_id = task_config.entry_id

        # --- 1. 确定目标表名 ---
        new_table_name = None
        update_task_config_flag = False
        old_table_name = task_config.table_name

        # 优先级 1: table_param (URL参数)
        if table_param and table_param.strip():
            new_table_name = table_param.strip()
            logger.info(
                f"task_id:[{task_config.id}] Using dynamic table name from ?table= parameter: {new_table_name}")
            # 检查是否需要更新配置库中的表名
            if old_table_name != new_table_name:
                logger.info(
                    f"task_id:[{task_config.id}] Task {task_config.id} table_name will be updated from '{old_table_name}' to: {new_table_name}")
                task_config.table_name = new_table_name
                update_task_config_flag = True

        # 优先级 2: task_config.table_name (数据库配置)
        elif old_table_name and old_table_name.strip():
            new_table_name = old_table_name.strip()
            logger.info(f"task_id:[{task_config.id}] Using table name from dynamic task config: {new_table_name}")

        # 优先级 3: (表单名转换)
        else:
            if not form_name:
                log_sync_error(
                    task_config=task_config,
                    error=Exception("无法确定表名"),
                    payload=payload,
                    extra_info="table_param 和 task_config.table_name 均为空，且无法从 payload 中提取 form_name。"
                )
                logger.error(f"task_id:[{task_config.id}] Error: Cannot determine table name for {app_id}/{entry_id}.")
                return

            # 使用表单转拼音
            new_table_name = convert_to_pinyin(form_name)
            logger.info(f"task_id:[{task_config.id}] Using table name from form name pinyin: {new_table_name}")

            # 如果配置中没有表名，则将新生成的拼音表名存入
            if old_table_name != new_table_name:
                logger.info(
                    f"task_id:[{task_config.id}] Task {task_config.id} table_name will be set to: {new_table_name}")
                task_config.table_name = new_table_name
                update_task_config_flag = True

        # --- 2. 提交表名更新 ---
        if update_task_config_flag:
            try:
                # 如果 table_name 发生变化 (例如自动生成或被 URL 参数覆盖),
                # 必须立即更新 task_config.webhook_url 字段以反映此更改。

                # 从 task_config (已加载) 中获取所需组件
                department_name = task_config.department.department_name
                database_id = task_config.database_id
                task_id = task_config.id
                # 'table_name' 变量此时已是新名称

                # 从 Config 获取基础 URL
                host_url = Config.WEB_HOOK_BASE_URL
                if not host_url:
                    # 如果未配置，记录一个严重错误，但继续执行 (URL 在数据库中将是旧的)
                    logger.error(
                        f"task_id:[{task_id}] CRITICAL: Config.WEB_HOOK_BASE_URL is not set. Cannot update webhook URL.")
                    log_sync_error(task_config=task_config, error=Exception("WEB_HOOK_BASE_URL 未设置"))

                else:
                    if host_url.endswith('/'):
                        host_url = host_url.rstrip('/')
                    # 使用新 table_name 重新生成 query_params 和 URL
                    query_params = f"dpt={quote(department_name)}&db_id={database_id}&task_id={task_id}&table={quote(new_table_name)}"
                    task_config.webhook_url = f"{host_url}/api/jdy/webhook?{query_params}"
                    logger.info(
                        f"task_id:[{task_id}] Task {task_id} webhook_url auto-updated to: {task_config.webhook_url}")

                # 检查老表是否存在 (重命名逻辑)
                if old_table_name and old_table_name != new_table_name:
                    logger.info(
                        f"task_id:[{task_config.id}] Table name changed. Checking if old table '{old_table_name}' exists...")
                    # 检查老表是否存在
                    inspector = inspect(dynamic_engine)

                    # 检查是否老表已存在
                    exist_old_table = inspector.has_table(old_table_name)
                    # 检查新表是否存在
                    exist_new_table = inspector.has_table(new_table_name)

                    if exist_old_table:
                        if not exist_new_table:
                            logger.info(
                                f"task_id:[{task_config.id}] Old table '{old_table_name}' exists. Renaming to '{new_table_name}'...")
                            with dynamic_engine.connect() as connection:
                                # 确保新表名不存在，否则 RENAME 会失败
                                if inspector.has_table(new_table_name):
                                    logger.warning(
                                        f"task_id:[{task_config.id}] Cannot rename: Target table '{new_table_name}' already exists. Skipping rename.")
                                else:
                                    connection.execute(
                                        text(f"RENAME TABLE `{old_table_name}` TO `{new_table_name}`"))
                                    logger.info(f"task_id:[{task_config.id}] Table renamed successfully.")
                                    # 更新缓存
                                    engine_url_key = str(dynamic_engine.url)
                                    if engine_url_key in self.inspected_tables_cache:
                                        # 弹出旧表名，（新表名将在 get_table_if_exists 时加载）
                                        self.inspected_tables_cache[engine_url_key].pop(old_table_name, None)
                                        logger.info(
                                            f"task_id:[{task_config.id}] Inspected tables cache updated (removed old table).")

                        else:
                            logger.warning(
                                f"task_id:[{task_config.id}] Old table '{old_table_name}' exists and new table '{new_table_name}' exists. Skipping rename.")
                            log_sync_error(
                                task_config=task_config,
                                error=Exception(
                                    f"task_id:[{task_config.id}] Old table '{old_table_name}' exists and new table '{new_table_name}' exists. Skipping rename."),
                                payload=payload,
                                extra_info="table already exists, do not recreate table!"
                            )

                    else:
                        logger.info(
                            f"task_id:[{task_config.id}] Old table '{old_table_name}' does not exist. No rename necessary.")

                config_session.commit()
                logger.info(
                    f"task_id:[{task_config.id}] Task {task_config.id} table_name and webhook_url updated successfully.")
            except SQLAlchemyError as e:  # 使用更具体的异常
                config_session.rollback()
                log_sync_error(
                    task_config=task_config,
                    error=e,
                    extra_info=f"更新 table_name 失败: {new_table_name}"
                )
                logger.error(
                    f"task_id:[{task_config.id}] Error: Failed to update table_name for task {task_config.id}: {e}")
                # 即使更新失败，也继续处理当前 webhook，使用已确定的 table_name

        # --- 4. 自动保存 App ID 和 Entry ID ---
        if not app_id or not entry_id:
            get_app_id, get_entry_id = None, None
            if isinstance(data, dict):
                # data 中是 appId 和 entryId
                get_app_id = data.get('appId')
                get_entry_id = data.get('entryId')

            if not all([get_app_id, get_entry_id]):
                # payload 中是 app_id 和 entry_id
                get_app_id = payload.get('app_id')
                get_entry_id = payload.get('entry_id')

            if all([get_app_id, get_entry_id]):
                try:
                    logger.info(
                        f"task_id:[{task_config.id}] Task {task_config.id} missing App/Entry ID, auto-filling from Webhook...")
                    task_config.app_id = get_app_id
                    task_config.entry_id = get_entry_id
                    config_session.commit()
                    app_id = get_app_id
                    entry_id = get_entry_id
                    logger.info(
                        f"task_id:[{task_config.id}] Task {task_config.id} updated with App ID={app_id}, Entry ID={entry_id}")
                except SQLAlchemyError as e:
                    config_session.rollback()
                    log_sync_error(task_config=task_config, error=e,
                                   extra_info="自动填充 App/Entry ID 失败 (可能唯一约束冲突)")
                    logger.error(f"task_id:[{task_config.id}] Task {task_config.id} auto-fill App/Entry ID failed: {e}")
                    # 如果填充失败 ，无法继续，因为映射依赖 App/Entry ID
                    return
            else:
                logger.error(
                    f"task_id:[{task_config.id}] Task {task_config.id} missing App/Entry ID and cannot be extracted from Webhook payload.")
                return  # 无法继续

        # --- 检查是否需要启动后台全量同步 ---
        if op in ('data_create', 'data_update', 'data_remove', 'data_recover', 'form_update'):
            table = self.get_table_if_exists(new_table_name, dynamic_engine)

            if table is None or task_config.is_full_replace_first:
                logger.info(
                    f"task_id:[{task_config.id}] Task {task_config.id}: Triggering initial full sync (is_full_replace_first={task_config.is_full_replace_first}, table_exists={table is not None})")

                # 1. (同步) 确保表结构存在
                if table is None:
                    try:
                        logger.info(
                            f"task_id:[{task_config.id}] Table {new_table_name} does not exist, creating from API Schema...")
                        # 调用 API 获取真实 schema，而不是传递 data 负载
                        form_widgets = api_client.get_form_widgets(app_id, entry_id)
                        # 组装为特定的 form_schema
                        form_schema = {'name': form_name, 'widgets': form_widgets.get('widgets', [])}
                        table = self.get_or_create_table_from_schema(new_table_name, form_schema, task_config,
                                                                     dynamic_engine, dynamic_metadata)
                        logger.info(f"task_id:[{task_config.id}] Table {new_table_name} created successfully.")
                    except Exception as schema_err:
                        logger.error(
                            f"task_id:[{task_config.id}] Task {task_config.id}: Failed to create table structure synchronously: {schema_err}",
                            exc_info=True)
                        log_sync_error(task_config, schema_err, payload, "首次同步-创建表结构失败")
                        return  # 创建表失败，无法继续

                # 2. (异步) 启动后台全量同步
                logger.info(
                    f"task_id:[{task_config.id}] [Task {task_config.id}]: Starting background full data sync...")
                thread = threading.Thread(
                    target=self._run_initial_full_sync,
                    args=(task_config.id,)  # 仅传递 task_id
                )
                thread.daemon = True  # 设置为守护线程
                thread.start()

                # 3. (同步) 立即返回
                # 全量同步已启动，将包含当前数据，故跳过当前 webhook 的单独处理。
                logger.info(
                    f"task_id:[{task_config.id}] Task {task_config.id}: Initial full sync background task started, skipping current webhook (op={op}) processing.")
                return

        # --- 3. 根据操作类型处理 ---
        try:
            # 3.1 form_update 单独处理
            if op == 'form_update':
                logger.info(
                    f"task_id:[{task_config.id}] Processing form_update event for {new_table_name} ({app_id}/{entry_id})")

                # 1. 获取或创建表
                table = self.get_table_if_exists(new_table_name, dynamic_engine)
                if table is None:
                    table = self.get_or_create_table_from_schema(new_table_name, data, task_config, dynamic_engine,
                                                                 dynamic_metadata)

                # 2. 先同步 DDL
                # 比较 DB 映射 (旧) 和 data (新))
                table = self.update_table_schema_from_form(config_session, table, data, task_config, dynamic_engine,
                                                           dynamic_metadata)

                # 3. DDL 成功后，再更新映射表
                self.mapping_service.create_or_update_form_fields_mapping(config_session, task_config, api_client,
                                                                          form_name)

            # 3.2 其他数据操作
            elif op in ('data_create', 'data_update', 'data_recover', 'data_remove'):
                logger.info(
                    f"task_id:[{task_config.id}] Processing {op} event for {new_table_name} ({app_id}/{entry_id})")
                # 确保映射存在 (如果不存在则创建)
                exists = config_session.query(FormFieldMapping).filter_by(
                    task_id=task_config.id
                ).first()
                if not exists:
                    logger.info(
                        f"task_id:[{task_config.id}] Mapping does not exist, creating for {app_id}/{entry_id} (Task {task_config.id})...")
                    self.mapping_service.create_or_update_form_fields_mapping(config_session, task_config,
                                                                              api_client, form_name)

                # 获取或创建表
                table = self.get_table_if_exists(new_table_name, dynamic_engine)
                if table is None and op != 'data_remove':  # 只有在需要写入数据时才创建表
                    logger.info(
                        f"task_id:[{task_config.id}] Table {new_table_name} does not exist, creating from data...")
                    table = self.get_or_create_table_from_data(new_table_name, [data], task_config, dynamic_engine,
                                                               dynamic_metadata)

                # 如果是 data_remove 且表不存在，则无需操作
                if table is None and op == 'data_remove':
                    logger.warning(
                        f"task_id:[{task_config.id}] Received data_remove operation, but table '{new_table_name}' does not exist. Skipping...")
                    return

                # --- 动态会话 DML ---
                # 使用动态会话执行数据库写入/删除
                with get_dynamic_session(task_config) as target_session:
                    try:
                        if op in ('data_create', 'data_update', 'data_recover'):
                            # 确保表结构与数据兼容 (主要处理新增列)
                            table = self.update_table_schema_from_data(table, [data], task_config, dynamic_engine,
                                                                       dynamic_metadata, config_session)
                            self.upsert_data(target_session, table, data, task_config)

                        elif op == 'data_remove':
                            self.delete_data(target_session, table, data, task_config)

                        target_session.commit()  # 提交 DML

                    except SQLAlchemyError as db_err:  # 捕获 DML 错误
                        logger.error(
                            f"task_id:[{task_config.id}] Database error during DML processing (op={op}): {db_err}",
                            exc_info=True)
                        try:
                            target_session.rollback()
                        except Exception as rb_err:
                            logger.error(f"task_id:[{task_config.id}] Error during target_session rollback: {rb_err}")
                        # 在会话回滚后记录错误
                        log_sync_error(task_config=task_config, error=db_err, payload=payload,
                                       extra_info=f"Database error during DML (op={op}) processing")
                    except Exception as e:  # 捕获 DML 期间的其他错误
                        logger.error(
                            f"task_id:[{task_config.id}] Unexpected error during DML processing (op={op}): {e}",
                            exc_info=True)
                        try:
                            target_session.rollback()
                        except Exception as rb_err:
                            logger.error(f"task_id:[{task_config.id}] Error during target_session rollback: {rb_err}")
                        log_sync_error(task_config=task_config, error=e, payload=payload,
                                       extra_info=f"Unexpected error during DML (op={op}) processing")

            else:
                logger.warning(f"task_id:[{task_config.id}] Received unhandled operation type: {op}")
                return  # 对于未处理的操作，直接返回

            # 仅当操作涉及 target_session 时才提交
            if op in ('data_create', 'data_update', 'data_recover', 'data_remove'):
                logger.info(
                    f"task_id:[{task_config.id}] Webhook data processed successfully: op={op}, table={new_table_name}")
            elif op == 'form_update':
                logger.info(
                    f"task_id:[{task_config.id}] Webhook form structure update processed successfully: table={new_table_name}")

        except SQLAlchemyError as db_err:  # 捕获 DDL 或 映射 相关的错误
            logger.error(f"task_id:[{task_config.id}] Database error during webhook processing (DDL/Mapping): {db_err}",
                         exc_info=True)
            # config_session 的回滚由 routes.py 处理
            log_sync_error(task_config=task_config, error=db_err, payload=payload,
                           extra_info="Database error during webhook DDL/Mapping processing")

        except Exception as e:  # 捕获其他所有意外错误
            logger.error(f"task_id:[{task_config.id}] Unexpected error during webhook processing: {e}", exc_info=True)
            # config_session 的回滚由 routes.py 处理
            log_sync_error(task_config=task_config, error=e, payload=payload,
                           extra_info="Unexpected error during webhook processing")

    # --- 后台全量同步方法 ---

    @retry()
    def _run_initial_full_sync(self, task_id: int):
        """
        在后台线程中执行首次全量同步。
        此方法必须是完全独立的，并创建自己的数据库会话。
        """
        logger.info(f"task_id:[{task_id}]: Background initial full sync thread started...")

        # 1. 创建此线程专用的 ConfigSession
        config_session = ConfigSession()
        task_config = None  # 必须从新会话中重新获取

        try:
            # 2. 重新获取任务配置 (必须 joinedload 依赖)
            task_config = config_session.query(SyncTask).options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                joinedload(SyncTask.database),
            ).get(task_id)

            if not task_config:
                logger.error(
                    f"task_id:[{task_id}]: Task not found in background thread, thread exiting.")
                return
            if not task_config.is_active:
                logger.error(f"task_id:[{task_id}]: Task is disabled, thread exiting.")
                return

            if task_config.sync_status == 'running':
                logger.error(f"task_id:[{task_id}]: Task is already running, thread exiting.")
                return

            if not (task_config.app_id, task_config.entry_id):
                logger.error(f"task_id:[{task_id}]: Missing app_id or entry_id, thread exiting.")
                return

            # 3. 实例化 DataApi
            if not task_config.department or not task_config.department.jdy_key_info:
                logger.error(
                    f"task_id:[{task_id}]: Task missing department or key info, thread exiting.")
                task_config.sync_status = 'error'
                config_session.commit()
                return

            api_key = task_config.department.jdy_key_info.api_key
            data_api_client = DataApi(
                api_key=api_key,
                host=Config.JDY_API_BASE_URL,
                qps=30
            )

            # 4. 执行全量同步 (self.sync_historical_data)
            # 设置状态为 running
            task_config.sync_status = 'running'
            config_session.commit()

            self.sync_historical_data(task_config, data_api_client, delete_first=False)

            # 5. [关键] 标记首次全量已完成
            # 再次查询最新的 task_config，以防在同步期间被修改
            try:
                task_to_update = config_session.query(SyncTask).get(task_id)
                if task_to_update:
                    task_to_update.is_full_replace_first = False
                    config_session.commit()
                    logger.info(
                        f"task_id:[{task_id}]: Initial full sync flag (is_full_replace_first=False) updated successfully.")
            except SQLAlchemyError as e:
                logger.error(
                    f"task_id:[{task_id}]: Error during commit of 'is_full_replace_first' flag: {e}",
                    exc_info=True)
                log_sync_error(task_config=task_config, error=e,
                               extra_info="Error during commit of 'is_full_replace_first' flag.")
            except Exception as e:
                logger.error(
                    f"task_id:[{task_id}]: Unexpected error during commit of 'is_full_replace_first' flag: {e}",
                    exc_info=True)
                log_sync_error(task_config=task_config, error=e,
                               extra_info="Unexpected error during commit of 'is_full_replace_first' flag.")


        except Exception as e:
            logger.error(f"task_id:[{task_id}]: Background full sync thread failed: {e}",
                         exc_info=True)
            if config_session:
                try:
                    config_session.rollback()
                except Exception:
                    pass
            # 记录错误 (task_config 可能是 None)
            log_sync_error(
                task_config=task_config,  # 传递可能已加载的 task_config
                error=e,
                extra_info=f"后台首次全量同步失败 (task_id:[{task_id}])"
            )
            # 确保在线程崩溃时状态被设为 error
            try:
                if task_config:  # (如果 task_config 已加载)
                    task_to_update = config_session.query(SyncTask).get(task_config.id)  # 重新获取
                    if task_to_update:
                        task_to_update.sync_status = 'error'
                        config_session.commit()
            except Exception as e_status:
                logger.error(
                    f"task_id:[{task_id}]: CRITICAL: Failed to set error status after thread crash: {e_status}")
                if config_session:
                    try:
                        config_session.rollback()
                    except Exception:
                        pass
        finally:
            if config_session:
                config_session.close()
            logger.info(f"task_id:[{task_id}]: Background thread exiting.")

    # --- 数据库表结构 (DDL) ---

    @retry()
    def get_table_if_exists(self, table_name: str, engine) -> Table | None:
        """
        如果表存在，则从缓存或数据库加载表定义。
        """
        engine_url_key = str(engine.url)

        if engine_url_key not in self.inspected_tables_cache:
            self.inspected_tables_cache[engine_url_key] = {}

        if table_name in self.inspected_tables_cache[engine_url_key]:
            return self.inspected_tables_cache[engine_url_key][table_name]

        try:
            inspector = inspect(engine)
            if inspector.has_table(table_name):
                metadata = get_dynamic_metadata(engine)
                table = Table(table_name, metadata, autoload_with=engine, extend_existing=True)
                self.inspected_tables_cache[engine_url_key][table_name] = table
                logger.info(f"Loading table definition from database ({engine_url_key}): {table_name}")
                return table
        except Exception as e:
            logger.error(f"Error checking or loading table definition for {table_name} ({engine_url_key}): {e}",
                         exc_info=True)
            self.inspected_tables_cache[engine_url_key].pop(table_name, None)
        return None

    @retry()
    def get_or_create_table_from_data(self, table_name: str, data_samples: list[dict], task_config: SyncTask, engine,
                                      metadata) -> Table:
        """
        获取或创建数据表。如果表不存在，则根据数据样本动态创建。
        """
        table = self.get_table_if_exists(table_name, engine)
        if table is not None:
            return table

        logger.info(f"task_id:[{task_config.id}] Table '{table_name}' does not exist, creating from data samples...")
        try:
            # 获取字段 comment
            column_defs, column_comments = self.get_column_schema_from_data(data_samples, task_config)
            columns = [Column(name, col_type, comment=column_comments.get(name)) for name, col_type in
                       column_defs.items()]

            # 获取表 comment
            if data_samples and data_samples[0]:  # 确保 data_samples 非空且第一个元素非空
                table_comment = data_samples[0].get('formName', table_name)
            else:
                table_comment = table_name

            # 确保 _id 列是主键或唯一键（如果创建新表）
            final_columns = []
            id_column_added = False
            for i, col_def in enumerate(columns):
                if col_def.name == '_id':
                    # 显式添加 unique=True
                    final_columns.append(
                        Column('_id', column_defs['_id'], unique=True, comment=column_comments.get('_id')))
                    id_column_added = True
                else:
                    final_columns.append(col_def)

            if not id_column_added:
                final_columns.insert(0, Column('_id', String(50), unique=True, comment='唯一索引id'))
                logger.warning(
                    f"task_id:[{task_config.id}] '_id' field not found in data samples for table '{table_name}', auto-adding as unique key.")

            # 确保列名不重复 (理论上 get_column_schema 应该处理了)
            final_column_names = {c.name for c in final_columns}
            if len(final_column_names) != len(final_columns):
                logger.error(
                    f"task_id:[{task_config.id}] Duplicate column names detected when creating table '{table_name}', check mapping logic.")
                # 可以选择抛出异常或尝试去重，这里选择记录错误并继续（可能导致建表失败）

            table = Table(table_name, metadata, *final_columns, comment=table_comment, mysql_charset='utf8mb4')
            metadata.create_all(engine)  # 这会创建表
            logger.info(f"task_id:[{task_config.id}] Table '{table_name}' created successfully.")

            engine_url_key = str(engine.url)
            self.inspected_tables_cache[engine_url_key][table_name] = table  # 加入缓存
            return table
        except Exception as e:
            logger.error(f"task_id:[{task_config.id}] Failed to create table '{table_name}' from data samples: {e}",
                         exc_info=True)
            log_sync_error(task_config=task_config, error=e,
                           extra_info=f"Failed to create table '{table_name}' from data samples")
            raise  # 创建失败则向上抛出异常

    @retry()
    def get_or_create_table_from_schema(self, table_name: str, form_schema_data: dict, task_config: SyncTask, engine,
                                        metadata) -> Table:
        """
        根据 form_update 事件中的 schema 数据获取或创建表。
        "op": "form_update",
        "data": {
            "appId": "6607ac2c0d1f1a7ae4807f44",
            "createTime": "2025-04-03T08:08:58.458Z",
            "entryId": "67ee421a6003aa139515e0cf",
            "name": "问题管理主表",
            "widgets": [
                {
                    "label": "问题ID",
                    "name": "question_id",
                    "type": "text",
                    "widgetName": "_widget_1742961087355"
                }]

        表单接口返回：
        "widgets": [
        {
            "name": "_widget_1529400746031",
            "widgetName": "_widget_1529400746031",
            "label": "单行文本",
            "type": "text"
        """
        table = self.get_table_if_exists(table_name, engine)
        if table is not None:
            return table

        logger.info(
            f"task_id:[{task_config.id}] Table '{table_name}' does not exist, creating from form structure (form_update)...")
        try:
            widgets = form_schema_data.get('widgets', [])
            columns = [Column('_id', String(50), unique=True, comment='唯一索引id')]  # 主键/唯一键

            # 预先添加通用的系统字段
            system_fields = {
                'appId': (String(50), 'appId'),
                'entryId': (String(50), 'entryId'),
                'creator': (JSON, '提交人'),
                'updater': (JSON, '修改人'),
                'deleter': (JSON, '删除人'),
                'createTime': (DateTime, '提交时间'),
                'updateTime': (DateTime, '更新时间'),
                'deleteTime': (DateTime, '删除时间'),
                'formName': (Text, '简道云表单名称'),
                'flowState': (BigInteger, '简道云流程状态'),
            }
            for name, (col_type, comment) in system_fields.items():
                columns.append(Column(name, col_type, comment=comment))

            # 根据 widget 生成列
            added_col_names = {'_id'} | set(system_fields.keys())  # 跟踪已添加的列名，防止重复
            for widget in widgets:
                col_name = self.mapping_service.get_column_name(widget, use_label_pinyin=task_config.label_to_pinyin)
                jdy_type = widget.get('type')
                jdy_label = widget.get('label')
                if col_name and col_name not in added_col_names:  # 确保列名有效且未重复添加
                    sql_type = self.mapping_service.get_sql_type(jdy_type, None)  # 从 schema 创建时不依赖样本值
                    columns.append(Column(col_name, sql_type, comment=jdy_label))
                    added_col_names.add(col_name)
                elif col_name in added_col_names:
                    logger.warning(
                        f"task_id:[{task_config.id}] Attempted to add duplicate column '{col_name}' to table '{table_name}' (likely from widget '{widget.get('widgetName')}'), skipped.")

            table_comment = form_schema_data.get('name', table_name)
            table = Table(table_name, metadata, *columns, comment=table_comment, mysql_charset='utf8mb4')
            metadata.create_all(engine)
            logger.info(f"task_id:[{task_config.id}] Table '{table_name}' created successfully.")

            engine_url_key = str(engine.url)
            self.inspected_tables_cache[engine_url_key][table_name] = table
            return table
        except Exception as e:
            logger.error(f"task_id:[{task_config.id}] Failed to create table '{table_name}' from schema: {e}",
                         exc_info=True)
            log_sync_error(task_config=task_config, error=e,
                           extra_info=f"Failed to create table '{table_name}' from schema")
            raise

    @retry()
    def get_column_schema_from_data(self, data_samples: list[dict], task_config: SyncTask) -> (dict, dict):
        """根据一批数据样本分析并生成所有字段的列定义"""
        column_types = {}
        column_comments = {}

        config_session = ConfigSession()
        try:

            mappings = {
                m.widget_name: m
                for m in config_session.query(FormFieldMapping).filter_by(
                    task_id=task_config.id
                ).all()
            }
        except SQLAlchemyError as e:
            logger.error(f"task_id:[{task_config.id}] Failed to query field mappings for Task {task_config.id}: {e}",
                         exc_info=True)
            mappings = {}
        finally:
            config_session.close()

        all_keys = set()
        for data in data_samples:
            if isinstance(data, dict):  # 确保 data 是字典
                all_keys.update(data.keys())

        processed_col_names = set()  # 跟踪处理过的最终列名，防止重复定义

        for key in all_keys:
            db_col_name = None
            comment = key
            sql_type_instance = Text()  # 默认类型实例
            jdy_type = None  # 初始化简道云类型

            sample_value = next(
                (d[key] for d in data_samples if isinstance(d, dict) and key in d and d[key] is not None),
                None)

            if key == '_id':
                db_col_name = '_id'
                comment = '唯一索引id'
                sql_type_instance = String(50)
            else:
                mapping_info = mappings.get(key)
                if mapping_info:
                    # 从 mapping_info 推断列名
                    widget = {'name': mapping_info.widget_alias, 'label': mapping_info.label,
                              'widgetName': mapping_info.widget_name}
                    db_col_name = self.mapping_service.get_column_name(widget, task_config.label_to_pinyin)
                    comment = mapping_info.label
                    jdy_type = mapping_info.type  # 获取简道云类型
                    sql_type_instance = self.mapping_service.get_sql_type(jdy_type, sample_value)  # 直接获取实例

                else:
                    # 如果映射不存在
                    # 如果使用 EXTRACT_SCHEMA_FROM_DATA
                    if EXTRACT_SCHEMA_FROM_DATA:
                        # 退化为旧逻辑
                        # # 替换所有非字母、数字、下划线的字符为空字符串
                        # db_col_name = re.sub(r'[^a-zA-Z0-9_]', '', key)
                        # 保留中文、英文、数字、下划线
                        db_col_name = re.sub(r'[^a-zA-Z0-9_\u4e00-\u9fff]', '', key)
                        sql_type_instance = self.mapping_service.get_sql_type(None, sample_value)
                        logger.warning(
                            f"task_id:[{task_config.id}] Field mapping not found for {task_config.app_id}/{task_config.entry_id}: {key}, extract column name = {db_col_name}.")

                    else:
                        logger.error(
                            f"task_id:[{task_config.id}] Field mapping not found for {task_config.app_id}/{task_config.entry_id}: {key}")
                        log_sync_error(task_config=task_config,
                                       error=Exception("Field mapping not found"),
                                       extra_info=f"字段映射不存在 for {task_config.app_id}/{task_config.entry_id}: {key}")

            if not db_col_name or db_col_name in processed_col_names:
                if db_col_name in processed_col_names:
                    logger.warning(
                        f"task_id:[{task_config.id}] Detected duplicate target column name '{db_col_name}' when inferring column definitions (possibly from key '{key}'), skipped.")
                continue  # 跳过无效或重复的列名

            # 类型兼容性/提升处理 (基于实例比较)
            if db_col_name in column_types:
                current_type_inst = column_types[db_col_name]

                # 使用 isinstance 检查类型关系，优先使用更通用的类型
                if isinstance(sql_type_instance, (LONGTEXT, Text)):
                    column_types[db_col_name] = sql_type_instance
                elif isinstance(sql_type_instance, JSON) and not isinstance(current_type_inst, (LONGTEXT, Text)):
                    column_types[db_col_name] = sql_type_instance
                elif isinstance(sql_type_instance, DateTime) and not isinstance(current_type_inst,
                                                                                (LONGTEXT, Text, JSON)):
                    column_types[db_col_name] = sql_type_instance
                elif isinstance(sql_type_instance, Float) and isinstance(current_type_inst, BigInteger):
                    column_types[db_col_name] = sql_type_instance
                elif isinstance(sql_type_instance, BigInteger) and isinstance(current_type_inst, Integer):
                    column_types[db_col_name] = sql_type_instance  # 提升为 BigInteger
                # 其他情况，保持现有类型不变 (例如，已有 Text 不会被 String 覆盖)

            else:
                column_types[db_col_name] = sql_type_instance

            column_comments[db_col_name] = comment
            processed_col_names.add(db_col_name)

        return column_types, column_comments

    # @retry()
    # def get_column_schema_from_widget(self, widget: dict, data: dict, use_label_pinyin: bool) -> str:
    #     column_name = self.mapping_service.get_column_name(widget, use_label_pinyin)
    #     data_value = data.values()[0]
    #     column_type = self.mapping_service.get_sql_type(widget.get('type'), data_value)
    #     column_comment = widget.get('label')
    #
    #     return column_name, column_type, column_comment

    @retry()
    def update_table_schema_from_data(self, table: Table, data_batch: list[dict], task_config: SyncTask, engine,
                                      metadata, config_session: Session) -> Table | None:
        """
        检查数据负载，如果发现 data 与 FormFieldMapping 不一致，则触发完整的 DDL 同步。
        """
        task_id = task_config.id

        try:
            # 1. 获取当前映射中的所有 widget_alias
            mapping_keys = {
                m.widget_alias for m in
                config_session.query(FormFieldMapping.widget_alias).filter_by(task_id=task_id).all()
            }

            # 2. 获取数据负载中的所有 keys
            data_keys = set()
            for data_item in data_batch:
                if isinstance(data_item, dict):
                    data_keys.update(data_item.keys())

            # 3. 排除已知的系统字段
            system_fields = {
                '_id', 'appId', 'entryId', 'creator', 'updater', 'deleter',
                'createTime', 'updateTime', 'deleteTime', 'formName',  # 'flowState', 不是系统默认字段
                # 'wx_open_id', 'wx_nickname', 'wx_gender' # 微信字段
            }

            # 4. 找到数据中有但映射中没有的 keys
            new_keys_found = data_keys - (mapping_keys.union(system_fields))
            # 4. 找到映射中有但数据中没有的 keys
            deleted_keys_found = (mapping_keys.union(system_fields)) - data_keys

            # 5. 如果发现 keys 变化，则触发完整的 DDL 和映射更新
            if new_keys_found or deleted_keys_found:
                if new_keys_found:
                    logger.warning(
                        f"task_id:[{task_id}] Stale mapping detected (data event). Data contains new keys: {new_keys_found}. Forcing full schema sync...")
                if deleted_keys_found:
                    logger.warning(
                        f"task_id:[{task_id}] Stale mapping detected (data event). Data contains deleted keys: {deleted_keys_found}. Forcing full schema sync...")

                # 5a. 调用 API 获取最新的表单结构
                if not task_config.department or not task_config.department.jdy_key_info:
                    raise ValueError(f"task_id:[{task_id}] Missing department/key info for stale mapping update")
                key_info = task_config.department.jdy_key_info

                api_client = FormApi(
                    api_key=key_info.api_key,
                    host=Config.JDY_API_BASE_URL
                )
                resp = api_client.get_form_widgets(task_config.app_id, task_config.entry_id)
                form_schema_data = {
                    'name': resp.get('name') or resp.get('formName', ''),
                    'widgets': resp.get('widgets', []),
                    'appId': task_config.app_id,
                    'entryId': task_config.entry_id
                }

                # 5b. 调用 update_table_schema_from_form 执行完整的 DDL (ADD/DROP/RENAME)
                # (传入 config_session，因为它包含旧的映射)
                new_table = self.update_table_schema_from_form(
                    config_session, table, form_schema_data, task_config, engine, metadata
                )

                # 5c. DDL 成功后，立即更新 FormFieldMapping 表
                self.mapping_service.create_or_update_form_fields_mapping(
                    config_session, task_config, api_client, form_schema_data.get('name')
                )

                logger.info(f"task_id:[{task_id}] Full schema sync triggered by data event completed.")
                return new_table  # 返回新加载的表

            else:
                # 6. ADD COLUMN FROM DATA
                if EXTRACT_SCHEMA_FROM_DATA:
                    # logger.debug(f"task_id:[{task_id}] Mapping appears fresh. Checking for missing and deleted columns.")

                    # 加载表 schema
                    inspector = inspect(engine)
                    try:
                        # get_columns 应该传入 table_name, 不是 table.name
                        existing_columns_map = {c['name']: c for c in
                                                inspector.get_columns(table.name, _warn_deprecated_dialect=True)}
                        existing_columns = set(existing_columns_map.keys())
                    except Exception as e:
                        logger.error(
                            f"task_id:[{task_id}] Cannot get column information for table '{table.name}': {e}",
                            exc_info=True)
                        return table  # 无法获取结构信息，直接返回

                    # 重新加载映射
                    mappings = {
                        m.widget_name: m
                        for m in config_session.query(FormFieldMapping).filter_by(
                            task_id=task_config.id
                        ).all()
                    }

                    # 需要增加的列
                    cols_to_add = {}  # {col_name: {"value": ..., "type": ..., "comment": ...}}
                    # 需要删除的列
                    cols_to_drop = {}

                    # 1. 计算需要新增的列
                    if data_batch:
                        for data_item in data_batch:
                            if not isinstance(data_item, dict):
                                continue  # 跳过无效数据项

                            for key, value in data_item.items():
                                mapping_info = mappings.get(key)
                                # 没找到映射
                                if not mapping_info:
                                    # 检查系统字段
                                    if key in system_fields:
                                        if key not in existing_columns:
                                            sql_type = self.mapping_service.get_sql_type(mapping_info.type, value)
                                            cols_to_add[key] = {
                                                "value": value,
                                                "type": sql_type,
                                                "comment": key  # 使用 简道云 widget_alias/widget_name 作为 comment
                                            }
                                        # 处理下一个
                                        continue

                                    # 如果映射不存在，并且不是系统字段，则使用旧逻辑
                                    if EXTRACT_SCHEMA_FROM_DATA:
                                        # # 替换所有非字母、数字、下划线的字符为空字符串
                                        # db_col_name = re.sub(r'[^a-zA-Z0-9_]', '', key)
                                        # 保留中文、英文、数字、下划线
                                        db_col_name = re.sub(r'[^a-zA-Z0-9_\u4e00-\u9fff]', '', key)
                                        if db_col_name and db_col_name not in existing_columns and db_col_name not in cols_to_add:
                                            sql_type = self.mapping_service.get_sql_type(None, value)
                                            cols_to_add[db_col_name] = {
                                                "value": value,
                                                "type": sql_type,
                                                "comment": key  # 使用 简道云 widget_alias/widget_name 作为 comment
                                            }
                                            logger.warning(
                                                f"task_id:[{task_id}] Field mapping not found for {key}, auto-adding column '{db_col_name}' based on data.")

                                    else:
                                        logger.warning(
                                            f"task_id:[{task_id}] Field mapping not found for {key}, skipping.")
                                        log_sync_error(task_config=task_config,
                                                       error=Exception("Field mapping not found"),
                                                       extra_info="Task skipped: Field mapping not found.")
                                    # 处理下一个
                                    continue

                                # 找到映射
                                else:
                                    widget_dict = {'widgetName': mapping_info.widget_name,
                                                   'name': mapping_info.widget_alias,
                                                   'label': mapping_info.label}
                                    db_col_name = self.mapping_service.get_column_name(widget_dict,
                                                                                       task_config.label_to_pinyin)

                                    if db_col_name and db_col_name not in existing_columns and db_col_name not in cols_to_add:
                                        cols_to_add[db_col_name] = {
                                            "value": value,
                                            "type": mapping_info.type,
                                            "comment": mapping_info.label
                                        }
                                    # 处理下一个
                                    continue

                    # 2. 计算需要删除的列
                    expected_db_columns = set()
                    for m in mappings.values():
                        # 动态获取列名
                        widget_dict = {'name': m.widget_alias, 'label': m.label, 'widgetName': m.widget_name}
                        db_col_name = self.mapping_service.get_column_name(widget_dict, task_config.label_to_pinyin)
                        expected_db_columns.add(db_col_name)
                    # 数据库中存在，但期望的列定义中没有的列
                    cols_to_drop = existing_columns - expected_db_columns - system_fields  # 排除系统字段
                    # 为防止映射表更新延迟，如果一个列刚被识别为新列，就不应该删除它
                    cols_to_drop = cols_to_drop - set(cols_to_add.keys())

                    if cols_to_add or cols_to_drop:
                        if cols_to_add:
                            # 调用 DDL
                            logger.info(
                                f"task_id:[{task_id}] Detected new fields in table '{table.name}', batch adding: {', '.join(cols_to_add.keys())}")
                        if cols_to_drop:
                            # 调用 DDL
                            logger.info(
                                f"task_id:[{task_id}] Detected deleted fields in table '{table.name}', batch deleting: {', '.join(cols_to_drop)}")
                        try:
                            with engine.connect() as connection:
                                with connection.begin() as transaction:

                                    for col_name, col_info in cols_to_add.items():
                                        if not col_name:
                                            continue
                                        sql_type_instance = self.mapping_service.get_sql_type(col_info["type"],
                                                                                              col_info['value'])
                                        type_string = sql_type_instance.compile(dialect=engine.dialect)
                                        try:
                                            logger.info(
                                                f"task_id:[{task_id}] Will add column '{col_name}' with type '{type_string}'")
                                            connection.execute(
                                                text(
                                                    f"ALTER TABLE `{table.name}` ADD COLUMN `{col_name}` {type_string} COMMENT :comment"),
                                                {'comment': col_info['comment']}
                                            )
                                        except Exception as e:
                                            logger.error(
                                                f"task_id:[{task_id}] Failed to add column '{col_name}': {e}")
                                            log_sync_error(task_config=task_config,
                                                           error=e,
                                                           extra_info="Error during update_table_schema_from_data (connection/transaction)")

                                    for col_name in cols_to_drop:
                                        if col_name == '_id':
                                            continue  # 保护 _id 不被删除
                                        try:
                                            logger.info(
                                                f"task_id:[{task_id}] Will delete column '{col_name}'")
                                            connection.execute(
                                                text(f"ALTER TABLE `{table.name}` DROP COLUMN `{col_name}`"))
                                        except Exception as e:
                                            logger.error(
                                                f"task_id:[{task_id}] Failed to delete column '{col_name}': {e}")
                                            log_sync_error(task_config=task_config, error=e,
                                                           extra_info="Error during update_table_schema_from_data (connection/transaction)")
                                    transaction.commit()

                            # 刷新表缓存
                            metadata.clear()
                            engine_url_key = str(engine.url)
                            if engine_url_key in self.inspected_tables_cache:
                                self.inspected_tables_cache[engine_url_key].pop(table.name, None)

                            new_table = self.get_table_if_exists(table.name, engine)  # 返回新表
                            if new_table is not None:
                                logger.info(
                                    f"task_id:[{task_id}] Table '{table.name}' structure updated and reloaded.")
                                return new_table
                            else:
                                logger.error(
                                    f"task_id:[{task_id}] Failed to reload table definition for '{table.name}'")
                                log_sync_error(task_config=task_config,
                                               error=Exception(f"Failed to reload table {table.name}"),
                                               extra_info=f"Failed to reload table definition for '{table.name}'")
                                return table  # 返回旧表

                        except Exception as e:
                            logger.error(f"task_id:[{task_id}] Connection error: {e}", exc_info=True)
                            log_sync_error(task_config=task_config, error=e, extra_info="Error during DDL")

                return table  # 返回原表


        except Exception as e:
            logger.error(f"task_id:[{task_id}] Error during update_table_schema_from_data: {e}", exc_info=True)
            log_sync_error(task_config=task_config, error=e, extra_info="Error in update_table_schema_from_data")
            return table  # 失败时返回原表

    # 优化：比较 SQLAlchemy 类型实例
    def _is_type_different(self, existing_sqlalch_type: TypeEngine, expected_sqlalch_type: TypeEngine) -> bool:
        """比较两个 SQLAlchemy 类型实例是否代表不同的数据库类型"""
        if type(existing_sqlalch_type) != type(expected_sqlalch_type):
            # # 允许从 String/Text 升级到 LONGTEXT
            # if isinstance(existing_sqlalch_type, (String, Text)) and isinstance(expected_sqlalch_type, (LONGTEXT)):
            #     return True
            # # 允许从 Integer 升级到 BigInteger
            # if isinstance(existing_sqlalch_type, Integer) and isinstance(expected_sqlalch_type, BigInteger):
            #     return True
            # # 允许从 Integer/BigInteger 升级到 Float
            # if isinstance(existing_sqlalch_type, (Integer, BigInteger)) and isinstance(expected_sqlalch_type, Float):
            #     return True
            # 其他类型不匹配
            return True

        # 对于 String 类型，还需要比较长度
        if isinstance(existing_sqlalch_type, String) and isinstance(expected_sqlalch_type, String):
            # 仅在期望长度大于现有长度时才认为不同 (允许扩展)
            if (existing_sqlalch_type.length is not None and
                    expected_sqlalch_type.length is not None and
                    expected_sqlalch_type.length > existing_sqlalch_type.length):
                return True
            # 允许从 String 升级到 Text/LONGTEXT (已在 type check 中处理)
            return False

        # 可以为其他需要比较属性的类型添加更多逻辑 (如 DECIMAL 的精度)
        return False

    def update_table_schema_from_form(self, config_session: Session, table: Table, data: dict, task_config: SyncTask,
                                      engine, metadata):
        """
        处理表单结构更新 (form_update)，比较 DB 映射 (旧) 和 Webhook (新)。
        此函数必须在 FormFieldMapping 更新 *之前* 调用。
        """
        table_name = table.name
        app_id = data.get('appId')
        entry_id = data.get('entryId')
        new_widgets_payload = data.get('widgets', [])
        form_name = data.get('name') or data.get('formName')
        task_id = task_config.id

        if not all([app_id, entry_id]):  # widgets 可以为空
            logger.warning(
                f"task_id:[{task_id}] form_update event missing app_id or entry_id, skipping structure processing.")
            return table

        logger.info(f"task_id:[{task_id}] Starting to sync table structure for '{table_name}' based on form_update...")
        try:
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                # 如果表不存在，直接基于 schema 创建，无需同步
                logger.info(
                    f"task_id:[{task_id}] Table '{table_name}' does not exist, will create directly from schema.")
                # (创建表后，调用者 handle_webhook_data 会继续更新映射表)
                return self.get_or_create_table_from_schema(table_name, data, task_config, engine, metadata)

            # 1. 获取数据库当前列信息 (包括类型)
            existing_columns_info = {c['name']: c for c in inspector.get_columns(table_name)}
            existing_column_names = set(existing_columns_info.keys())

            # 定义系统字段 (这些字段不参与 DDL 变更)
            system_fields = {
                '_id', 'appId', 'entryId', 'creator', 'updater', 'deleter',
                'createTime', 'updateTime', 'deleteTime', 'formName',  # 'flowState', 不是系统默认字段
                # 'wx_open_id', 'wx_nickname', 'wx_gender' # 微信字段
            }
            # 2. 获取旧状态 (Old State)
            # 从 FormFieldMapping 表 (config_session)
            old_mappings = {
                m.widget_name: m
                for m in config_session.query(FormFieldMapping).filter_by(task_id=task_id).all()
            }
            old_widget_names = set(old_mappings.keys())

            # 3. 获取新状态 (New State)
            # 从 Webhook 'data' 负载
            new_mappings = {w.get('widgetName'): w for w in new_widgets_payload if w.get('widgetName')}
            new_widget_names = set(new_mappings.keys())

            # 4. 计算变更集 (基于 widgetName)
            widgets_to_add = new_widget_names - old_widget_names
            widgets_to_drop = old_widget_names - new_widget_names
            widgets_to_check = old_widget_names.intersection(new_widget_names)  # 检查更新

            cols_to_add = []  # (db_col_name, sql_type, comment)
            cols_to_drop = []  # (db_col_name)
            cols_to_rename = []  # (old_db_col_name, new_db_col_name, new_sql_type, new_comment)
            cols_to_modify = []  # (db_col_name, new_sql_type, new_comment)

            # 5. 处理新增
            for widget_name in widgets_to_add:
                widget = new_mappings[widget_name]
                db_col_name = self.mapping_service.get_column_name(widget, task_config.label_to_pinyin)
                if not db_col_name:
                    continue

                sql_type = self.mapping_service.get_sql_type(widget.get('type'), None)
                comment = widget.get('label', '')

                # 检查此列是否已存在 (例如系统字段或冲突)
                if db_col_name not in existing_column_names:
                    cols_to_add.append((db_col_name, sql_type, comment))
                else:
                    logger.warning(
                        f"task_id:[{task_id}] New widget '{widget_name}' maps to existing DB column '{db_col_name}'. Skipping ADD.")

            # 6. 处理删除
            for widget_name in widgets_to_drop:
                old_mapping = old_mappings[widget_name]
                # 计算 OLD column name
                old_db_col_name = self.mapping_service.get_column_name(
                    {'name': old_mapping.widget_alias, 'label': old_mapping.label,
                     'widgetName': old_mapping.widget_name},
                    task_config.label_to_pinyin
                )
                if not old_db_col_name:
                    continue

                if old_db_col_name in existing_column_names and old_db_col_name not in system_fields:
                    cols_to_drop.append(old_db_col_name)

            # 7. 处理更新
            for widget_name in widgets_to_check:
                old_mapping = old_mappings[widget_name]
                new_widget = new_mappings[widget_name]

                # 获取 OLD derived name 和 type
                old_db_col_name = self.mapping_service.get_column_name(
                    {'name': old_mapping.widget_alias, 'label': old_mapping.label,
                     'widgetName': old_mapping.widget_name},
                    task_config.label_to_pinyin
                )
                old_sql_type = self.mapping_service.get_sql_type(old_mapping.type, None)

                # 获取 NEW derived name 和 type
                new_db_col_name = self.mapping_service.get_column_name(new_widget, task_config.label_to_pinyin)
                new_sql_type = self.mapping_service.get_sql_type(new_widget.get('type'), None)
                new_comment = new_widget.get('label', '')

                if not old_db_col_name or not new_db_col_name:
                    logger.warning(
                        f"task_id:[{task_id}] Could not derive column name for widget '{widget_name}' during update check. Skipping.")
                    continue

                # 确保旧列存在于 DB 中
                if old_db_col_name not in existing_column_names:
                    # 旧列名在 DB 中不存在 (可能上次 DDL 失败了？)
                    # 检查新列名是否存在
                    if new_db_col_name not in existing_column_names:
                        # 都不存在，视为 ADD
                        cols_to_add.append((new_db_col_name, new_sql_type, new_comment))
                    # (如果 new_db_col_name 存在，则忽略)
                    continue

                # --- 核心 DDL 逻辑 ---

                # Case 1: RENAME (列名变化)
                if old_db_col_name != new_db_col_name:
                    cols_to_rename.append((old_db_col_name, new_db_col_name, new_sql_type, new_comment))

                # Case 2: MODIFY TYPE or COMMENT (列名未变)
                elif old_db_col_name == new_db_col_name:
                    existing_col_info = existing_columns_info[new_db_col_name]
                    existing_sql_type = existing_col_info['type']
                    existing_comment = existing_col_info.get('comment', '')

                    type_is_different = self._is_type_different(existing_sql_type, new_sql_type)
                    # 仅当 comment 非空时才比较
                    comment_is_different = (new_comment and existing_comment != new_comment)

                    if type_is_different or comment_is_different:
                        cols_to_modify.append((new_db_col_name, new_sql_type, new_comment))

            # 8. 执行 DDL
            if not cols_to_add and not cols_to_drop and not cols_to_rename and not cols_to_modify:
                logger.info(f"task_id:[{task_id}] Table '{table_name}' structure is up-to-date.")
                return table

            logger.info(f"task_id:[{task_id}] Starting to sync table structure for '{table_name}'...")
            ddl_executed = False

            with engine.connect() as connection:
                with connection.begin() as transaction:
                    try:
                        # 执行删除列
                        if cols_to_drop:
                            logger.info(
                                f"task_id:[{task_id}] Will drop columns from table '{table_name}': {', '.join(cols_to_drop)}")
                            for col_name in cols_to_drop:
                                connection.execute(text(f"ALTER TABLE `{table_name}` DROP COLUMN `{col_name}`"))
                                ddl_executed = True

                        # 执行添加列
                        if cols_to_add:
                            logger.info(
                                f"task_id:[{task_id}] Will add columns to table '{table_name}': {', '.join([c[0] for c in cols_to_add])}")
                            for col_name, sql_type, comment in cols_to_add:
                                type_string = sql_type.compile(dialect=engine.dialect)
                                connection.execute(
                                    text(
                                        f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {type_string} COMMENT :comment"),
                                    {'comment': comment}
                                )
                                ddl_executed = True

                        # 执行重命名列
                        if cols_to_rename:
                            logger.info(
                                f"task_id:[{task_id}] Will rename columns in table '{table_name}': {', '.join([f'`{o}`->`{n}`' for o, n, _, _ in cols_to_rename])}")
                            for old_name, new_name, sql_type, comment in cols_to_rename:
                                type_string = sql_type.compile(dialect=engine.dialect)
                                connection.execute(text(
                                    f"ALTER TABLE `{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` {type_string} COMMENT :comment"),
                                    {'comment': comment}
                                )
                                ddl_executed = True

                        # 执行修改列类型
                        if cols_to_modify:
                            logger.info(
                                f"task_id:[{task_id}] Will modify column types in table '{table_name}': {', '.join([f'`{n}`' for n, _, _ in cols_to_modify])}")
                            for col_name, sql_type, comment in cols_to_modify:
                                type_string = sql_type.compile(dialect=engine.dialect)
                                connection.execute(text(
                                    f"ALTER TABLE `{table_name}` MODIFY COLUMN `{col_name}` {type_string} COMMENT :comment"),
                                    {'comment': comment}
                                )
                                ddl_executed = True

                        transaction.commit()
                        logger.info(f"task_id:[{task_id}] Table '{table_name}' structure sync complete.")

                    except Exception as e:
                        logger.error(f"task_id:[{task_id}] Error syncing table structure for '{table_name}': {e}",
                                     exc_info=True)
                        transaction.rollback()
                        raise  # 向上抛出异常

            # 9. 如果执行了 DDL，则刷新表定义缓存
            if ddl_executed:
                metadata.clear()

                engine_url_key = str(engine.url)
                if engine_url_key in self.inspected_tables_cache:
                    self.inspected_tables_cache[engine_url_key].pop(table_name, None)

                new_table = self.get_table_if_exists(table_name, engine)
                if new_table is not None:
                    logger.info(f"task_id:[{task_id}] Table '{table_name}' definition refreshed.")
                    return new_table
                else:
                    logger.error(
                        f"task_id:[{task_id}] Failed to reload table definition for '{table_name}' after DDL execution!")
                    return table  # 返回旧表

            return table  # 如果没有执行 DDL，返回原表

        except SQLAlchemyError as db_err:
            # config_session.rollback() # 回滚由调用者 handle_webhook_data 处理
            logger.error(f"task_id:[{task_id}] Database error during form structure update processing: {db_err}",
                         exc_info=True)
            log_sync_error(task_config=task_config, error=db_err, payload=data,
                           extra_info="Database error during update_table_schema_from_form")
            return table  # 返回旧表
        except Exception as e:
            # config_session.rollback()
            logger.error(f"task_id:[{task_id}] Unexpected error during form structure update processing: {e}",
                         exc_info=True)
            log_sync_error(task_config=task_config, error=e, payload=data,
                           extra_info="Unexpected error during update_table_schema_from_form")
            return table  # 返回旧表

    # --- 数据库数据 (DML) ---

    @retry()
    def clean_data_for_dml(self, table: Table, data: dict, task_config: SyncTask) -> dict:
        """根据表结构和动态配置清理数据"""
        table_columns = {c.name: c for c in table.columns}  # 存储列对象以获取类型，不是table.columns()
        cleaned_data = {}
        task_id = task_config.id

        config_session = ConfigSession()
        try:
            # 1. 加载映射
            all_mappings = config_session.query(FormFieldMapping).filter_by(task_id=task_id).all()
            mappings = {}
            # 存储 widget 字典
            widget_map_for_name_gen = {}

            for m in all_mappings:
                widget_dict = {'name': m.widget_alias, 'label': m.label, 'widgetName': m.widget_name}
                db_col_name = self.mapping_service.get_column_name(widget_dict, task_config.label_to_pinyin)

                if db_col_name:
                    mappings[m.widget_name] = db_col_name
                    if m.widget_alias and m.widget_alias.strip():
                        mappings[m.widget_alias] = db_col_name
                widget_map_for_name_gen[m.widget_name] = widget_dict

            # 2. 添加系统字段映射 (如果它们不在映射表中)
            system_fields = [
                '_id', 'appId', 'entryId', 'creator', 'updater', 'deleter',
                'createTime', 'updateTime', 'deleteTime', 'formName',  # 'flowState', 不是系统默认字段
                # 'wx_open_id', 'wx_nickname', 'wx_gender' # 微信字段
            ]
            for field in system_fields:
                if field not in mappings:
                    mappings[field] = field  # 假设系统字段名与列名相同

            # 3. 清理数据
            for key, value in data.items():
                # 使用新的双重映射字典查找列名
                db_col_name = mappings.get(key)

                # 如果映射中没有，并且 EXTRACT_SCHEMA_FROM_DATA 为 True，尝试从 key 推断 db_col_name
                if not db_col_name and key not in system_fields:
                    if EXTRACT_SCHEMA_FROM_DATA:
                        # # 替换所有非字母、数字、下划线的字符为空字符串
                        # db_col_name = re.sub(r'[^a-zA-Z0-9_]', '', key)
                        # 保留中文、英文、数字、下划线
                        db_col_name = re.sub(r'[^a-zA-Z0-9_\u4e00-\u9fff]', '', key)
                        if db_col_name and db_col_name[0].isdigit():
                            db_col_name = '_' + db_col_name
                        logger.warning(
                            f"task_id:[{task_id}] Inferring column name for field '{key}' as '{db_col_name}'.")

                    else:
                        logger.error(
                            f"task_id:[{task_id}] Cannot infer column name for field '{key}', skipping.")
                        log_sync_error(task_config=task_config,
                                       error=Exception(f"Cannot infer column name for field '{key}'"),
                                       extra_info=f"Cannot infer column name for field '{key}'", )

                if db_col_name and db_col_name in table_columns:
                    column_obj = table_columns[db_col_name]
                    col_type = column_obj.type

                    # 处理 None 值
                    if value is None:
                        cleaned_data[db_col_name] = None
                        continue

                    # 处理列表/字典 -> JSON 或 String
                    if isinstance(value, (dict, list)):
                        if task_config.json_as_string or not isinstance(col_type, JSON):
                            try:
                                cleaned_data[db_col_name] = json.dumps(value, ensure_ascii=False)
                            except TypeError as e:
                                logger.warning(
                                    f"task_id:[{task_id}] Cannot serialize value for field '{key}' (column: {db_col_name}): {e}, storing as string.")
                                cleaned_data[db_col_name] = str(value)
                        else:
                            cleaned_data[db_col_name] = value  # 直接赋值

                    # 处理字符串 -> DateTime
                    elif isinstance(col_type, DateTime) and isinstance(value, str):
                        try:
                            # 增加对无 Z 或 +00:00 的处理，假设它们是 UTC
                            if 'Z' not in value and '+' not in value and '-' not in value[10:]:  # 避免负号时区
                                value_to_parse = value + 'Z'
                            else:
                                value_to_parse = value.replace('Z', '+00:00')

                            dt_utc = datetime.fromisoformat(value_to_parse)
                            cleaned_data[db_col_name] = dt_utc.astimezone(TZ_UTC_8)  # 转换为本地时区
                        except (ValueError, TypeError):
                            logger.warning(
                                f"task_id:[{task_id}] Cannot parse string '{value}' to DateTime for field '{key}' (column: {db_col_name}), retaining original value.")
                            cleaned_data[db_col_name] = value  # 解析失败则保留原字符串

                    # 处理布尔值 (如果数据库列不是 Boolean 类型)
                    elif isinstance(value, bool) and not isinstance(col_type, Boolean):
                        cleaned_data[db_col_name] = 1 if value else 0  # 转为 1/0

                    # 其他类型直接赋值 (包括数字、布尔值到Boolean列等)
                    else:
                        # 添加类型检查，防止如数字写入文本列的问题 (虽然通常数据库会处理)
                        if isinstance(col_type, (String, Text, LONGTEXT)) and not isinstance(value, str):
                            cleaned_data[db_col_name] = str(value)
                        elif isinstance(col_type, (Integer, BigInteger, Float)) and not isinstance(value,
                                                                                                   (int, float)):
                            # 尝试转换，失败则记录警告
                            try:
                                cleaned_data[db_col_name] = int(value) if isinstance(col_type,
                                                                                     (Integer, BigInteger)) else float(
                                    value)
                            except (ValueError, TypeError):
                                logger.warning(
                                    f"task_id:[{task_id}] Cannot convert value '{value}' (type {type(value).__name__}) to numeric type for column '{db_col_name}', will store as None or raise error.")
                                cleaned_data[db_col_name] = None  # 或者可以选择跳过这个字段
                        else:
                            cleaned_data[db_col_name] = value  # 类型匹配或兼容，直接赋值

                # else: # 取消日志记录，避免过多无关信息
                #    logger.debug(f"字段 '{key}' (映射到列: {db_col_name}) 不在目标表 '{table.name}' 或映射中，已跳过。")

            return cleaned_data

        except SQLAlchemyError as e:
            logger.error(f"task_id:[{task_id}] Failed to query mappings during data cleaning: {e}",
                         exc_info=True)
            return {}
        finally:
            if config_session.is_active:  # 确保会话仍然活动
                config_session.close()

    @retry()
    def upsert_data(self, session: Session, table: Table, data: dict, task_config: SyncTask):
        """插入或更新单条数据 (使用 ON DUPLICATE KEY UPDATE 增强)"""
        cleaned_data = self.clean_data_for_dml(table, data, task_config)
        if not cleaned_data:
            logger.warning(f"task_id:[{task_config.id}] Cleaned data is empty, nothing to sync.")
            return

        data_id = cleaned_data.get('_id')
        if not data_id:
            logger.warning(
                f"task_id:[{task_config.id}] Data missing '_id', cannot perform upsert operation. Data: %s",
                json.dumps(data, ensure_ascii=False, default=str))
            return

        try:
            # 构造 INSERT ... ON DUPLICATE KEY UPDATE 语句
            stmt = insert(table).values(**cleaned_data)
            # 排除 _id 字段在 UPDATE 部分中更新自己
            update_data = {k: stmt.inserted[k] for k, v in cleaned_data.items() if k != '_id'}  # 使用 inserted 引用新值
            if update_data:  # 只有在除了 _id 还有其他字段时才添加 ON DUPLICATE KEY UPDATE
                on_duplicate_stmt = stmt.on_duplicate_key_update(**update_data)
            else:
                # 如果只有 _id，使用 INSERT IGNORE 或类似的逻辑避免错误，
                # 但这里假设总有其他数据或至少系统字段，所以直接用 stmt
                # 或者，如果确定是更新操作，可以构造 UPDATE 语句
                # 这里简化处理：如果只有 _id，仍然尝试 INSERT，让 ON DUPLICATE 处理
                on_duplicate_stmt = stmt.on_duplicate_key_update(_id=stmt.inserted._id)  # 无意义的更新，但能触发逻辑

            session.execute(on_duplicate_stmt)
            logger.debug(
                f"task_id:[{task_config.id}] Data upserted successfully (_id: {data_id}) to table '{table.name}'.")
            # commit 移到 handle_webhook_data 末尾

        except SQLAlchemyError as e:
            logger.error(
                f"task_id:[{task_config.id}] Failed to upsert data (_id: {data_id}) to table '{table.name}': {e}",
                exc_info=False)
            # (rollback 和 log_sync_error 移到 handle_webhook_data 的 except 块中)
            raise  # 重新抛出，让 handle_webhook_data 捕获并回滚
        except Exception as e:
            logger.error(f"task_id:[{task_config.id}] Unexpected error during upsert data (_id: {data_id}): {e}",
                         exc_info=True)
            raise

    @retry()
    def delete_data(self, session: Session, table: Table, data: dict, task_config: SyncTask):
        """根据 _id 删除数据"""
        data_id = data.get('_id')
        if not data_id:
            logger.warning(f"task_id:[{task_config.id}] Data for delete operation missing '_id'. Data: %s",
                           json.dumps(data, ensure_ascii=False, default=str))
            return

        logger.debug(f"task_id:[{task_config.id}] Preparing to delete data (_id: {data_id}) from '{table.name}'...")
        try:
            stmt = table.delete().where(table.c._id == data_id)
            result = session.execute(stmt)
            # (commit 移到 handle_webhook_data 的 with 块末尾)
            if result.rowcount == 0:
                logger.warning(
                    f"task_id:[{task_config.id}] Attempted to delete data (_id: {data_id}), but not found in database.")
            else:
                logger.debug(
                    f"task_id:[{task_config.id}] Successfully deleted data (_id: {data_id}) from '{table.name}'.")
        except SQLAlchemyError as e:
            logger.error(
                f"task_id:[{task_config.id}] Failed to delete data (_id: {data_id}) from table '{table.name}': {e}",
                exc_info=True)
            # (rollback 和 log_sync_error 移到 handle_webhook_data 的 except 块中)
            raise

    # --- 历史数据同步核心逻辑 ---

    @retry()
    def sync_historical_data(self, task_config: SyncTask, api_client: DataApi, delete_first: bool):
        """获取并同步指定任务的所有历史数据（已重构和优化）。"""
        app_id = task_config.app_id
        entry_id = task_config.entry_id
        table_name = task_config.table_name

        if not all([app_id, entry_id, table_name]):
            logger.error(
                f"task_id:[{task_config.id}] Task {task_config.id} (Tenant: {task_config.department.department_name}) missing App ID, Entry ID, or Table Name. Cannot perform full sync.")
            log_sync_error(task_config=task_config,
                           error=ValueError("Missing App ID/Entry ID/Table Name for full sync"))
            return

        logger.info(
            f"task_id:[{task_config.id}] Starting full sync task: {table_name} (Tenant: {task_config.department.department_name})")

        last_data_id = None
        total_records = 0
        sync_start_time = datetime.now(TZ_UTC_8)  # 记录同步开始时间

        # 每次同步都使用新的会话
        config_session = ConfigSession()

        # 在 sync_historical_data 开始时 task_config 是从当前 config_session 加载的
        try:
            # 重新加载 task_config
            task_config = config_session.query(SyncTask).options(
                joinedload(SyncTask.department).joinedload(Department.jdy_key_info),
                joinedload(SyncTask.database),
            ).get(task_config.id)
            if not task_config:
                logger.error(f"task_id:[{task_config.id}] Task not found in config_session for full sync.")
                return
        except Exception as e_fetch:
            logger.error(f"task_id:[{task_config.id}] Failed to fetch task in config_session: {e_fetch}")
            config_session.close()
            return

        # --- 11. 动态引擎/会话 ---
        try:
            dynamic_engine = get_dynamic_engine(task_config)
            dynamic_metadata = get_dynamic_metadata(dynamic_engine)
        except Exception as e:
            logger.error(f"task_id:[{task_config.id}] Task {table_name}: Failed to get dynamic engine: {e}",
                         exc_info=True)
            log_sync_error(task_config=task_config, error=e, extra_info="Failed to get dynamic engine for full sync")
            config_session.close()
            return

        try:
            # --- 1. 准备目标表 ---
            logger.info(f"task_id:[{task_config.id}] Task {table_name}: Preparing target table...")
            table = self.get_table_if_exists(table_name, dynamic_engine)

            # 检查是否需要清空表 (仅当表存在时)
            if delete_first:
                if table is not None:
                    logger.info(f"task_id:[{task_config.id}] Task {table_name}: Truncating table for full sync...")
                    try:
                        with get_dynamic_session(task_config) as target_session:
                            target_session.execute(table.delete())
                            target_session.commit()
                        logger.info(f"task_id:[{task_config.id}] Task {table_name}: Table truncated.")
                    except SQLAlchemyError as clear_err:
                        logger.error(f"task_id:[{task_config.id}] Failed to truncate table {table_name}: {clear_err}",
                                     exc_info=True)
                        # (rollback 在 with 块中自动处理)
                        raise  # 清空失败则无法继续全量同步

            # --- 2. 循环拉取和写入数据 ---
            logger.info(f"task_id:[{task_config.id}] Task {table_name}: Starting data fetch...")
            first_batch = True
            while True:
                try:
                    # 使用动态传入的 api_client
                    response_data = api_client.query_list_data(
                        app_id=app_id,
                        entry_id=entry_id,
                        limit=100,
                        data_id=last_data_id
                    )
                    data_list = response_data.get('data', [])
                except Exception as api_err:
                    logger.error(f"task_id:[{task_config.id}] Task {table_name}: API request failed: {api_err}",
                                 exc_info=True)
                    # 记录错误并中止本次同步
                    raise api_err  # 重新抛出，由外层 try-except 处理状态更新

                if not data_list:
                    logger.info(
                        f"task_id:[{task_config.id}] Task {table_name}: All data fetched. Total records processed: {total_records}.")
                    break

                logger.info(
                    f"task_id:[{task_config.id}] Task {table_name}: Successfully fetched {len(data_list)} data records.")
                total_records += len(data_list)

                # 如果是第一批数据且表不存在，则创建表并更新表结构
                if first_batch and table is None:
                    logger.info(
                        f"task_id:[{task_config.id}] Task {table_name}: Table does not exist, creating from first batch...")
                    table = self.get_or_create_table_from_data(table_name, data_list, task_config, dynamic_engine,
                                                               dynamic_metadata)
                    table = self.update_table_schema_from_data(table, data_list, task_config, dynamic_engine,
                                                               dynamic_metadata, config_session)
                    first_batch = False
                elif first_batch and table is not None:
                    # 表已存在，但仍需根据第一批数据检查并更新结构
                    logger.info(
                        f"task_id:[{task_config.id}] Task {table_name}: Table exists, checking structure against first batch...")
                    table = self.update_table_schema_from_data(table, data_list, task_config, dynamic_engine,
                                                               dynamic_metadata, config_session)
                    first_batch = False
                elif table is None:
                    # 理论上不应发生，因为表应在第一批数据时创建
                    logger.error(f"task_id:[{task_config.id}] Task {table_name}: Critical error - Table object lost!")
                    raise Exception(f"Table object lost during historical sync for {table_name}")

                # 批量处理数据写入
                try:
                    with get_dynamic_session(task_config) as target_session:
                        for item in data_list:
                            self.upsert_data(target_session, table, item, task_config)
                        target_session.commit()  # 每批提交一次
                    last_data_id = data_list[-1]['_id']
                except SQLAlchemyError as batch_err:
                    logger.error(
                        f"task_id:[{task_config.id}] Task {table_name}: Database error processing batch (last_data_id={last_data_id}): {batch_err}",
                        exc_info=True)
                    # (rollback 在 with 块中自动处理)
                    raise batch_err
                except Exception as item_err:  # 处理 upsert_data 内部可能捕获并记录的错误后继续的情况
                    # 如果 upsert_data 内部处理了错误并且没有重新抛出，这里不会捕获
                    # 如果 upsert_data 重新抛出了非 SQLAlchemyError，这里会捕获
                    logger.error(
                        f"task_id:[{task_config.id}] Task {table_name}: Unexpected error processing item (last_data_id={last_data_id}): {item_err}",
                        exc_info=True)
                    raise item_err

                # QPS 由 jdy_api.py 内部的 _throttle 控制

            # --- 3. 更新任务状态为成功 ---
            config_session.query(SyncTask).filter_by(id=task_config.id).update(
                {"sync_status": 'idle', "last_sync_time": datetime.now(TZ_UTC_8)}
            )
            config_session.commit()
            logger.info(f"task_id:[{task_config.id}] Task {table_name} full sync completed successfully.")

        except Exception as e:
            # --- 4. 处理同步过程中的任何异常 ---
            try:
                config_session.rollback()
            except Exception as rb_err:
                logger.error(f"task_id:[{task_config.id}] Error during config_session rollback: {rb_err}")

            # 更新任务状态为失败
            try:
                # 确保使用一个新的 session 或现有 session（如果仍然可用）来更新状态
                if not config_session.is_active:
                    config_session = ConfigSession()  # 创建新会话
                    logger.warning("Config session was inactive, created a new one to update failure status.")

                # 重新获取 task_to_update
                task_to_update = config_session.query(SyncTask).get(task_config.id)
                if task_to_update:
                    task_to_update.sync_status = 'error'
                    # 使用开始时间标记失败时间点
                    task_to_update.last_sync_time = sync_start_time
                    config_session.commit()
                    logger.info(f"task_id:[{task_config.id}] Task {table_name} status updated to error.")
                else:
                    logger.error(f"task_id:[{task_config.id}] Task not found for error status update.")

            except Exception as e_update:
                logger.error(f"task_id:[{task_config.id}] Error updating task {table_name} failure status: {e_update}",
                             exc_info=True)
                try:
                    config_session.rollback()  # 回滚状态更新的尝试
                except Exception:
                    pass

            logger.error(f"task_id:[{task_config.id}] Task {table_name} sync failed: {e}", exc_info=True)
            log_sync_error(
                task_config=task_config,
                error=e,
                payload={"task_id": task_config.id, "last_processed_data_id": last_data_id},
                extra_info="Error during sync_historical_data"
            )
        finally:
            # --- 5. 关闭会话 ---
            if config_session.is_active:
                config_session.close()
