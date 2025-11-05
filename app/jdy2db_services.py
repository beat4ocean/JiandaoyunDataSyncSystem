# -*- coding: utf-8 -*-
import json
import logging
import re
from datetime import datetime, time

from sqlalchemy import inspect, Table, Column, String, text, JSON, DateTime, Text, BigInteger, Float
from sqlalchemy.dialects.mysql import insert, LONGTEXT
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql.sqltypes import TypeEngine, Integer, Boolean, Time

from app import Config
from app.jdy_api import FormApi, DataApi
from app.models import (SyncTask, FormFieldMapping, ConfigSession)
from app.utils import (retry, convert_to_pinyin, log_sync_error, TZ_UTC_8, get_dynamic_engine, get_dynamic_session,
                       get_dynamic_metadata)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
    #     #     entry_id_from_payload = payload.get('entryId')
    #     #     entry_id = entry_id_from_data or entry_id_from_payload
    #     #     form_name = f"unknown_form_{entry_id}" if entry_id else "unknown_form"
    #     #     print(f"警告: 无法从 payload 中确定表单名称，使用默认值: {form_name}")
    #
    #     form_pinyin_name = convert_to_pinyin(form_name)
    #     # 返回原始名称和转换后的名称
    #     return form_pinyin_name

    @retry()
    def get_column_name(self, widget: dict, use_label_pinyin: bool) -> str:
        """
        根据动态配置获取数据库列名。
        1. 字段别名 (name)，如果不是默认的 _widget_ 开头
        2. 字段标题 (label)，根据配置决定是否转拼音
        3. 字段ID (widgetName) 作为备用
        :param widget: 简道云的字段对象
        :param use_label_pinyin: 是否将 Label 转为拼音
        :return: 数据库列名
        """
        widget_name = widget.get('widgetName')  # 这是字段 ID，通常是 _widget_ 开头
        widget_alias = widget.get('name')
        label = widget.get('label')

        final_name = None

        # 1. 优先使用别名 (如果已设置且非默认)
        if widget_alias and isinstance(widget_alias, str) and \
                not (widget_alias.startswith('_widget_') and widget_alias[8:].isdigit()):
            # 别名通常是用户自定义的，可能包含非法字符，需要清理
            final_name = widget_alias
            # print(f"使用别名: {widget_alias} -> {final_name}")

        # 2. 其次使用 label
        if not final_name and label and isinstance(label, str):
            if use_label_pinyin:
                final_name = convert_to_pinyin(label)
                # print(f"使用标签 (拼音): {label} -> {final_name}")
            else:
                final_name = label
                # print(f"使用标签 (原文清理): {label} -> {final_name}")

        # 3. 最后备用 widgetName (字段 ID)
        if not final_name and widget_name and isinstance(widget_name, str):
            # widgetName 通常是 _widget_xxx
            final_name = widget_name
            # print(f"使用 Widget Name: {widget_name} -> {final_name}")

        # 4. 清理 final_name 中的非法字符
        if final_name:
            # 替换所有非字母、数字、下划线的字符为空字符串
            final_name = re.sub(r'[^a-zA-Z0-9_]', '', final_name)
            # 确保不以数字开头 (如果数据库有此限制)
            if final_name and final_name[0].isdigit():
                final_name = '_' + final_name

        # 添加一个最终的非空检查
        if not final_name:
            print(f"警告：无法为 widget {widget} 生成有效的列名，将使用 'invalid_column'")
            final_name = 'invalid_column'

        return final_name

    @retry()
    def get_sql_type(self, type: str, data_value: any) -> TypeEngine:
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
            # print(f"JDY Type '{jdy_type}' mapped to {sql_type_class.__name__}")

        # 2. 如果类型映射成功，但需要根据值调整 (例如 String vs Text)
        if sql_type_class:
            if sql_type_class in (Text, String) and isinstance(data_value, str):
                if len(data_value) > 65535:
                    # print(f"Value length > 65535, promoting to LONGTEXT")
                    return LONGTEXT()
                elif len(data_value) > 1024:
                    # print(f"Value length > 1024, promoting to TEXT")
                    return Text()
                else:
                    # print(f"Value fits in String(1024)")
                    # For serial_number, radiogroup, combo, allow longer String if needed, e.g., String(255)
                    # Adjust based on expected max length for these types
                    return String(1024)

            elif sql_type_class is Float and isinstance(data_value, int):
                # Allow integers to be stored in Float columns
                # print("Integer value for Float type, allowed.")
                return Float()

            elif sql_type_class is BigInteger and isinstance(data_value, (int, str)):
                # Allow potential string representation of big integers
                try:
                    int(data_value)  # Check if convertible
                    # print("String/Int value for BigInteger type, allowed.")
                    return BigInteger()
                except (ValueError, TypeError):
                    print(f"警告：值 '{data_value}' 无法转换为 BigInteger，将使用 Text。")
                    return Text()  # Fallback if value cannot be converted
            # If type is mapped and doesn't need value adjustment, return instance
            # print(f"Returning instance: {sql_type_class.__name__}()")

            return sql_type_class

        # 3. 如果没有提供简道云类型或类型未知，则回退到基于值的推断
        # print(f"No JDY type or unknown type '{jdy_type}', inferring from value type: {type(data_value).__name__}")
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
        # print("Unknown value type, falling back to Text()")
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
            logger.error(f"任务 {task_id} 缺少 App ID 或 Entry ID，无法同步字段映射。")
            return

        logger.info(f"接收 {app_id}/{entry_id} (Task {task_id}) 数据，正在同步字段映射...")

        try:
            # 1. 从 API 获取最新的“目标”字段列表
            resp = api_client.get_form_widgets(app_id, entry_id)
            api_widgets = resp.get('widgets', [])

            if not api_widgets:
                logger.warning(f"警告: API 未返回 任务 {task_id} 的任何字段信息，跳过不予处理。")
                # 根据需求，这里可以决定是否要删除所有现有映射
                # config_session.query(FormFieldMapping).filter_by(app_id=app_id, entry_id=entry_id).delete()
                # config_session.commit()
                return

            # 2. 获取数据库中已存在的“当前”映射 (按 task_id)
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
                    logger.warning(f"API 返回的 widget 缺少 widgetName: {widget}")
                    continue
                api_widget_names.add(widget_name)

                # 统一计算列名
                column_name = self.get_column_name(widget, task_config.label_to_pinyin)

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
                logger.info(f"为 Task {task_id} 添加 {len(mappings_to_add)} 个新字段映射。")
            if mappings_to_delete:
                logger.info(f"为 Task {task_id} 检测到 {len(mappings_to_delete)} 个已删除字段，将从映射中移除。")
                for m in mappings_to_delete:
                    config_session.delete(m)

            # 只有在有实际变更时才 commit
            if config_session.new or config_session.dirty or config_session.deleted:
                config_session.commit()
                logger.info(f"字段映射同步完成 for Task {task_id}.")
            else:
                logger.info(f"字段映射无需更新 for Task {task_id}.")


        except SQLAlchemyError as db_err:
            config_session.rollback()
            logger.error(f"同步字段映射时发生数据库错误 for Task {task_id}: {db_err}", exc_info=True)
            log_sync_error(
                task_config=task_config,
                error=db_err,
                extra_info=f"Database error during form fields mapping sync for Task {task_id}"
            )
        except Exception as e:
            config_session.rollback()
            logger.error(f"拉取或更新字段映射失败 for Task {task_id}: {e}", exc_info=True)
            log_sync_error(
                task_config=task_config,
                error=e,
                extra_info=f"Failed to create or update form fields mapping for Task {task_id}"
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
            logger.warning("Webhook 负载中没有数据，已忽略。")
            return

        # --- 3. 获取动态引擎和元数据 ---
        try:
            if not task_config.database:
                task_config = config_session.query(SyncTask).options(joinedload(SyncTask.database)).get(task_config.id)
                if not task_config.database:
                    raise ValueError(f"任务 {task_config.id} 缺少关联的数据库配置。")

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
            logger.error(f"错误: 无法为任务 {task_config.id} 初始化动态数据库引擎: {e}")
            return

        app_id = task_config.app_id
        entry_id = task_config.entry_id

        # --- 1. 确定目标表名 ---
        table_name = None
        update_task_config_flag = False  # 重命名标志以避免歧义

        # 优先级 1: table_param (URL参数)
        if table_param and table_param.strip():
            table_name = table_param.strip()
            logger.info(f"使用 ?table= 参数指定的动态表名: {table_name}")
            # 检查是否需要更新配置库中的表名
            if task_config.table_name != table_name:
                logger.info(f"任务 {task_config.id} 的 table_name 将从 '{task_config.table_name}' 更新为: {table_name}")
                task_config.table_name = table_name
                update_task_config_flag = True

        # 优先级 2: task_config.table_name (数据库配置)
        elif task_config.table_name and task_config.table_name.strip():
            table_name = task_config.table_name.strip()
            logger.info(f"使用动态任务配置指定的表名: {table_name}")

        # 优先级 3: (表单名转换)
        else:
            if not form_name:
                log_sync_error(
                    task_config=task_config,
                    error=Exception("无法确定表名"),
                    payload=payload,
                    extra_info="table_param 和 task_config.table_name 均为空，且无法从 payload 中提取 form_name。"
                )
                logger.error(f"错误: 无法确定 {app_id}/{entry_id} 的表名。")
                return

            # 使用表单转拼音
            table_name = convert_to_pinyin(form_name)
            logger.info(f"使用表单转拼音表名: {table_name}")

            # 如果配置中没有表名，则将新生成的拼音表名存入
            if task_config.table_name != table_name:
                logger.info(f"任务 {task_config.id} 的 table_name 将设置为: {table_name}")
                task_config.table_name = table_name
                update_task_config_flag = True

        # --- 2. 提交表名更新 (如果需要) ---
        if update_task_config_flag:
            try:
                config_session.commit()
                logger.info(f"任务 {task_config.id} 的 table_name 已成功更新。")
            except SQLAlchemyError as e:  # 使用更具体的异常
                config_session.rollback()
                log_sync_error(
                    task_config=task_config,
                    error=e,
                    extra_info=f"更新 table_name 失败: {table_name}"
                )
                logger.error(f"错误: 更新任务 {task_config.id} 的 table_name 失败: {e}")
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
                    logger.info(f"任务 {task_config.id} 缺少 App/Entry ID，正在从 Webhook 自动填充...")
                    task_config.app_id = get_app_id
                    task_config.entry_id = get_entry_id
                    config_session.commit()
                    app_id = get_app_id
                    entry_id = get_entry_id
                    logger.info(f"任务 {task_config.id} 已更新 App ID={app_id}, Entry ID={entry_id}")
                except SQLAlchemyError as e:
                    config_session.rollback()
                    log_sync_error(task_config=task_config, error=e,
                                   extra_info="自动填充 App/Entry ID 失败 (可能唯一约束冲突)")
                    logger.error(f"任务 {task_config.id} 自动填充 App/Entry ID 失败: {e}")
                    # 如果填充失败 ，我们无法继续，因为映射依赖 App/Entry ID
                    return
            else:
                logger.error(f"任务 {task_config.id} 缺少 App/Entry ID，且无法从 Webhook 负载中提取。")
                return  # 无法继续

        # --- 3. 根据操作类型处理 ---
        try:
            # 3.1 form_update 单独处理
            if op == 'form_update':
                logger.info(f"处理 form_update 事件 for {table_name} ({app_id}/{entry_id})")
                # 立即同步映射表
                self.mapping_service.create_or_update_form_fields_mapping(config_session, task_config, api_client,
                                                                          form_name)

                # 获取或创建表
                table = self.get_table_if_exists(table_name, dynamic_engine)
                if table is None:
                    table = self.get_or_create_table_from_schema(table_name, data, task_config, dynamic_engine,
                                                                 dynamic_metadata)

                # 更新表结构 (处理添加、删除、重命名、类型变更)
                table = self.handle_table_schema_from_form(config_session, table, data, task_config, dynamic_engine,
                                                           dynamic_metadata)
                # form_update 不涉及数据写入，只需确保表结构最新

            # 3.2 其他数据操作
            elif op in ('data_create', 'data_update', 'data_recover', 'data_remove'):
                logger.info(f"处理 {op} 事件 for {table_name} ({app_id}/{entry_id})")
                # 确保映射存在 (如果不存在则创建)
                exists = config_session.query(FormFieldMapping).filter_by(
                    task_id=task_config.id
                ).first()
                if not exists:
                    logger.info(f"映射不存在，为 {app_id}/{entry_id} (Task {task_config.id}) 创建...")
                    self.mapping_service.create_or_update_form_fields_mapping(config_session, task_config,
                                                                              api_client, form_name)

                # 获取或创建表
                table = self.get_table_if_exists(table_name, dynamic_engine)
                if table is None and op != 'data_remove':  # 只有在需要写入数据时才创建表
                    logger.info(f"表 {table_name} 不存在，根据数据创建...")
                    table = self.get_or_create_table_from_data(table_name, [data], task_config, dynamic_engine,
                                                               dynamic_metadata)

                # # --- 检查是否需要 "首次全量覆盖" ---
                # todo
                # if (table is None or task_config.is_full_replace_first) and op != 'data_remove':
                #   此处拉起一个独立进程，用于处理 "首次全量覆盖"
                #   处理完成后，更新 task_config.is_full_replace_first 为 False

                if (table is None or task_config.is_full_replace_first) and op != 'data_remove':
                    logger.info(
                        f"任务 {task_config.id}: 触发首次全量同步 (is_full_replace_first={task_config.is_full_replace_first}, table_exists={table is not None})")

                    # 1. 确保表结构存在 (如果表不存在)
                    if table is None:
                        table = self.get_or_create_table_from_schema(table_name, data, task_config, dynamic_engine,
                                                                     dynamic_metadata)

                    # 2. 实例化 DataApi
                    data_api_client = DataApi(
                        api_key=task_config.department.jdy_key_info.api_key,
                        host=Config.JDY_API_HOST,
                        qps=30  # query_list_data
                    )
                    # 3. 执行全量同步
                    self.sync_historical_data(task_config, data_api_client)

                    # 4. [关键] 标记首次全量已完成
                    task_config.is_full_replace_first = False
                    config_session.commit()

                    # 5. 全量同步已包含当前数据，直接返回
                    logger.info(f"任务 {task_config.id}: 首次全量同步完成，跳过当前 webhook (op={op}) 的单独处理。")
                    return

                # 如果是 data_remove 且表不存在，则无需操作
                if table is None and op == 'data_remove':
                    logger.warning(f"警告: 收到删除操作，但表 '{table_name}' 不存在。跳过...")
                    return

                # --- 动态会话 DML ---
                # 使用动态会话执行数据库写入/删除
                with get_dynamic_session(task_config) as target_session:
                    try:
                        if op in ('data_create', 'data_update', 'data_recover'):
                            # 确保表结构与数据兼容 (主要处理新增列)
                            table = self.handle_table_schema_from_data(table, [data], task_config, dynamic_engine,
                                                                       dynamic_metadata)
                            self.upsert_data(target_session, table, data, task_config)

                        elif op == 'data_remove':
                            self.delete_data(target_session, table, data, task_config)

                        target_session.commit()  # 提交 DML

                    except SQLAlchemyError as db_err:  # 捕获 DML 错误
                        logger.error(f"处理 DML (op={op}) 时发生数据库错误: {db_err}", exc_info=True)
                        try:
                            target_session.rollback()
                        except Exception as rb_err:
                            logger.error(f"回滚 target_session 时出错: {rb_err}")
                        # 在会话回滚后记录错误
                        log_sync_error(task_config=task_config, error=db_err, payload=payload,
                                       extra_info=f"Database error during DML (op={op}) processing")
                    except Exception as e:  # 捕获 DML 期间的其他错误
                        logger.error(f"处理 DML (op={op}) 时发生意外错误: {e}", exc_info=True)
                        try:
                            target_session.rollback()
                        except Exception as rb_err:
                            logger.error(f"回滚 target_session 时出错: {rb_err}")
                        log_sync_error(task_config=task_config, error=e, payload=payload,
                                       extra_info=f"Unexpected error during DML (op={op}) processing")

            else:
                logger.warning(f"接收到未处理的操作类型: {op}")
                return  # 对于未处理的操作，直接返回

            # 仅当操作涉及 target_session 时才提交
            if op in ('data_create', 'data_update', 'data_recover', 'data_remove'):
                logger.info(f"Webhook 数据处理成功: op={op}, table={table_name}")
            elif op == 'form_update':
                logger.info(f"Webhook 表单结构更新处理成功: table={table_name}")

        except SQLAlchemyError as db_err:  # 捕获 DDL 或 映射 相关的错误
            logger.error(f"处理 webhook (DDL/Mapping) 时发生数据库错误: {db_err}", exc_info=True)
            # config_session 的回滚由 routes.py 处理
            log_sync_error(task_config=task_config, error=db_err, payload=payload,
                           extra_info="Database error during webhook DDL/Mapping processing")

        except Exception as e:  # 捕获其他所有意外错误
            logger.error(f"处理 webhook 时发生意外错误: {e}", exc_info=True)
            # config_session 的回滚由 routes.py 处理
            log_sync_error(task_config=task_config, error=e, payload=payload,
                           extra_info="Unexpected error during webhook processing")

    # --- 数据库表结构 (DDL) ---

    def get_table_if_exists(self, table_name: str, engine) -> Table | None:
        """
        如果表存在，则从缓存或数据库加载表定义。
        --- 6. 动态引擎 ---
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
                logger.info(f"从数据库 ({engine_url_key}) 加载表定义: {table_name}")
                return table
        except Exception as e:
            logger.error(f"检查或加载表 {table_name} 定义时出错 ({engine_url_key}): {e}", exc_info=True)
            self.inspected_tables_cache[engine_url_key].pop(table_name, None)
        return None

    def get_or_create_table_from_data(self, table_name: str, data_samples: list[dict], task_config: SyncTask, engine,
                                      metadata) -> Table:
        """
        获取或创建数据表。如果表不存在，则根据数据样本动态创建。
        --- 7. 动态引擎/元数据 ---
        """
        table = self.get_table_if_exists(table_name, engine)
        if table is not None:
            return table

        logger.info(f"表 '{table_name}' 不存在，正在根据数据样本创建...")
        try:
            column_defs, column_comments = self.get_column_schema(data_samples, task_config)
            columns = [Column(name, col_type, comment=column_comments.get(name)) for name, col_type in
                       column_defs.items()]

            table_comment = table_name
            if data_samples and data_samples[0]:  # 确保 data_samples 非空且第一个元素非空
                table_comment = data_samples[0].get('formName', table_name)

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
                logger.warning(f"警告: 表 '{table_name}' 数据样本中没有找到 '_id' 字段，已自动添加为唯一索引。")

            # 确保列名不重复 (理论上 get_column_definitions 应该处理了)
            final_column_names = {c.name for c in final_columns}
            if len(final_column_names) != len(final_columns):
                logger.error(f"创建表 '{table_name}' 时检测到重复的列名，请检查映射逻辑。")
                # 可以选择抛出异常或尝试去重，这里选择记录错误并继续（可能导致建表失败）

            table = Table(table_name, metadata, *final_columns, comment=table_comment, mysql_charset='utf8mb4')
            metadata.create_all(engine)  # 这会创建表
            logger.info(f"表 '{table_name}' 创建成功。")

            engine_url_key = str(engine.url)
            self.inspected_tables_cache[engine_url_key][table_name] = table  # 加入缓存
            return table
        except Exception as e:
            logger.error(f"根据数据样本创建表 '{table_name}' 失败: {e}", exc_info=True)
            log_sync_error(task_config=task_config, error=e,
                           extra_info=f"Failed to create table '{table_name}' from data samples")
            raise  # 创建失败则向上抛出异常

    def get_or_create_table_from_schema(self, table_name: str, form_schema_data: dict, task_config: SyncTask, engine,
                                        metadata) -> Table:
        """
        根据 form_update 事件中的 schema 数据获取或创建表。
        --- 8. 动态引擎/元数据 ---
        """
        table = self.get_table_if_exists(table_name, engine)
        if table is not None:
            return table

        logger.info(f"表 '{table_name}' 不存在，正在根据表单结构 (form_update) 创建...")
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
                        f"尝试为表 '{table_name}' 重复添加列 '{col_name}' (可能由 widget '{widget.get('widgetName')}' 产生)，已跳过。")

            table_comment = form_schema_data.get('name', table_name)
            table = Table(table_name, metadata, *columns, comment=table_comment, mysql_charset='utf8mb4')
            metadata.create_all(engine)
            logger.info(f"表 '{table_name}' 创建成功。")

            engine_url_key = str(engine.url)
            self.inspected_tables_cache[engine_url_key][table_name] = table
            return table
        except Exception as e:
            logger.error(f"根据 schema 创建表 '{table_name}' 失败: {e}", exc_info=True)
            log_sync_error(task_config=task_config, error=e,
                           extra_info=f"Failed to create table '{table_name}' from schema")
            raise

    def get_column_schema(self, data_samples: list[dict], task_config: SyncTask) -> (dict, dict):
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
            logger.error(f"查询字段映射失败 for Task {task_config.id}: {e}", exc_info=True)
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
                    db_col_name = self.mapping_service.get_column_name(
                        {'name': mapping_info.widget_alias, 'label': mapping_info.label,
                         'widgetName': mapping_info.widget_name},
                        task_config.label_to_pinyin
                    )
                    comment = mapping_info.label
                    jdy_type = mapping_info.type  # 获取简道云类型
                    sql_type_instance = self.mapping_service.get_sql_type(jdy_type, sample_value)  # 直接获取实例
                else:
                    # # 如果映射不存在，则退化为旧逻辑
                    # db_col_name = re.sub(r'[^a-zA-Z0-9_]', '', key)
                    # sql_type_instance = get_sql_type(None, sample_value)
                    logger.error(f"字段映射不存在 for {task_config.app_id}/{task_config.entry_id}: {key}")
                    log_sync_error(task_config=task_config,
                                   error=Exception("Field mapping not found"),
                                   extra_info=f"字段映射不存在 for {task_config.app_id}/{task_config.entry_id}: {key}")

            if not db_col_name or db_col_name in processed_col_names:
                if db_col_name in processed_col_names:
                    logger.warning(f"推断列定义时检测到重复的目标列名 '{db_col_name}' (可能源自 key '{key}'), 已跳过。")
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

    def handle_table_schema_from_data(self, table: Table, data_batch: list[dict], task_config: SyncTask, engine,
                                      metadata) -> Table:
        """
        同步数据库表结构（数据同步触发）：仅处理新增列。
        --- 9. 动态引擎/元数据 ---
        """
        inspector = inspect(engine)
        try:
            existing_columns_info = {c['name']: c for c in inspector.get_columns(table.name)}
            existing_columns = set(existing_columns_info.keys())
        except Exception as e:
            logger.error(f"无法获取表 '{table.name}' 的列信息: {e}", exc_info=True)
            return table  # 无法获取结构信息，直接返回

        config_session = ConfigSession()
        all_new_columns = {}  # {col_name: {"value": ..., "type": ..., "comment": ...}}

        # 存储 widget 字典
        widget_map_for_name_gen = {}

        try:
            # 1. 获取当前映射
            mappings = {
                m.widget_name: m
                for m in config_session.query(FormFieldMapping).filter_by(
                    task_id=task_config.id
                ).all()
            }

            expected_db_columns = set()
            for m in mappings.values():
                # 动态获取列名
                widget_dict = {'name': m.widget_alias, 'label': m.label, 'widgetName': m.widget_name}
                db_col_name = self.mapping_service.get_column_name(widget_dict, task_config.label_to_pinyin)
                expected_db_columns.add(db_col_name)
                widget_map_for_name_gen[m.widget_name] = widget_dict

            system_fields = {
                '_id', 'appId', 'entryId', 'creator', 'updater', 'deleter', 'createTime', 'updateTime', 'deleteTime',
                'formName', 'flowState'}
            all_expected_columns = expected_db_columns.union(system_fields)

            # 2. 计算需要添加的列 (基于传入的数据和映射)
            if data_batch:  # 仅当有数据时才检查新列
                for data_item in data_batch:
                    if not isinstance(data_item, dict):
                        continue  # 跳过无效数据项

                    for key, value in data_item.items():
                        mapping_info = mappings.get(key)
                        if not mapping_info: continue

                        # 动态获取列名
                        widget_dict = widget_map_for_name_gen.get(key)
                        if not widget_dict: continue  # 理论上不应发生

                        db_col_name = self.mapping_service.get_column_name(widget_dict, task_config.label_to_pinyin)
                        if not db_col_name or not db_col_name.strip():
                            # logger.warning(f"警告: 检测到空的列名，跳过添加。Widget Name: {key}")
                            continue

                        # 如果列不存在 且 未被标记为待添加
                        if db_col_name not in existing_columns and db_col_name not in all_new_columns:
                            all_new_columns[db_col_name] = {
                                "value": value,  # 用于类型推断
                                "type": mapping_info.type,  # 简道云类型
                                "comment": mapping_info.label
                            }

            # 3. 计算需要删除的列
            # 数据库中存在，但期望的列定义中没有的列
            cols_to_drop = existing_columns - all_expected_columns
            # 为防止映射表更新延迟，如果一个列刚被识别为新列，就不应该删除它
            cols_to_drop = cols_to_drop - set(all_new_columns.keys())

        except SQLAlchemyError as e:
            logger.error(f"查询字段映射失败 for Task {task_config.id}: {e}", exc_info=True)
            # 映射查询失败，无法安全地添加列
            all_new_columns = {}
        finally:
            config_session.close()

        # 如果没有结构变更，则直接返回
        if not all_new_columns and not cols_to_drop:
            return table

        # 4. 执行数据库变更
        logger.info(f"开始同步表 '{table.name}' 的结构...")
        if all_new_columns:
            logger.info(f"在表 '{table.name}' 中检测到新字段，正在批量添加: {', '.join(all_new_columns.keys())}")
        if cols_to_drop:
            logger.info(f"在表 '{table.name}' 中检测到多余字段，正在批量删除: {', '.join(cols_to_drop)}")

        try:
            with engine.connect() as connection:
                with connection.begin() as transaction:
                    # 执行删除操作
                    for col_name in cols_to_drop:
                        if col_name == '_id':
                            continue  # 保护 _id 不被删除
                        connection.execute(text(f"ALTER TABLE `{table.name}` DROP COLUMN `{col_name}`"))

                    # 执行添加操作
                    for col_name, col_info in all_new_columns.items():
                        if not col_name or not col_name.strip():  # 再次检查
                            logger.warning(f"警告: 再次检测到空的列名，在执行SQL前跳过。")
                            continue

                        # 使用 get_sql_type 获取 SQLAlchemy 类型实例
                        sql_type_instance = self.mapping_service.get_sql_type(col_info["type"], col_info['value'])
                        # 获取类型的 SQL 字符串表示
                        type_string = sql_type_instance.compile(dialect=engine.dialect)

                        try:
                            connection.execute(
                                text(
                                    f"ALTER TABLE `{table.name}` ADD COLUMN `{col_name}` {type_string} COLLATE utf8mb4_general_ci COMMENT :comment"),
                                {'comment': col_info['comment']}
                            )
                            logger.info(f"成功为表 '{table.name}' 添加列 '{col_name}' 类型 '{type_string}'。")
                        except Exception as alter_err:
                            # 如果添加单列失败（例如，列已存在于并发操作中），记录错误并继续尝试添加其他列
                            logger.error(f"为表 '{table.name}' 添加列 '{col_name}' 失败: {alter_err}")
                            # 不回滚整个事务，允许其他列的添加继续

                    transaction.commit()  # 提交所有成功的 ALTER TABLE 操作
        except Exception as e:
            # 如果连接或事务启动失败
            # transaction.rollback() # (已在 with connection.begin() 中自动回滚)
            logger.error(f"为表 '{table.name}' 添加列时发生连接或事务错误: {e}", exc_info=True)
            log_sync_error(task_config=task_config, error=e,
                           extra_info="Error during handle_table_schema_from_data (connection/transaction)")
            # 发生严重错误，可能无法继续，但还是尝试重新加载表定义
            # return table # 返回旧表定义可能更安全

        # 清理缓存并重新加载更新后的表定义
        metadata.clear()

        engine_url_key = str(engine.url)
        if engine_url_key in self.inspected_tables_cache:
            self.inspected_tables_cache[engine_url_key].pop(table.name, None)

        new_table = self.get_table_if_exists(table.name, engine)
        if new_table:
            logger.info(f"表 '{table.name}' 结构已更新并重新加载。")
            return new_table
        else:
            logger.error(f"添加列后无法重新加载表 '{table.name}' 的定义！")
            return table  # 返回旧表

    # 优化：比较 SQLAlchemy 类型实例
    def _is_type_different(self, existing_sqlalch_type: TypeEngine, expected_sqlalch_type: TypeEngine) -> bool:
        """比较两个 SQLAlchemy 类型实例是否代表不同的数据库类型"""
        if type(existing_sqlalch_type) != type(expected_sqlalch_type):
            return True
        # 对于 String 类型，还需要比较长度
        if isinstance(existing_sqlalch_type, String) and isinstance(expected_sqlalch_type, String):
            # 注意：这里的比较可能不完全准确，因为数据库实际长度可能不同
            # return existing_sqlalch_type.length != expected_sqlalch_type.length
            return False  # 忽略长度比较
        # 可以为其他需要比较属性的类型添加更多逻辑 (如 DECIMAL 的精度)
        return False

    def handle_table_schema_from_form(self, config_session: Session, table: Table, data: dict, task_config: SyncTask,
                                      engine, metadata):
        """
        处理表单结构更新事件 (form_update)，处理列的添加、删除、重命名和类型变更。
        --- 10. 动态引擎/元数据 ---
        """
        table_name = table.name
        app_id = data.get('appId')
        entry_id = data.get('entryId')
        widgets = data.get('widgets', [])
        form_name = data.get('name') or data.get('formName')
        task_id = task_config.id

        if not all([app_id, entry_id]):  # widgets 可以为空
            logger.warning("form_update 事件缺少 app_id 或 entry_id，跳过结构处理。")
            return table

        logger.info(f"开始根据 form_update 同步表 '{table_name}' 的结构...")
        try:
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                # 如果表不存在，直接基于 schema 创建，无需同步
                logger.info(f"表 '{table_name}' 不存在，将直接根据 schema 创建。")
                return self.get_or_create_table_from_schema(table_name, data, task_config, engine, metadata)

            # 1. 获取数据库当前列信息 (包括类型)
            existing_columns_info = {c['name']: c for c in inspector.get_columns(table_name)}
            existing_columns = set(existing_columns_info.keys())

            # 2. 获取映射表的当前状态 (按 task_id)
            current_mappings = {
                m.widget_name: m for m in
                config_session.query(FormFieldMapping).filter_by(task_id=task_id).all()
            }

            # 生成旧的 列名 -> widget_name 映射
            old_col_to_widget_map = {}
            for m in current_mappings.values():
                old_col_name = self.mapping_service.get_column_name(
                    {'name': m.widget_alias, 'label': m.label, 'widgetName': m.widget_name},
                    task_config.label_to_pinyin
                )
                old_col_to_widget_map[old_col_name] = m.widget_name

            # 3. 根据传入的 widgets 定义期望的表结构
            expected_db_columns = {}  # {db_col_name: {'widget': widget, 'sql_type': SQLAType}}
            widget_to_db_col_map = {}  # {widget_name: db_col_name}

            # 添加系统字段到期望结构中 (类型从 get_or_create_table_from_schema 获取)
            system_fields_defs = {
                'appId': String(50), 'entryId': String(50), 'creator': JSON, 'updater': JSON,
                'deleter': JSON, 'createTime': DateTime, 'updateTime': DateTime, 'deleteTime': DateTime,
                'formName': Text, 'flowState': BigInteger, '_id': String(50)  # 加入 _id
            }
            for name, sql_type_inst in system_fields_defs.items():
                expected_db_columns[name] = {'widget': None, 'sql_type': sql_type_inst}

            # 处理来自 API 的 widgets
            for widget in widgets:
                db_col_name = self.mapping_service.get_column_name(widget, task_config.label_to_pinyin)
                if db_col_name:
                    sql_type_instance = self.mapping_service.get_sql_type(widget.get('type'), None)
                    expected_db_columns[db_col_name] = {'widget': widget, 'sql_type': sql_type_instance}
                    widget_name = widget.get('widgetName')
                    if widget_name:
                        widget_to_db_col_map[widget_name] = db_col_name

            # 4. 计算 DDL 变更
            cols_to_add = set(expected_db_columns.keys()) - existing_columns
            cols_to_drop = set()
            cols_to_rename = []  # (old_name, new_name, widget, sql_type_instance)
            cols_to_modify = []  # (col_name, widget, new_sql_type_instance)

            processed_for_rename = set()

            # 遍历数据库中的旧列，而不是映射
            for old_col_name in existing_columns:
                if old_col_name in system_fields_defs:
                    continue  # 跳过系统字段

                # 找到这个旧列对应的 widget_name
                widget_name = old_col_to_widget_map.get(old_col_name)
                if not widget_name:
                    # 数据库列在映射中不存在，可能是历史遗留的，检查是否要删除
                    if old_col_name not in expected_db_columns:
                        cols_to_drop.add(old_col_name)
                    continue

                # 检查是否需要重命名
                new_col_name = widget_to_db_col_map.get(widget_name)
                expected_info = expected_db_columns.get(new_col_name) if new_col_name else None

                if new_col_name and old_col_name != new_col_name:
                    # 需要重命名
                    if expected_info:
                        cols_to_rename.append(
                            (old_col_name, new_col_name, expected_info['widget'], expected_info['sql_type']))
                        processed_for_rename.add(old_col_name)
                        # 如果新列名原本在待添加列表里，移除它，因为它将通过重命名产生
                        cols_to_add.discard(new_col_name)
                    else:
                        # 理论上不应发生，因为 widget_to_db_col_map 来自 expected_db_columns
                        logger.warning(f"重命名列 '{old_col_name}' 时找不到目标列 '{new_col_name}' 的 widget 信息。")

                elif new_col_name and old_col_name == new_col_name:
                    # 名称相同，检查类型是否需要修改
                    if expected_info:
                        existing_col_info = existing_columns_info.get(old_col_name)
                        if existing_col_info:
                            # existing_col_info['type'] 是 SQLAlchemy Type 对象
                            if self._is_type_different(existing_col_info['type'], expected_info['sql_type']):
                                cols_to_modify.append(
                                    (old_col_name, expected_info['widget'], expected_info['sql_type']))
                                processed_for_rename.add(old_col_name)  # 标记已处理
                        else:
                            # 理论上不应发生
                            logger.warning(f"检查列 '{old_col_name}' 类型时找不到其现有信息。")

                # 如果 widget_name 不在 widget_to_db_col_map 中，说明该字段已被删除
                elif widget_name not in widget_to_db_col_map:
                    if old_col_name not in processed_for_rename:  # 确保不是即将被重命名的列
                        cols_to_drop.add(old_col_name)

            # 再次确认要删除的列：存在于数据库，但不在期望的列中，且不是系统字段，也不是重命名的源列
            final_cols_to_drop = (existing_columns - set(expected_db_columns.keys()) - set(
                system_fields_defs.keys())) | cols_to_drop
            final_cols_to_drop -= processed_for_rename  # 从待删除中移除已被重命名或修改类型处理的列

            # 5. 执行 DDL 变更
            if not cols_to_add and not final_cols_to_drop and not cols_to_rename and not cols_to_modify:
                logger.info(f"表 '{table_name}' 结构无需更新。")
                return table

            logger.info(f"开始同步表 '{table_name}' 的结构...")
            ddl_executed = False

            with engine.connect() as connection:
                with connection.begin() as transaction:
                    try:
                        # 执行删除列
                        if final_cols_to_drop:
                            logger.info(f"将从表 '{table_name}' 删除列: {', '.join(final_cols_to_drop)}")
                            for col_name in final_cols_to_drop:
                                if col_name in existing_columns and col_name not in system_fields_defs:
                                    connection.execute(text(f"ALTER TABLE `{table_name}` DROP COLUMN `{col_name}`"))
                                    ddl_executed = True

                        # 执行添加列
                        if cols_to_add:
                            logger.info(f"将向表 '{table_name}' 添加列: {', '.join(cols_to_add)}")
                            for col_name in cols_to_add:
                                info = expected_db_columns[col_name]
                                sql_type_inst = info['sql_type']
                                type_string = sql_type_inst.compile(dialect=engine.dialect)
                                comment = info['widget'].get('label', '') if info[
                                    'widget'] else col_name  # 使用 widget label 或列名作为 comment
                                connection.execute(
                                    text(
                                        f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {type_string} COLLATE utf8mb4_general_ci COMMENT :comment"),
                                    {'comment': comment}
                                )
                                ddl_executed = True

                        # 执行重命名列
                        if cols_to_rename:
                            logger.info(
                                f"将在表 '{table_name}' 重命名列: {', '.join([f'`{o}`->`{n}`' for o, n, _, _ in cols_to_rename])}")
                            for old_name, new_name, widget, sql_type_inst in cols_to_rename:
                                # 注意: 重命名通常需要知道原列类型，这里简化处理，假设类型不变或在MODIFY步骤处理
                                # MySQL 使用 CHANGE COLUMN 同时指定新旧名称和类型定义
                                existing_col_info = existing_columns_info.get(old_name)
                                if existing_col_info:
                                    # 使用推断出的 *新* SQL 类型和 comment 来定义新列
                                    type_string = sql_type_inst.compile(dialect=engine.dialect)
                                    comment = widget.get('label', '') if widget else new_name
                                    # 检查新列名是否已存在（理论上不应发生，因为已从 cols_to_add 移除）
                                    temp_inspector = inspect(connection)
                                    if new_name in [c['name'] for c in temp_inspector.get_columns(table_name) if
                                                    c['name'] != old_name]:
                                        logger.error(
                                            f"无法重命名列 '{old_name}' 为 '{new_name}'，因为目标列名已存在。")
                                        continue  # 跳过此重命名

                                    connection.execute(text(
                                        f"ALTER TABLE `{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` {type_string} COLLATE utf8mb4_general_ci COMMENT :comment"),
                                        {'comment': comment}
                                    )
                                    ddl_executed = True
                                else:
                                    logger.warning(f"尝试重命名列 '{old_name}' 时未找到其原始信息，跳过。")

                        # 执行修改列类型
                        if cols_to_modify:
                            logger.info(
                                f"将在表 '{table_name}' 修改列类型: {', '.join([f'`{n}`' for n, _, _ in cols_to_modify])}")
                            for col_name, widget, new_sql_type_inst in cols_to_modify:
                                type_string = new_sql_type_inst.compile(dialect=engine.dialect)
                                comment = widget.get('label', '') if widget else col_name  # 保持 comment
                                connection.execute(text(
                                    f"ALTER TABLE `{table_name}` MODIFY COLUMN `{col_name}` {type_string} COLLATE utf8mb4_general_ci COMMENT :comment"),
                                    {'comment': comment}
                                )
                                ddl_executed = True

                        transaction.commit()
                        logger.info(f"表 '{table_name}' 结构同步完成。")

                    except Exception as e:
                        logger.error(f"同步表 '{table_name}' 结构时出错: {e}", exc_info=True)
                        transaction.rollback()
                        raise  # 向上抛出异常

            # 6. 如果执行了 DDL，则刷新表定义缓存
            if ddl_executed:
                metadata.clear()

                engine_url_key = str(engine.url)
                if engine_url_key in self.inspected_tables_cache:
                    self.inspected_tables_cache[engine_url_key].pop(table_name, None)

                new_table = self.get_table_if_exists(table_name, engine)
                if new_table:
                    logger.info(f"表 '{table_name}' 定义已刷新。")
                    return new_table
                else:
                    logger.error(f"执行 DDL 后无法重新加载表 '{table_name}' 定义！")
                    return table  # 返回旧表

            return table  # 如果没有执行 DDL，返回原表

        except SQLAlchemyError as db_err:
            config_session.rollback()  # 回滚映射会话（如果之前有更改）
            logger.error(f"处理表单结构更新时发生数据库错误: {db_err}", exc_info=True)
            log_sync_error(task_config=task_config, error=db_err, payload=data,
                           extra_info="Database error during handle_table_schema_from_form")
            return table  # 返回旧表
        except Exception as e:
            config_session.rollback()
            logger.error(f"处理表单结构更新时发生意外错误: {e}", exc_info=True)
            log_sync_error(task_config=task_config, error=e, payload=data,
                           extra_info="Unexpected error during handle_table_schema_from_form")
            return table  # 返回旧表

    # --- 数据库数据 (DML) ---

    def clean_data_for_db(self, table: Table, data: dict, task_config: SyncTask) -> dict:
        """根据表结构和动态配置清理数据"""
        table_columns = {c.name: c for c in table.columns}  # 存储列对象以获取类型
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
            system_fields = ['_id', 'appId', 'entryId', 'creator', 'updater', 'deleter',
                             'createTime', 'updateTime', 'deleteTime', 'formName', 'flowState']
            for field in system_fields:
                if field not in mappings:
                    mappings[field] = field  # 假设系统字段名与列名相同

            # 3. 清理数据
            for key, value in data.items():
                # 使用新的双重映射字典查找列名
                db_col_name = mappings.get(key)

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
                                    f"无法序列化字段 '{key}' (列: {db_col_name}) 的值 {value}: {e}，将存为字符串。")
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
                                f"无法将字符串 '{value}' 解析为字段 '{key}' (列: {db_col_name}) 的 DateTime，将保留原值。")
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
                                    f"无法将值 '{value}' (类型 {type(value).__name__}) 转换为列 '{db_col_name}' 的数字类型，将存为 None 或引发错误。")
                                cleaned_data[db_col_name] = None  # 或者可以选择跳过这个字段
                        else:
                            cleaned_data[db_col_name] = value  # 类型匹配或兼容，直接赋值

                # else: # 取消日志记录，避免过多无关信息
                #    logger.debug(f"字段 '{key}' (映射到列: {db_col_name}) 不在目标表 '{table.name}' 或映射中，已跳过。")

            return cleaned_data

        except SQLAlchemyError as e:
            logger.error(f"清理数据时查询映射失败 for Task {task_id}: {e}", exc_info=True)
            return {}
        finally:
            if config_session.is_active:  # 确保会话仍然活动
                config_session.close()

    @retry()
    def upsert_data(self, session: Session, table: Table, data: dict, task_config: SyncTask):
        """插入或更新单条数据 (使用 ON DUPLICATE KEY UPDATE 增强)"""
        cleaned_data = self.clean_data_for_db(table, data, task_config)
        if not cleaned_data:
            logger.warning("警告：清理后的数据为空，没有可同步的内容。")
            return

        data_id = cleaned_data.get('_id')
        if not data_id:
            logger.warning("警告：数据中缺少 '_id'，无法执行更新/插入操作。数据: %s",
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
            # logger.info(f"成功 Upsert 数据 (ID: {data_id}) 到表 '{table.name}'。")
            # commit 移到 handle_webhook_data 末尾

        except SQLAlchemyError as e:
            logger.error(f"Upsert 数据 (ID: {data_id}) 到表 '{table.name}' 失败: {e}", exc_info=False)
            logger.debug("失败的 cleaned_data: %s", cleaned_data)
            # (rollback 和 log_sync_error 移到 handle_webhook_data 的 except 块中)
            raise  # 重新抛出，让 handle_webhook_data 捕获并回滚
        except Exception as e:
            logger.error(f"Upsert 数据 (ID: {data_id}) 时发生意外错误: {e}", exc_info=True)
            raise

    @retry()
    def delete_data(self, session: Session, table: Table, data: dict, task_config: SyncTask):
        """根据 _id 删除数据"""
        data_id = data.get('_id')
        if not data_id:
            logger.warning("警告：删除操作的数据中缺少 '_id'。数据: %s",
                           json.dumps(data, ensure_ascii=False, default=str))
            return

        logger.info(f"准备从 '{table.name}' 删除数据 (ID: {data_id})...")
        try:
            stmt = table.delete().where(table.c._id == data_id)
            result = session.execute(stmt)
            # (commit 移到 handle_webhook_data 的 with 块末尾)
            if result.rowcount == 0:
                logger.warning(f"警告：尝试删除数据 (ID: {data_id})，但在数据库中未找到。")
            else:
                logger.info(f"成功从 '{table.name}' 删除数据 (ID: {data_id})。")
        except SQLAlchemyError as e:
            logger.error(f"从表 '{table.name}' 删除数据 (ID: {data_id}) 失败: {e}", exc_info=True)
            # (rollback 和 log_sync_error 移到 handle_webhook_data 的 except 块中)
            raise

    # --- 历史数据同步核心逻辑 ---

    @retry()
    def sync_historical_data(self, task_config: SyncTask, api_client: DataApi):
        """获取并同步指定任务的所有历史数据（已重构和优化）。"""
        app_id = task_config.app_id
        entry_id = task_config.entry_id
        table_name = task_config.table_name

        if not all([app_id, entry_id, table_name]):
            logger.error(
                f"任务 {task_config.id} (租户: {task_config.department.department_name}) 缺少 App ID, Entry ID 或 Table Name，无法执行全量同步。")
            log_sync_error(task_config=task_config,
                           error=ValueError("Missing App ID/Entry ID/Table Name for full sync"))
            return

        logger.info(f"开始执行全量同步任务: {table_name} (租户: {task_config.department.department_name})")

        last_data_id = None
        total_records = 0
        sync_start_time = datetime.now(TZ_UTC_8)  # 记录同步开始时间

        # 每次同步都使用新的会话
        config_session = ConfigSession()

        # --- 11. 动态引擎/会话 ---
        try:
            dynamic_engine = get_dynamic_engine(task_config)
            dynamic_metadata = get_dynamic_metadata(dynamic_engine)
        except Exception as e:
            logger.error(f"任务 {table_name}: 无法获取动态引擎: {e}", exc_info=True)
            log_sync_error(task_config=task_config, error=e, extra_info="Failed to get dynamic engine for full sync")
            config_session.close()
            return

        try:
            # --- 1. 准备目标表 ---
            logger.info(f"任务 {table_name}: 准备目标表...")
            table = self.get_table_if_exists(table_name, dynamic_engine)

            # 检查是否需要清空表 (仅当表存在时)
            if table is not None:
                logger.info(f"任务 {table_name}: 正在清空表以进行全量同步...")
                try:
                    with get_dynamic_session(task_config) as target_session:
                        target_session.execute(table.delete())
                        target_session.commit()
                    logger.info(f"任务 {table_name}: 表已清空。")
                except SQLAlchemyError as clear_err:
                    logger.error(f"清空表 {table_name} 失败: {clear_err}", exc_info=True)
                    # (rollback 在 with 块中自动处理)
                    raise  # 清空失败则无法继续全量同步

            # --- 2. 循环拉取和写入数据 ---
            logger.info(f"任务 {table_name}: 开始拉取数据...")
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
                    logger.error(f"任务 {table_name}: API 请求失败: {api_err}", exc_info=True)
                    # 记录错误并中止本次同步
                    raise api_err  # 重新抛出，由外层 try-except 处理状态更新

                if not data_list:
                    logger.info(f"任务 {table_name}: 已拉取所有数据。总共处理 {total_records} 条记录。")
                    break

                logger.info(f"任务 {table_name}: 成功拉取 {len(data_list)} 条数据。")
                total_records += len(data_list)

                # 如果是第一批数据且表不存在，则创建表并更新表结构
                if first_batch and table is None:
                    logger.info(f"任务 {table_name}: 表不存在，根据第一批数据创建...")
                    table = self.get_or_create_table_from_data(table_name, data_list, task_config, dynamic_engine,
                                                               dynamic_metadata)
                    table = self.handle_table_schema_from_data(table, data_list, task_config, dynamic_engine,
                                                               dynamic_metadata)
                    first_batch = False
                elif first_batch and table is not None:
                    # 表已存在，但仍需根据第一批数据检查并更新结构
                    logger.info(f"任务 {table_name}: 表已存在，根据第一批数据检查结构...")
                    table = self.handle_table_schema_from_data(table, data_list, task_config, dynamic_engine,
                                                               dynamic_metadata)
                    first_batch = False
                elif table is None:
                    # 理论上不应发生，因为表应在第一批数据时创建
                    logger.error(f"任务 {table_name}: 严重错误 - 表对象丢失！")
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
                        f"任务 {table_name}: 处理批次数据时发生数据库错误 (last_data_id={last_data_id}): {batch_err}",
                        exc_info=True)
                    # (rollback 在 with 块中自动处理)
                    raise batch_err
                except Exception as item_err:  # 处理 upsert_data 内部可能捕获并记录的错误后继续的情况
                    # 如果 upsert_data 内部处理了错误并且没有重新抛出，这里不会捕获
                    # 如果 upsert_data 重新抛出了非 SQLAlchemyError，这里会捕获
                    logger.error(
                        f"任务 {table_name}: 处理单条数据时发生意外错误 (last_data_id={last_data_id}): {item_err}",
                        exc_info=True)
                    target_session.rollback()
                    raise item_err

                # QPS 由 jdy_api.py 内部的 _throttle 控制

            # --- 3. 更新任务状态为成功 ---
            config_session.query(SyncTask).filter_by(id=task_config.id).update(
                {"sync_status": 'idle', "last_sync_time": datetime.now(TZ_UTC_8)}
            )
            config_session.commit()
            logger.info(f"任务 {table_name} 全量同步成功完成。")

        except Exception as e:
            # --- 4. 处理同步过程中的任何异常 ---
            try:
                config_session.rollback()
            except Exception as rb_err:
                logger.error(f"回滚 config_session 时出错: {rb_err}")

            # 更新任务状态为失败
            try:
                # 确保使用一个新的 session 或现有 session（如果仍然可用）来更新状态
                if not config_session.is_active:
                    config_session = ConfigSession()  # 创建新会话
                    logger.warning("Config session was inactive, created a new one to update failure status.")

                config_session.query(SyncTask).filter_by(id=task_config.id).update(
                    {"sync_status": 'error', "last_sync_time": sync_start_time}  # 使用开始时间标记失败时间点
                )
                config_session.commit()
                logger.info(f"任务 {table_name} 状态已更新为 error。")
            except Exception as e_update:
                logger.error(f"更新任务 {table_name} 失败状态时出错: {e_update}", exc_info=True)
                try:
                    config_session.rollback()  # 回滚状态更新的尝试
                except Exception:
                    pass

            logger.error(f"任务 {table_name} 同步失败: {e}", exc_info=True)
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
