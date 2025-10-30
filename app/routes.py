from flask import Blueprint, request, jsonify, current_app, g
from sqlalchemy import text

from app.models import SyncTask
from app.services import SyncService
from app.utils import log_sync_error

# 1. 创建蓝图
main_bp = Blueprint('main', __name__)


@main_bp.route('/webhook/jdy/<int:task_id>', methods=['POST'])
def handle_jdy_webhook(task_id):
    """
    处理简道云的数据推送 Webhook
    用于实时回写 _id
    URL 示例: https://your-domain.com/webhook/jdy/1
    """

    # g.config_session 和 g.source_session 由 app/__init__.py 中的 @before_request 创建
    config_session = g.config_session
    source_session = g.source_session

    task = None
    try:
        payload = request.json
        if not payload:
            return jsonify({"code": 400, "message": "Invalid JSON"}), 400

        op = payload.get('op')
        data = payload.get('data')

        if not op or not data:
            return jsonify({"code": 400, "message": "Missing 'op' or 'data'"}), 400

        # 从 g.config_session 获取任务
        task = config_session.get(SyncTask, task_id)
        if not task:
            current_app.logger.error(f"[Webhook {task_id}] Task not found.")
            return jsonify({"code": 404, "message": "Task not found"}), 404

        # 我们只关心创建和更新操作
        if op in ('data_create', 'data_update'):
            jdy_id = data.get('_id')
            if not jdy_id:
                current_app.logger.warning(f"[Webhook {task_id}] Received {op} but no _id found.")
                return jsonify({"code": 202, "message": "No _id, accepted."})

            # 实例化服务
            sync_service = SyncService()

            pk_field_name = task.pk_field_name

            # 字段映射服务现在需要会话
            alias_map = sync_service.mapping_service.get_alias_mapping(config_session, task.task_id)
            if pk_field_name not in alias_map:
                msg = f"PK field '{pk_field_name}' not in alias_map for webhook."
                current_app.logger.error(f"[Webhook {task_id}] {msg}")
                # 使用新的 log_sync_error
                log_sync_error(task_config=task, error=Exception(msg), payload=payload)
                return jsonify({"code": 500, "message": msg}), 500

            pk_field_alias = alias_map[pk_field_name]
            # V5: 'value' 可能在也可能不在，取决于字段类型
            business_pk_value_data = data.get(pk_field_alias)

            # 检查 business_pk_value_data 是否为字典 (例如 { "value": "PK_001" } )
            if isinstance(business_pk_value_data, dict):
                business_pk_value = business_pk_value_data.get('value')
            else:
                # 否则直接取值 (例如简单的文本字段 "PK_001")
                business_pk_value = business_pk_value_data

            if not business_pk_value:
                current_app.logger.warning(
                    f"[Webhook {task_id}] Received _id={jdy_id} but PK alias '{pk_field_alias}' not found in payload.")
                return jsonify({"code": 202, "message": "PK not found, accepted."})

            # 执行回写 (服务内部会处理视图检查)
            sync_service.update_id_from_webhook(task, business_pk_value, jdy_id)

            return jsonify({"code": 200, "message": "Webhook processed"})

        else:
            # data_remove 或其他操作
            return jsonify({"code": 200, "message": "Operation skipped"})

    except Exception as e:
        current_app.logger.error(f"[Webhook {task_id}] Error processing webhook: {e}")
        # 使用新的 log_sync_error
        # task 可能在 try 块中已成功获取
        log_sync_error(task_config=task, error=e, payload=request.json)

        return jsonify({"code": 500, "message": "Internal Server Error"}), 500


@main_bp.route('/health', methods=['GET'])
def health_check():
    """
    健康检查端点
    """
    try:
        # 检查配置数据库
        g.config_session.execute(text("SELECT 1"))
        # 检查目标数据库
        g.source_session.execute(text("SELECT 1"))
        return jsonify({"status": "ok", "databases": "connected"})
    except Exception as e:
        current_app.logger.error(f"Health check failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 503
