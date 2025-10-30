from flask import Blueprint, request, jsonify, current_app, abort
from sqlalchemy import text

from app.services import SyncService
from app.models import db, SyncTask

# 1. 创建蓝图
main_bp = Blueprint('main', __name__)


@main_bp.route('/webhook/jdy/<int:task_id>', methods=['POST'])
def handle_jdy_webhook(task_id):
    """
    处理简道云的数据推送 Webhook
    用于实时回写 _id
    URL 示例: https://your-domain.com/webhook/jdy/1
    """

    # (可选) 验证签名
    # signature = request.headers.get('X-Jdy-Signature')
    # if not validate_signature(signature, request.data):
    #     abort(401)

    try:
        payload = request.json
        if not payload:
            return jsonify({"code": 400, "message": "Invalid JSON"}), 400

        op = payload.get('op')
        data = payload.get('data')

        if not op or not data:
            return jsonify({"code": 400, "message": "Missing 'op' or 'data'"}), 400

        # 我们只关心创建和更新操作，因为它们会返回 _id
        if op in ('data_create', 'data_update'):
            jdy_id = data.get('_id')
            if not jdy_id:
                current_app.logger.warning(f"[Webhook {task_id}] Received {op} but no _id found in payload.")
                return jsonify({"code": 202, "message": "No _id, accepted."})

            # 实例化服务
            sync_service = SyncService()

            # 获取任务配置
            task = sync_service.get_task(task_id)
            if not task:
                current_app.logger.error(f"[Webhook {task_id}] Task not found.")
                return jsonify({"code": 404, "message": "Task not found"}), 404

            pk_field_name = task.pk_field_name

            # 从 data 中提取业务主键的值
            # 我们假设简道云字段名 (jdy_field) 与源表字段名 (source_field/pk_field_name) 相同
            # 这是一个强假设，在 FieldMappingService 中已体现
            business_pk_value = data.get(pk_field_name)

            if not business_pk_value:
                current_app.logger.warning(
                    f"[Webhook {task_id}] Received _id={jdy_id} but PK field '{pk_field_name}' not found in payload.")
                return jsonify({"code": 202, "message": "PK not found, accepted."})

            # 执行回写
            sync_service.update_id_from_webhook(task_id, business_pk_value, jdy_id)

            return jsonify({"code": 200, "message": "Webhook processed"})

        else:
            # data_remove 或其他操作
            return jsonify({"code": 200, "message": "Operation skipped"})

    except Exception as e:
        current_app.logger.error(f"[Webhook {task_id}] Error processing webhook: {e}")
        # 在 app 上下文之外，我们不能调用 log_sync_error
        # 但 Webhook 路由总是在 app 上下文中的
        try:
            from app.utils import log_sync_error
            log_sync_error(task_id, f"Webhook 处理失败: {e}", request.json)
        except:
            pass  # 避免循环错误

        return jsonify({"code": 500, "message": "Internal Server Error"}), 500


@main_bp.route('/health', methods=['GET'])
def health_check():
    """
    健康检查端点
    """
    try:
        # 检查配置数据库
        db.session.execute(text("SELECT 1"))
        # 检查源数据库
        db.session.get_bind('source_db').connect().execute(text("SELECT 1"))
        return jsonify({"status": "ok", "databases": "connected"})
    except Exception as e:
        current_app.logger.error(f"Health check failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 503
