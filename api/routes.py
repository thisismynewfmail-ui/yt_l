from flask import Blueprint, request, jsonify, Response
import json
import threading

api_bp = Blueprint('api', __name__)

_manager = None
_db = None
_broadcaster = None
_config = None


def init_routes(manager, db, broadcaster, config):
    global _manager, _db, _broadcaster, _config
    _manager = manager
    _db = db
    _broadcaster = broadcaster
    _config = config


@api_bp.route('/downloads', methods=['GET'])
def list_downloads():
    downloads = _db.get_all_downloads()
    return jsonify(downloads)


@api_bp.route('/downloads', methods=['POST'])
def add_download():
    data = request.get_json()
    if not data or not data.get('url'):
        return jsonify({'error': 'URL is required'}), 400

    url = data['url'].strip()
    download_dir = (data.get('download_dir') or '').strip() or None

    download_id = _manager.add_download(url, download_dir)
    return jsonify({'id': download_id, 'message': 'Added to queue'}), 201


@api_bp.route('/downloads/<int:download_id>', methods=['GET'])
def get_download(download_id):
    entry = _db.get_download(download_id)
    if not entry:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(entry)


@api_bp.route('/downloads/<int:download_id>', methods=['PATCH'])
def update_download(download_id):
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    entry = _db.get_download(download_id)
    if not entry:
        return jsonify({'error': 'Not found'}), 404

    action = data.get('action')

    if action == 'pause':
        _manager.pause_download(download_id)
    elif action == 'resume':
        _manager.resume_download(download_id)
    elif action == 'retry':
        _manager.retry_download(download_id)
    elif 'download_dir' in data:
        _db.update_download(download_id, download_dir=data['download_dir'] or None)

    return jsonify({'message': 'Updated'})


@api_bp.route('/downloads/<int:download_id>', methods=['DELETE'])
def delete_download(download_id):
    _manager.remove_download(download_id)
    return jsonify({'message': 'Removed'})


@api_bp.route('/config', methods=['GET'])
def get_config():
    return jsonify(_config.load_config())


@api_bp.route('/config', methods=['PUT'])
def update_config():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    _config.save_config(data)

    current = _config.load_config()
    if _manager:
        _manager.update_config(current)

    return jsonify({'message': 'Config updated', 'config': current})


@api_bp.route('/engine/status', methods=['GET'])
def engine_status():
    return jsonify({
        'version': _manager.engine_manager.version,
        'update_in_progress': _manager.engine_manager.update_in_progress,
    })


@api_bp.route('/engine/update', methods=['POST'])
def engine_update():
    success, msg = _manager.trigger_engine_update()
    return jsonify({'message': msg, 'success': success})


@api_bp.route('/stats', methods=['GET'])
def get_stats():
    return jsonify(_manager.get_stats())


@api_bp.route('/proxy/status', methods=['GET'])
def proxy_status():
    pm = _manager.proxy_manager
    status = pm.status()
    status['pool'] = pm.list_pool(limit=50)
    status['favorites_list'] = pm.list_favorites(limit=50)
    return jsonify(status)


@api_bp.route('/proxy/mode', methods=['POST'])
def proxy_set_mode():
    data = request.get_json() or {}
    mode = str(data.get('mode', '')).lower()
    if mode not in ('off', 'auto', 'always'):
        return jsonify({'error': 'mode must be off, auto, or always'}), 400
    # Persist so the choice survives a restart, then apply at runtime.
    _config.save_config({'proxy_mode': mode})
    _manager.update_config(_config.load_config())
    return jsonify({'message': f'Proxy mode set to {mode}', 'status': _manager.proxy_manager.status()})


@api_bp.route('/proxy/refresh', methods=['POST'])
def proxy_refresh():
    pm = _manager.proxy_manager
    # Sourcing + probing is slow; do it off the request thread.
    threading.Thread(target=lambda: pm.refresh_pool(test=True), daemon=True).start()
    return jsonify({'message': 'Refreshing proxy pool in the background'}), 202


@api_bp.route('/proxy/test', methods=['POST'])
def proxy_test():
    pm = _manager.proxy_manager
    threading.Thread(target=lambda: pm.health_check(sample=40), daemon=True).start()
    return jsonify({'message': 'Health-checking proxies in the background'}), 202


@api_bp.route('/proxy/deactivate', methods=['POST'])
def proxy_deactivate():
    _manager.proxy_manager.deactivate('manual')
    return jsonify({'message': 'Proxy system disengaged', 'status': _manager.proxy_manager.status()})


@api_bp.route('/queue/restart-all', methods=['POST'])
def restart_all():
    _manager.restart_all()
    return jsonify({'message': 'Re-queued all finished items from the top'})


@api_bp.route('/logs/recent', methods=['GET'])
def recent_logs():
    limit = request.args.get('limit', 200, type=int)
    download_id = request.args.get('download_id', type=int)
    logs = _broadcaster.get_recent(limit=limit, download_id=download_id)
    return jsonify(logs)


@api_bp.route('/logs/stream')
def log_stream():
    download_id = request.args.get('download_id', type=int)
    return Response(
        _broadcaster.stream(download_id=download_id),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        }
    )


@api_bp.route('/scheduler/pause-all', methods=['POST'])
def pause_all():
    _manager.pause_all()
    return jsonify({'message': 'All downloads paused'})


@api_bp.route('/scheduler/resume-all', methods=['POST'])
def resume_all():
    _manager.resume_first()
    return jsonify({'message': 'Next item in queue resumed'})
