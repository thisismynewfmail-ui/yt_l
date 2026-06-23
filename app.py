import os
import sys
import json
from flask import Flask, send_from_directory

sys.path.insert(0, os.path.dirname(__file__))

from models import db
from core.download_manager import DownloadManager
from core.scheduler import DownloadScheduler
from api.routes import api_bp, init_routes
from api.sse import LogBroadcaster
import config


def create_app():
    app = Flask(__name__, static_folder=None)
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

    db.init_db()

    broadcaster = LogBroadcaster()

    def log_callback(download_id, level, message):
        broadcaster.log(download_id, level, message)

    cfg = config.load_config()
    manager = DownloadManager(db, cfg, log_callback=log_callback)

    init_routes(manager, db, broadcaster, config)

    scheduler = DownloadScheduler(manager, db, config)
    scheduler.start()

    app.register_blueprint(api_bp, url_prefix='/api')

    static_dir = os.path.join(os.path.dirname(__file__), 'static')

    @app.route('/')
    def index():
        return send_from_directory(static_dir, 'index.html')

    @app.route('/css/<path:filename>')
    def serve_css(filename):
        return send_from_directory(os.path.join(static_dir, 'css'), filename)

    @app.route('/js/<path:filename>')
    def serve_js(filename):
        return send_from_directory(os.path.join(static_dir, 'js'), filename)

    manager.start_dispatcher()

    app._download_manager = manager
    app._scheduler = scheduler

    return app


if __name__ == '__main__':
    app = create_app()
    print("Starting YT-DLP Downloader v2...")
    port = int(os.environ.get('PORT', 8080))
    print(f"Open http://localhost:{port} in your browser")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
