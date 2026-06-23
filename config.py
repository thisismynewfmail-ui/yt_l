import os
import json
from models import db

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')

DEFAULTS = {
    'sleep_interval': '540',
    'max_sleep_interval': '720',
    'format': 'bestvideo[height<=720][ext=mp4][vcodec^=avc]+bestaudio[ext=m4a]/bestvideo[height<=720]+bestaudio',
    'output_dir': os.path.join(os.path.dirname(__file__), 'downloads'),
    'archive_enabled': 'true',
    'max_concurrent': '1',
    'restart_delay': '300',
    'scheduled_restart_enabled': 'false',
    'scheduled_restart_hour': '3',
    'scheduled_restart_minute': '0',
}


def load_config():
    config = dict(DEFAULTS)
    db_config = db.get_all_config()
    config.update(db_config)
    return config


def save_config(values):
    for key, value in values.items():
        db.set_config(key, str(value))


def get_ydl_opts(entry, config):
    output_dir = entry.get('download_dir') or config.get('output_dir', DEFAULTS['output_dir'])
    playlist_title = entry.get('title', 'untitled')
    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in playlist_title)

    archive_path = os.path.join(output_dir, '.archive.txt')

    outtmpl = os.path.join(output_dir, '%(title)s.%(ext)s')
    if entry.get('total_videos', 0) > 1 or entry.get('url', '').endswith('/'):
        outtmpl = os.path.join(output_dir, '%(playlist_title)s', '%(title)s.%(ext)s')

    opts = {
        'outtmpl': outtmpl,
        'format': config.get('format', DEFAULTS['format']),
        'sleep_interval': int(config.get('sleep_interval', 540)),
        'max_sleep_interval': int(config.get('max_sleep_interval', 720)),
        'ignoreerrors': True,
        'noprogress': True,
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
        'js_runtimes': {'node': {}},
        'remote_components': {'ejs:github'},
        'retries': 10,
        'fragment_retries': 10,
        'file_access_retries': 3,
        'skip_unavailable_fragments': True,
    }

    if config.get('archive_enabled', 'true') == 'true':
        opts['download_archive'] = archive_path

    return opts
