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
    # Proxy system. mode: off | auto | always.
    # In 'auto' mode the proxy engages itself on rate-limits/errors and turns
    # back off after proxy_active_seconds of not being needed.
    'proxy_mode': 'off',
    'proxy_active_seconds': '600',
    # Optional explicit proxy list (newline/comma separated, scheme://host:port).
    # Leave blank to auto-source a wide pool of public proxies.
    'proxy_list': '',
}


def load_config():
    config = dict(DEFAULTS)
    db_config = db.get_all_config()
    config.update(db_config)
    return config


def save_config(values):
    for key, value in values.items():
        db.set_config(key, str(value))


def get_ydl_opts(entry, config, proxy=None):
    output_dir = entry.get('download_dir') or config.get('output_dir', DEFAULTS['output_dir'])

    archive_path = os.path.join(output_dir, '.archive.txt')

    # Use a per-playlist subfolder for multi-video items. Guard against a NULL
    # title/total_videos (e.g. metadata pre-fetch failed) so we never crash here.
    outtmpl = os.path.join(output_dir, '%(title)s.%(ext)s')
    total_videos = entry.get('total_videos') or 0
    url = entry.get('url') or ''
    if total_videos > 1 or url.endswith('/'):
        outtmpl = os.path.join(output_dir, '%(playlist_title)s', '%(title)s.%(ext)s')

    opts = {
        'outtmpl': outtmpl,
        'format': config.get('format', DEFAULTS['format']),
        'sleep_interval': int(config.get('sleep_interval', 540)),
        'max_sleep_interval': int(config.get('max_sleep_interval', 720)),
        'ignoreerrors': True,
        'noprogress': True,
        'quiet': True,
        'no_color': True,
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

    if proxy:
        opts['proxy'] = proxy

    return opts
