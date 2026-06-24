import os
import re
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
    # Default 'auto': normal downloads run on a direct connection, but the
    # moment a rate-limit / YouTube bot-check / network error is hit the proxy
    # system engages itself and rotates to a fresh proxy, then turns back off
    # after proxy_active_seconds of not being needed. This is what makes a
    # bot-check actually switch IPs instead of failing the item.
    'proxy_mode': 'auto',
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


def _uses_playlist_subfolder(entry):
    """Whether this item's videos are written into a per-playlist subfolder.

    Multi-video items (or explicit playlist URLs ending in '/') get their own
    folder; single videos land directly in the output dir. Kept as one helper so
    get_ydl_opts (which builds the download path) and get_media_dir (which scans
    it for already-downloaded videos) can never disagree.
    """
    total_videos = entry.get('total_videos') or 0
    url = entry.get('url') or ''
    return total_videos > 1 or url.endswith('/')


def _sanitize_path_segment(name):
    """Sanitise a single path component the way yt-dlp names folders, so the
    directory we scan matches the one yt-dlp actually wrote to.

    Prefers yt-dlp's own sanitiser (available at runtime, where the engine is on
    sys.path); falls back to a conservative cleanup of the characters yt-dlp
    would never put in a path component when the engine isn't importable.
    """
    if not name:
        return ''
    try:
        from yt_dlp.utils import sanitize_filename
        return sanitize_filename(str(name), restricted=False)
    except Exception:
        return re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', str(name)).strip().strip('.') or ''


def get_media_dir(entry, config, playlist_title=None):
    """Directory where this item's video files are written.

    This is the directory to scan for already-downloaded videos. It mirrors the
    outtmpl built by get_ydl_opts: the base output dir, plus a per-playlist
    subfolder for multi-video items.
    """
    output_dir = entry.get('download_dir') or config.get('output_dir', DEFAULTS['output_dir'])
    if _uses_playlist_subfolder(entry):
        folder = _sanitize_path_segment(playlist_title or entry.get('title') or '')
        if folder:
            return os.path.join(output_dir, folder)
    return output_dir


def get_ydl_opts(entry, config, proxy=None):
    output_dir = entry.get('download_dir') or config.get('output_dir', DEFAULTS['output_dir'])

    archive_path = os.path.join(output_dir, '.archive.txt')

    # Use a per-playlist subfolder for multi-video items. Guard against a NULL
    # title/total_videos (e.g. metadata pre-fetch failed) so we never crash here.
    if _uses_playlist_subfolder(entry):
        outtmpl = os.path.join(output_dir, '%(playlist_title)s', '%(title)s.%(ext)s')
    else:
        outtmpl = os.path.join(output_dir, '%(title)s.%(ext)s')

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
        # Retry the extraction request 3 times (e.g. a flaky proxy connection)
        # before giving up; the worker then rotates to a fresh proxy and
        # re-queues. This is what bounds "try 3 times, then switch proxy".
        'extractor_retries': 3,
        'file_access_retries': 3,
        'skip_unavailable_fragments': True,
    }

    if config.get('archive_enabled', 'true') == 'true':
        opts['download_archive'] = archive_path

    if proxy:
        opts['proxy'] = proxy

    return opts
