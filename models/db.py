import sqlite3
import os
import threading
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'app.db')

_local = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    title TEXT DEFAULT NULL,
    download_dir TEXT DEFAULT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    total_videos INTEGER DEFAULT 0,
    completed_videos INTEGER DEFAULT 0,
    failed_videos INTEGER DEFAULT 0,
    current_video TEXT DEFAULT NULL,
    current_speed REAL DEFAULT NULL,
    current_eta INTEGER DEFAULT NULL,
    error_message TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);

CREATE TABLE IF NOT EXISTS log_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    download_id INTEGER DEFAULT NULL,
    level TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_log_created ON log_entries(created_at);

CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def get_conn():
    if not hasattr(_local, 'conn') or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


def init_db():
    conn = get_conn()
    conn.executescript(SCHEMA)
    conn.commit()


def _row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def _rows_to_dicts(rows):
    return [dict(r) for r in rows]


def add_download(url, download_dir=None):
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO downloads (url, download_dir) VALUES (?, ?)",
        (url, download_dir)
    )
    conn.commit()
    return cur.lastrowid


def get_download(download_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM downloads WHERE id = ?", (download_id,)).fetchone()
    return _row_to_dict(row)


def get_all_downloads():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM downloads ORDER BY id ASC").fetchall()
    return _rows_to_dicts(rows)


def update_download(download_id, **kwargs):
    if not kwargs:
        return
    kwargs['updated_at'] = datetime.utcnow().isoformat()
    sets = ', '.join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [download_id]
    conn = get_conn()
    conn.execute(f"UPDATE downloads SET {sets} WHERE id = ?", vals)
    conn.commit()


def delete_download(download_id):
    conn = get_conn()
    conn.execute("DELETE FROM downloads WHERE id = ?", (download_id,))
    conn.execute("DELETE FROM log_entries WHERE download_id = ?", (download_id,))
    conn.commit()


def set_status(download_id, status, **extra):
    update_download(download_id, status=status, **extra)


def get_downloads_by_status(status):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM downloads WHERE status = ? ORDER BY id ASC", (status,)
    ).fetchall()
    return _rows_to_dicts(rows)


def add_log(download_id, level, message):
    conn = get_conn()
    conn.execute(
        "INSERT INTO log_entries (download_id, level, message) VALUES (?, ?, ?)",
        (download_id, level, message)
    )
    conn.commit()


def get_recent_logs(limit=200, download_id=None):
    conn = get_conn()
    if download_id:
        rows = conn.execute(
            "SELECT * FROM log_entries WHERE download_id = ? ORDER BY id DESC LIMIT ?",
            (download_id, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM log_entries ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return _rows_to_dicts(rows)


def get_config(key, default=None):
    conn = get_conn()
    row = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,)).fetchone()
    if row:
        return row['value']
    return default


def set_config(key, value):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
        (key, str(value))
    )
    conn.commit()


def get_all_config():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM app_config").fetchall()
    return {row['key']: row['value'] for row in rows}


def get_stats():
    conn = get_conn()
    row = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) as queued,
            SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END) as downloading,
            SUM(CASE WHEN status = 'extracting' THEN 1 ELSE 0 END) as extracting,
            SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END) as paused,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN status = 'rate_limited' THEN 1 ELSE 0 END) as rate_limited,
            SUM(completed_videos) as total_completed_videos,
            SUM(total_videos) as total_videos_all
        FROM downloads
    """).fetchone()
    return _row_to_dict(row)
