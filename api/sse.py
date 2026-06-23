import json
import queue
import threading
from collections import deque
from datetime import datetime


class LogBroadcaster:
    def __init__(self, max_buffer=5000):
        self._buffer = deque(maxlen=max_buffer)
        self._subscribers = []
        self._lock = threading.Lock()

    def log(self, download_id, level, message):
        entry = {
            'download_id': download_id,
            'level': level,
            'message': message,
            'timestamp': datetime.utcnow().isoformat(),
        }
        with self._lock:
            self._buffer.append(entry)
            for q in self._subscribers:
                try:
                    q.put_nowait(entry)
                except queue.Full:
                    pass

    def get_recent(self, limit=200, download_id=None):
        with self._lock:
            entries = list(self._buffer)
        if download_id:
            entries = [e for e in entries if e['download_id'] == download_id]
        return entries[-limit:]

    def stream(self, download_id=None):
        q = queue.Queue(maxsize=1000)
        with self._lock:
            self._subscribers.append(q)

        try:
            with self._lock:
                for entry in list(self._buffer):
                    if download_id and entry['download_id'] != download_id:
                        continue
                    yield f"data: {json.dumps(entry)}\n\n"

            while True:
                try:
                    entry = q.get(timeout=30)
                    if download_id and entry['download_id'] != download_id:
                        continue
                    yield f"data: {json.dumps(entry)}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with self._lock:
                if q in self._subscribers:
                    self._subscribers.remove(q)
