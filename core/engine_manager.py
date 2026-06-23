import importlib
import os
import sys
import subprocess
import threading


class EngineManager:
    def __init__(self):
        self._update_lock = threading.Lock()
        self._update_in_progress = False
        self._workers_done_event = threading.Event()
        self._active_workers = 0
        self._active_workers_lock = threading.Lock()
        self._yt_dlp_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 'yt-dlp-mastercode'
        )
        self._version = self._get_version()

    @property
    def update_in_progress(self):
        return self._update_in_progress

    @property
    def version(self):
        return self._version

    def _get_version(self):
        try:
            sys.path.insert(0, self._yt_dlp_path)
            from yt_dlp.version import __version__
            return __version__
        except Exception:
            return 'unknown'

    def create_engine(self, opts):
        sys.path.insert(0, self._yt_dlp_path)
        from yt_dlp import YoutubeDL
        return YoutubeDL(opts)

    def register_worker(self):
        with self._active_workers_lock:
            self._active_workers += 1

    def unregister_worker(self):
        with self._active_workers_lock:
            self._active_workers -= 1
            if self._update_in_progress and self._active_workers <= 0:
                self._workers_done_event.set()

    def should_stop(self):
        return self._update_in_progress

    def trigger_update(self, log_callback=None):
        with self._update_lock:
            if self._update_in_progress:
                return False, "Update already in progress"

            self._update_in_progress = True
            self._workers_done_event.clear()

            if log_callback:
                log_callback('info', 'Engine update requested. Waiting for active downloads to finish current video...')

            with self._active_workers_lock:
                if self._active_workers > 0:
                    if log_callback:
                        log_callback('info', f'Waiting for {self._active_workers} active worker(s) to finish...')
                    self._workers_done_event.wait(timeout=600)

            if log_callback:
                log_callback('info', 'Pulling latest engine from repository...')

            try:
                result = subprocess.run(
                    ['git', '-C', self._yt_dlp_path, 'pull'],
                    capture_output=True, text=True, timeout=120
                )
                if result.returncode != 0:
                    raise RuntimeError(f"git pull failed: {result.stderr}")

                output = result.stdout.strip()
                if log_callback:
                    log_callback('info', f'Git pull result: {output}')

                mods_to_clear = [k for k in sys.modules if k.startswith('yt_dlp')]
                for mod in mods_to_clear:
                    del sys.modules[mod]
                importlib.invalidate_caches()

                self._version = self._get_version()

                if log_callback:
                    log_callback('info', f'Engine updated successfully. New version: {self._version}')

                return True, f"Updated to {self._version}"

            except Exception as e:
                if log_callback:
                    log_callback('error', f'Engine update failed: {e}')
                return False, str(e)
            finally:
                self._update_in_progress = False
