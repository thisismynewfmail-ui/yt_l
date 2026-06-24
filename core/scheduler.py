import time
import threading
from datetime import datetime


class DownloadScheduler:
    def __init__(self, download_manager, db, config):
        self.manager = download_manager
        self.db = db
        self.config = config
        self._scheduler_thread = None
        self._running = False

    def start(self):
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._loop, daemon=True)
        self._scheduler_thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                cfg = self.config.load_config()
                if cfg.get('scheduled_restart_enabled') == 'true':
                    hour = int(cfg.get('scheduled_restart_hour', 3))
                    minute = int(cfg.get('scheduled_restart_minute', 0))
                    now = datetime.utcnow()
                    if now.hour == hour and now.minute == minute:
                        self._do_scheduled_restart()
                        time.sleep(60)
            except Exception:
                pass
            time.sleep(30)

    def _do_scheduled_restart(self):
        # Re-queue the whole archive from the top for a fresh nightly pass.
        # (Previously this called manager.resume_all(), which did not exist and
        # raised AttributeError, so scheduled restarts silently never ran.)
        try:
            self.manager.scheduled_restart()
        except Exception:
            pass
