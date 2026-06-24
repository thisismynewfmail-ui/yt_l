import threading
import time
from datetime import datetime
from core.engine_manager import EngineManager
from core.error_handler import ErrorHandler
from core.proxy_manager import ProxyManager
from core.download_worker import DownloadWorker

# Statuses that mean an item is still "in play" and should block the
# everything-is-done restart sweep.
ACTIVE_STATUSES = ('queued', 'downloading', 'extracting', 'paused', 'rate_limited')


class DownloadManager:
    def __init__(self, db, config, log_callback=None):
        self.db = db
        self.config = config
        self.log_callback = log_callback
        self.engine_manager = EngineManager()
        self.proxy_manager = ProxyManager(config=config, log_callback=log_callback)
        self.error_handler = ErrorHandler(db, log_callback, proxy_manager=self.proxy_manager)
        self._workers = {}
        self._workers_lock = threading.Lock()
        self._dispatcher_thread = None
        self._dispatcher_running = False
        # Timestamp (monotonic) at which the queue first became fully idle, used
        # to schedule the non-blocking "restart from the top" sweep.
        self._idle_since = None

    def _get_max_concurrent(self):
        try:
            return max(1, int(self.config.get('max_concurrent', 1)))
        except (TypeError, ValueError):
            return 1

    def _get_restart_delay(self):
        try:
            return max(0, int(self.config.get('restart_delay', 300)))
        except (TypeError, ValueError):
            return 300

    def update_config(self, cfg):
        """Apply a freshly-saved config to the manager and subsystems."""
        self.config = cfg
        self.proxy_manager.configure(cfg)

    def recover_orphans(self):
        """Re-queue items left mid-flight by a previous run/crash.

        At startup no workers exist yet, so any row still marked 'downloading'
        or 'extracting' is an orphan (e.g. the process was killed). Put it back
        to 'queued' so it resumes instead of sitting stuck forever.
        """
        recovered = 0
        for entry in self.db.get_all_downloads():
            if entry['status'] in ('downloading', 'extracting'):
                self.db.set_status(entry['id'], 'queued')
                recovered += 1
        if recovered:
            self._log(None, 'info', f'Recovered {recovered} interrupted download(s) from a previous session.')

    def start_dispatcher(self):
        if self._dispatcher_running:
            return
        self.recover_orphans()
        self._dispatcher_running = True
        self._dispatcher_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher_thread.start()

    def _reap_workers(self):
        """Drop references to finished worker threads so the map stays small."""
        with self._workers_lock:
            for did in [d for d, w in self._workers.items() if not w.is_alive()]:
                self._workers.pop(did, None)

    def _active_worker_count(self):
        with self._workers_lock:
            return sum(1 for w in self._workers.values() if w.is_alive())

    def _dispatch_loop(self):
        while self._dispatcher_running:
            try:
                self._reap_workers()
                max_concurrent = self._get_max_concurrent()
                active_count = self._active_worker_count()

                # Don't start new work while the engine is hot-swapping.
                if not self.engine_manager.update_in_progress and active_count < max_concurrent:
                    queued = self.db.get_downloads_by_status('queued')
                    for entry in queued:
                        if active_count >= max_concurrent:
                            break
                        self._start_worker(entry)
                        active_count += 1

                self._maybe_restart_when_idle(active_count)
            except Exception:
                pass
            time.sleep(2)

    def _maybe_restart_when_idle(self, active_count):
        """Non-blocking "restart from the top" once the whole queue is done.

        Unlike a blocking sleep, this lets newly-added URLs start immediately
        and re-evaluates every tick, so a freshly added item resets the timer
        instead of being stalled behind it.
        """
        all_downloads = self.db.get_all_downloads()
        if not all_downloads:
            self._idle_since = None
            return

        busy = active_count > 0 or any(d['status'] in ACTIVE_STATUSES for d in all_downloads)
        if busy:
            self._idle_since = None
            return

        delay = self._get_restart_delay()
        now = time.monotonic()
        if self._idle_since is None:
            self._idle_since = now
            self._log(None, 'info',
                      f'All downloads complete. Re-checking the entire queue from the top in {delay}s...')
            return

        if now - self._idle_since >= delay:
            self._requeue_all_for_recheck()
            self._idle_since = None

    def _requeue_all_for_recheck(self):
        """Re-queue every finished item from the top for a fresh archive pass."""
        requeued = 0
        for entry in self.db.get_all_downloads():
            if entry['status'] in ('completed', 'failed'):
                self.db.reset_progress_counters(
                    entry['id'],
                    status='queued',
                    retry_count=0,
                    proxy_rotations=0,
                    error_message=None,
                    recheck_count=(entry.get('recheck_count') or 0) + 1,
                    last_checked_at=datetime.utcnow().isoformat(),
                )
                requeued += 1
        if requeued:
            self._log(None, 'info',
                      f'Re-queued {requeued} item(s) from the top for re-archive / recheck.')

    def _start_worker(self, entry):
        download_id = entry['id']
        with self._workers_lock:
            existing = self._workers.get(download_id)
            if existing and existing.is_alive():
                return

            worker = DownloadWorker(
                entry=entry,
                config=self.config,
                engine_manager=self.engine_manager,
                error_handler=self.error_handler,
                proxy_manager=self.proxy_manager,
                db=self.db,
                log_callback=self.log_callback,
            )
            self._workers[download_id] = worker

        # Claim the item before the thread spins up so the dispatcher does not
        # pick the same queued row again on its next tick (fixes a race where
        # the worker's own status write lagged a loop iteration).
        self.db.set_status(download_id, 'extracting')
        worker.start()

    def add_download(self, url, download_dir=None):
        download_id = self.db.add_download(url, download_dir)
        # A new item resets the idle restart timer so it starts promptly.
        self._idle_since = None
        self._log(download_id, 'info', f'Added to queue: {url}')
        return download_id

    def pause_download(self, download_id):
        with self._workers_lock:
            worker = self._workers.get(download_id)
            if worker and worker.is_alive():
                worker.pause()

        self.db.set_status(download_id, 'paused')
        self.error_handler.cancel_retry(download_id)
        self._log(download_id, 'info', 'Download paused')

    def resume_download(self, download_id):
        entry = self.db.get_download(download_id)
        if not entry:
            return

        if entry['status'] in ('paused', 'failed', 'rate_limited'):
            with self._workers_lock:
                worker = self._workers.get(download_id)
                alive = worker and worker.is_alive()
            if alive:
                worker.resume()
                self.db.set_status(download_id, 'downloading')
                self._log(download_id, 'info', 'Download resumed')
            else:
                self.db.set_status(download_id, 'queued', error_message=None)
                self._log(download_id, 'info', 'Re-queued for download')

        self.error_handler.cancel_retry(download_id)

    def remove_download(self, download_id):
        """Stop any worker and delete the item from the queue + its logs.

        IMPORTANT: removing a playlist only drops its *queue row* and logs. The
        downloaded video files and the yt-dlp archive (``.archive.txt``) are
        deliberately left untouched on disk, so:
          * the user never loses videos they already downloaded, and
          * if the same playlist is added again, the already-downloaded videos
            are detected (by title -- see DownloadWorker's match_filter) and
            skipped instead of being fetched a second time.

        We also intentionally do *not* sweep ``*.part`` files from the shared
        output directory here -- doing so previously deleted the partial files
        of *other* concurrent downloads. yt-dlp safely resumes from leftover
        .part files, so they are harmless to leave behind.
        """
        with self._workers_lock:
            worker = self._workers.get(download_id)
        if worker and worker.is_alive():
            worker.stop()
            worker.join(timeout=5)
        with self._workers_lock:
            self._workers.pop(download_id, None)

        self.error_handler.cancel_retry(download_id)
        self.db.delete_download(download_id)
        self._log(None, 'info', f'Removed item #{download_id} from the queue')

    def retry_download(self, download_id):
        entry = self.db.get_download(download_id)
        if not entry:
            return

        # Stop a lingering worker before re-queuing so we start clean.
        with self._workers_lock:
            worker = self._workers.get(download_id)
        if worker and worker.is_alive():
            worker.stop()
            worker.join(timeout=5)
        with self._workers_lock:
            self._workers.pop(download_id, None)

        # Reset the per-pass counters so the retried attempt counts from zero
        # rather than carrying over a stale completed/failed/archived tally.
        self.db.reset_progress_counters(
            download_id,
            status='queued',
            retry_count=0,
            proxy_rotations=0,
            error_message=None,
        )
        self._idle_since = None
        self._log(download_id, 'info', 'Retrying download (counters reset)')

    def pause_all(self):
        for entry in self.db.get_all_downloads():
            if entry['status'] in ('downloading', 'extracting', 'queued'):
                self.pause_download(entry['id'])

    def resume_all(self):
        resumed = 0
        for entry in self.db.get_all_downloads():
            if entry['status'] in ('paused', 'failed', 'rate_limited'):
                self.resume_download(entry['id'])
                resumed += 1
        if resumed:
            self._log(None, 'info', f'Resumed {resumed} item(s)')

    def resume_first(self):
        for entry in self.db.get_all_downloads():
            if entry['status'] in ('paused', 'failed', 'rate_limited'):
                self.resume_download(entry['id'])
                self._log(entry['id'], 'info', 'Resumed as next in queue')
                return

    def restart_all(self):
        """Manually restart the whole queue from the top (re-archive pass)."""
        self._requeue_all_for_recheck()
        self._idle_since = None

    def scheduled_restart(self):
        self._log(None, 'info', 'Scheduled restart: re-queuing the entire archive from the top.')
        self._requeue_all_for_recheck()
        self._idle_since = None

    def trigger_engine_update(self):
        def _do_update():
            success, msg = self.engine_manager.trigger_update(log_callback=self.log_callback)
            if success:
                self._restart_workers_after_update()

        thread = threading.Thread(target=_do_update, daemon=True)
        thread.start()
        return True, "Update started"

    def _restart_workers_after_update(self):
        for entry in self.db.get_all_downloads():
            if entry['status'] in ('paused', 'downloading', 'extracting'):
                self.db.set_status(entry['id'], 'queued')

    def get_stats(self):
        stats = self.db.get_stats()
        stats['proxy'] = self.proxy_manager.status()
        return stats

    def _log(self, download_id, level, message):
        self.db.add_log(download_id, level, message)
        if self.log_callback:
            self.log_callback(download_id, level, message)
