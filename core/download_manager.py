import glob
import os
import threading
import time
from core.engine_manager import EngineManager
from core.error_handler import ErrorHandler
from core.download_worker import DownloadWorker


class DownloadManager:
    def __init__(self, db, config, log_callback=None):
        self.db = db
        self.config = config
        self.log_callback = log_callback
        self.engine_manager = EngineManager()
        self.error_handler = ErrorHandler(db, log_callback)
        self._workers = {}
        self._workers_lock = threading.Lock()
        self._dispatcher_thread = None
        self._dispatcher_running = False

    def _get_max_concurrent(self):
        return int(self.config.get('max_concurrent', 1))

    def _get_restart_delay(self):
        return int(self.config.get('restart_delay', 300))

    def start_dispatcher(self):
        if self._dispatcher_running:
            return
        self._dispatcher_running = True
        self._dispatcher_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher_thread.start()

    def _dispatch_loop(self):
        while self._dispatcher_running:
            try:
                max_concurrent = self._get_max_concurrent()

                active_count = sum(
                    1 for w in self._workers.values() if w.is_alive()
                )

                if active_count < max_concurrent:
                    queued = self.db.get_downloads_by_status('queued')
                    for entry in queued:
                        if active_count >= max_concurrent:
                            break
                        if self.engine_manager.update_in_progress:
                            break
                        self._start_worker(entry)
                        active_count += 1

                if active_count == 0 and not queued:
                    self._check_requeue_loop()

            except Exception:
                pass
            time.sleep(2)

    def _check_requeue_loop(self):
        all_downloads = self.db.get_all_downloads()
        if not all_downloads:
            return

        has_active = any(
            d['status'] in ('queued', 'downloading', 'extracting', 'paused', 'rate_limited')
            for d in all_downloads
        )
        if has_active:
            return

        restart_delay = self._get_restart_delay()
        self._log(None, 'info', f'All downloads complete. Re-checking queue in {restart_delay} seconds...')
        time.sleep(restart_delay)

        if not self._dispatcher_running:
            return

        all_downloads = self.db.get_all_downloads()
        requeued = 0
        for entry in all_downloads:
            if entry['status'] in ('completed', 'failed'):
                self.db.update_download(
                    entry['id'],
                    status='queued',
                    retry_count=0,
                    error_message=None,
                    current_video=None,
                    current_speed=None,
                    current_eta=None,
                )
                requeued += 1

        if requeued > 0:
            self._log(None, 'info', f'Re-queued {requeued} entries for re-check')

    def _start_worker(self, entry):
        download_id = entry['id']
        with self._workers_lock:
            if download_id in self._workers and self._workers[download_id].is_alive():
                return

        worker = DownloadWorker(
            entry=entry,
            config=self.config,
            engine_manager=self.engine_manager,
            error_handler=self.error_handler,
            db=self.db,
            log_callback=self.log_callback,
        )

        with self._workers_lock:
            self._workers[download_id] = worker

        worker.start()
        self.db.set_status(download_id, 'downloading')

    def add_download(self, url, download_dir=None):
        download_id = self.db.add_download(url, download_dir)
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
                if worker and worker.is_alive():
                    worker.resume()
                    self.db.set_status(download_id, 'downloading')
                    self._log(download_id, 'info', 'Download resumed')
                else:
                    self.db.set_status(download_id, 'queued')
                    self._log(download_id, 'info', 'Re-queued for download')

        self.error_handler.cancel_retry(download_id)

    def remove_download(self, download_id):
        entry = self.db.get_download(download_id)

        with self._workers_lock:
            worker = self._workers.get(download_id)
            if worker and worker.is_alive():
                worker.stop()
                worker.join(timeout=5)
            self._workers.pop(download_id, None)

        self.error_handler.cancel_retry(download_id)

        if entry:
            output_dir = entry.get('download_dir') or self.config.get('output_dir', '')
            if output_dir and os.path.isdir(output_dir):
                for part_file in glob.glob(os.path.join(output_dir, '**', '*.part'), recursive=True):
                    try:
                        os.remove(part_file)
                    except OSError:
                        pass

        self.db.delete_download(download_id)

    def retry_download(self, download_id):
        entry = self.db.get_download(download_id)
        if not entry:
            return

        self.db.update_download(
            download_id,
            status='queued',
            retry_count=0,
            error_message=None
        )
        self._log(download_id, 'info', 'Retrying download')

    def pause_all(self):
        downloads = self.db.get_all_downloads()
        for entry in downloads:
            if entry['status'] in ('downloading', 'extracting', 'queued'):
                self.pause_download(entry['id'])

    def resume_first(self):
        downloads = self.db.get_all_downloads()
        for entry in downloads:
            if entry['status'] in ('paused', 'failed', 'rate_limited'):
                self.resume_download(entry['id'])
                self._log(entry['id'], 'info', 'Resumed as next in queue')
                return

    def trigger_engine_update(self):
        def _do_update():
            success, msg = self.engine_manager.trigger_update(log_callback=self.log_callback)
            if success:
                self._restart_workers_after_update()

        thread = threading.Thread(target=_do_update, daemon=True)
        thread.start()
        return True, "Update started"

    def _restart_workers_after_update(self):
        downloads = self.db.get_all_downloads()
        for entry in downloads:
            if entry['status'] in ('paused', 'downloading'):
                self.db.set_status(entry['id'], 'queued')

    def get_stats(self):
        return self.db.get_stats()

    def _log(self, download_id, level, message):
        self.db.add_log(download_id, level, message)
        if self.log_callback:
            self.log_callback(download_id, level, message)
