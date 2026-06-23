import glob
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'yt-dlp-mastercode'))


class LogAdapter:
    def __init__(self, db, download_id, log_callback=None):
        self.db = db
        self.download_id = download_id
        self.log_callback = log_callback

    def debug(self, msg):
        if msg.startswith('[download]'):
            self._log('debug', msg)
        pass

    def warning(self, msg):
        self._log('warning', msg)

    def error(self, msg):
        self._log('error', msg)

    def _log(self, level, msg):
        self.db.add_log(self.download_id, level, msg)
        if self.log_callback:
            self.log_callback(self.download_id, level, msg)


class DownloadWorker(threading.Thread):
    def __init__(self, entry, config, engine_manager, error_handler, db, log_callback=None):
        super().__init__(daemon=True)
        self.entry = entry
        self.download_id = entry['id']
        self.config = config
        self.engine_manager = engine_manager
        self.error_handler = error_handler
        self.db = db
        self.log_callback = log_callback
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()

    def stop(self):
        self._stop_event.set()
        self._pause_event.set()

    def pause(self):
        self._pause_event.clear()

    def resume(self):
        self._pause_event.set()

    def run(self):
        try:
            self.db.set_status(self.download_id, 'extracting')
            self._log('info', f'Starting download: {self.entry["url"]}')

            self._pre_fetch_metadata()

            entry = self.db.get_download(self.download_id)
            if entry['status'] == 'paused' or entry['status'] == 'completed':
                return

            self.db.set_status(self.download_id, 'downloading')
            self._download()

        except Exception as e:
            self._log('error', f'Worker error: {e}')
            self.db.set_status(self.download_id, 'failed', error_message=str(e))

    def _pre_fetch_metadata(self):
        try:
            opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': 'in_playlist',
                'ignoreerrors': True,
            }

            self.engine_manager.register_worker()
            try:
                ydl = self.engine_manager.create_engine(opts)
                info = ydl.extract_info(self.entry['url'], download=False)

                if info:
                    title = info.get('title', 'Unknown')
                    entries = info.get('entries', [])
                    total = info.get('playlist_count') or len(entries) or 1

                    self.db.update_download(
                        self.download_id,
                        title=title,
                        total_videos=total
                    )
                    self._log('info', f'Found: {title} ({total} videos)')
            finally:
                self.engine_manager.unregister_worker()

        except Exception as e:
            self._log('warning', f'Could not pre-fetch metadata: {e}')
            self.db.update_download(self.download_id, title='Unknown')

    def _download(self):
        try:
            from config import get_ydl_opts

            entry = self.db.get_download(self.download_id)
            opts = get_ydl_opts(entry, self.config)
            opts['progress_hooks'] = [self._on_progress]
            opts['logger'] = LogAdapter(self.db, self.download_id, self.log_callback)

            self.engine_manager.register_worker()
            try:
                ydl = self.engine_manager.create_engine(opts)
                ydl.download([self.entry['url']])

                self.db.set_status(self.download_id, 'completed')
                self._log('info', 'Download completed successfully')

            finally:
                self.engine_manager.unregister_worker()

        except Exception as e:
            error_msg = str(e)
            self._cleanup_part_files()
            result = self.error_handler.handle_error(self.download_id, error_msg, entry)

            if result == 'retry' or result == 'rate_limited':
                pass
            elif result == 'failed':
                pass

    def _on_progress(self, d):
        if self._stop_event.is_set():
            raise Exception("Download cancelled by user")

        self._pause_event.wait()

        if self.engine_manager.update_in_progress:
            self._log('info', 'Engine update in progress. Pausing for hotswap.')
            self.db.set_status(self.download_id, 'paused')
            raise Exception("Paused for engine hotswap")

        status = d.get('status')

        if status == 'downloading':
            speed = d.get('speed')
            eta = d.get('eta')
            downloaded = d.get('downloaded_bytes', 0)
            total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
            title = d.get('info_dict', {}).get('title', 'Unknown')

            self.db.update_download(
                self.download_id,
                current_video=title,
                current_speed=speed,
                current_eta=eta
            )

            if total and total > 0:
                pct = (downloaded / total) * 100
                speed_str = f"{speed / 1024 / 1024:.1f}MB/s" if speed else "N/A"
                eta_str = f"{eta // 60}m{eta % 60}s" if eta else "N/A"
                self._log('debug', f'Downloading: {title} - {pct:.1f}% at {speed_str} ETA {eta_str}')

        elif status == 'finished':
            self.db.update_download(
                self.download_id,
                completed_videos=self.db.get_download(self.download_id).get('completed_videos', 0) + 1,
                current_speed=None,
                current_eta=None
            )
            self._log('info', f'Finished: {d.get("info_dict", {}).get("title", "video")}')

    def _cleanup_part_files(self):
        try:
            output_dir = self.entry.get('download_dir') or self.config.get('output_dir', '')
            if not output_dir or not os.path.isdir(output_dir):
                return
            for part_file in glob.glob(os.path.join(output_dir, '**', '*.part'), recursive=True):
                try:
                    os.remove(part_file)
                    self._log('info', f'Cleaned up partial file: {os.path.basename(part_file)}')
                except OSError:
                    pass
        except Exception:
            pass

    def _log(self, level, message):
        self.db.add_log(self.download_id, level, message)
        if self.log_callback:
            self.log_callback(self.download_id, level, message)
