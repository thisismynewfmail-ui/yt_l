import glob
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'yt-dlp-mastercode'))

# Substrings yt-dlp prints (via the logger) when it skips a video because it is
# already recorded in the download archive.
ARCHIVE_SKIP_SIGNALS = (
    'has already been recorded in the archive',
    'has already been downloaded',
)


class BlockingError(Exception):
    """Raised to abort a run when a blocking error (YouTube bot-check /
    rate-limit / network failure) is seen, so we stop churning through the
    rest of the playlist on a flagged IP and retry on a fresh proxy instead."""


class LogAdapter:
    """Bridges yt-dlp's logger into our DB/SSE log, watches for archive-skip
    notices (to count re-checked videos), and forwards yt-dlp ERROR lines to
    the worker so blocking errors swallowed by ``ignoreerrors`` can still be
    detected and acted on."""

    def __init__(self, db, download_id, log_callback=None,
                 on_archive_skip=None, on_error=None):
        self.db = db
        self.download_id = download_id
        self.log_callback = log_callback
        self.on_archive_skip = on_archive_skip
        self.on_error = on_error

    def debug(self, msg):
        if any(sig in msg for sig in ARCHIVE_SKIP_SIGNALS):
            if self.on_archive_skip:
                self.on_archive_skip(msg)
        if msg.startswith('[download]'):
            self._log('debug', msg)

    def warning(self, msg):
        self._log('warning', msg)

    def error(self, msg):
        self._log('error', msg)
        if self.on_error:
            # May raise BlockingError to abort the run early.
            self.on_error(msg)

    def _log(self, level, msg):
        self.db.add_log(self.download_id, level, msg)
        if self.log_callback:
            self.log_callback(self.download_id, level, msg)


class DownloadWorker(threading.Thread):
    def __init__(self, entry, config, engine_manager, error_handler, db,
                 proxy_manager=None, log_callback=None):
        super().__init__(daemon=True)
        self.entry = entry
        self.download_id = entry['id']
        self.config = config
        self.engine_manager = engine_manager
        self.error_handler = error_handler
        self.proxy_manager = proxy_manager
        self.db = db
        self.log_callback = log_callback
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._active_proxy = None
        # Track which videos we've already tallied so multi-file downloads
        # (separate video+audio) don't double-count a single video.
        self._counted_done = set()
        self._counted_failed = set()
        # Set when a blocking error (bot-check / rate-limit / network) is seen
        # so we abort and retry on a fresh proxy instead of skipping the video.
        self._blocking_error = None
        self._abort_raised = False

    def stop(self):
        self._stop_event.set()
        self._pause_event.set()

    def pause(self):
        self._pause_event.clear()

    def resume(self):
        self._pause_event.set()

    def _current_proxy(self):
        if self.proxy_manager is None:
            return None
        try:
            return self.proxy_manager.get_proxy()
        except Exception:
            return None

    def run(self):
        try:
            self.db.set_status(self.download_id, 'extracting')
            self._log('info', f'Starting download: {self.entry["url"]}')

            self._pre_fetch_metadata()

            entry = self.db.get_download(self.download_id)
            # Item may have been removed, paused, or already finished while we
            # were extracting metadata.
            if not entry or entry['status'] in ('paused', 'completed'):
                return
            if self._stop_event.is_set():
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
                'no_color': True,
                'no_warnings': True,
                'extract_flat': 'in_playlist',
                'ignoreerrors': True,
            }
            proxy = self._current_proxy()
            if proxy:
                opts['proxy'] = proxy

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
        from config import get_ydl_opts

        entry = self.db.get_download(self.download_id)
        proxy = self._current_proxy()
        self._active_proxy = proxy
        self._blocking_error = None
        self._abort_raised = False

        # Start this pass's per-item tallies from zero. Each run re-processes the
        # playlist from the top, so without this an automatic retry (after a
        # bot-check / proxy switch) would ADD a second pass's archive-skips and
        # downloads on top of the aborted attempt -- inflating the counts and
        # making skipped videos look downloaded. retry_count / proxy_rotations
        # are deliberately NOT reset here (they bound the cross-run retry loop).
        self.db.reset_progress_counters(self.download_id)
        self._counted_done.clear()
        self._counted_failed.clear()

        if proxy:
            self._log('info', f'Routing download through proxy: {proxy}')

        opts = get_ydl_opts(entry, self.config, proxy=proxy)
        opts['progress_hooks'] = [self._on_progress]
        opts['logger'] = LogAdapter(self.db, self.download_id, self.log_callback,
                                    on_archive_skip=self._on_archive_skip,
                                    on_error=self._on_engine_error)

        try:
            self.engine_manager.register_worker()
            try:
                ydl = self.engine_manager.create_engine(opts)
                ydl.download([self.entry['url']])
            finally:
                self.engine_manager.unregister_worker()

            # ignoreerrors lets a playlist "finish" even though a video hit a
            # blocking error (e.g. YouTube's bot-check) that was logged and
            # skipped. Do NOT mark the item completed in that case -- switch
            # proxy and retry so the skipped video is actually re-attempted.
            if self._blocking_error:
                self._handle_failure(entry, self._blocking_error)
                return

            if proxy and self.proxy_manager:
                self.proxy_manager.report_success(proxy)

            self.db.set_status(self.download_id, 'completed', current_video=None,
                               current_speed=None, current_eta=None)
            self._log('info', 'Download completed successfully')

        except BlockingError as e:
            # Early-aborted on the first blocking error -- retry on a new proxy.
            self._handle_failure(entry, self._blocking_error or str(e))
        except Exception as e:
            error_msg = str(e)
            # A pause/hotswap or user-cancel is a controlled interruption.
            if 'hotswap' in error_msg or self._stop_event.is_set():
                return
            self._handle_failure(entry, self._blocking_error or error_msg)

    def _handle_failure(self, entry, msg):
        """Route a failed run: switch proxy on blocking errors, then ask the
        error handler whether to retry (and how soon) or give up."""
        etype = self.error_handler.detect_error_type(msg)
        proxy_switched = False
        if self.proxy_manager is not None and etype in ('rate_limit', 'network_error'):
            # A YouTube bot-check (flagged IP) or a broken/unreachable proxy is
            # worthless going forward -- mark it bad outright and rotate off it.
            hard = self._is_bot_check(msg) or self._is_proxy_error(msg)
            new_proxy = self.proxy_manager.trigger(
                self._short_reason(msg), failed_proxy=self._active_proxy, hard=hard)
            proxy_switched = new_proxy is not None

        # If videos were downloaded this run AND the archive is on (so a retry
        # resumes instead of re-downloading), the connection is making progress
        # -- keep retrying with fresh proxies rather than burning the retry
        # budget on a long playlist.
        archive_on = str(self.config.get('archive_enabled', 'true')) == 'true'
        progressed = archive_on and len(self._counted_done) > 0
        result = self.error_handler.handle_error(
            self.download_id, msg, entry,
            proxy_switched=proxy_switched, progressed=progressed)
        if result == 'failed':
            self._cleanup_part_files()
        return result

    @staticmethod
    def _is_bot_check(msg):
        low = str(msg).lower()
        return ('not a bot' in low or 'sign in to confirm' in low
                or 'unusual traffic' in low or 'automated queries' in low
                or 'captcha' in low or 'verify you' in low)

    @staticmethod
    def _is_proxy_error(msg):
        low = str(msg).lower()
        return ('unable to connect to proxy' in low or 'cannot connect to proxy' in low
                or 'wrong_version_number' in low or 'your proxy appears' in low
                or 'tunnel connection failed' in low or 'proxyerror' in low)

    @staticmethod
    def _short_reason(msg):
        low = str(msg).lower()
        if ('not a bot' in low or 'sign in to confirm' in low
                or 'unusual traffic' in low or 'captcha' in low
                or 'automated queries' in low or 'verify you' in low):
            return 'youtube bot-check'
        if ('proxy' in low or 'wrong_version_number' in low):
            return 'broken proxy connection'
        if 'http error 429' in low or 'too many requests' in low:
            return 'rate limit (429)'
        return 'network/blocking error'

    def _on_engine_error(self, msg):
        """Called for every yt-dlp ERROR line (even ones ignoreerrors swallows).

        Records the first blocking error and aborts the run early so we don't
        keep hammering a flagged IP for the rest of the playlist."""
        etype = self.error_handler.detect_error_type(msg)
        if etype in ('rate_limit', 'network_error'):
            if self._blocking_error is None:
                self._blocking_error = msg
            if not self._abort_raised:
                self._abort_raised = True
                raise BlockingError(msg)

    def _on_archive_skip(self, msg):
        self.db.increment_download(self.download_id, archived_videos=1)

    def _on_progress(self, d):
        if self._stop_event.is_set():
            raise Exception("Download cancelled by user")

        self._pause_event.wait()

        # If a blocking error was seen but couldn't abort from the logger (e.g.
        # the engine swallowed it), abort now that a hook is firing.
        if self._abort_raised:
            raise BlockingError(self._blocking_error or 'blocking error')

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
            # A blocking error (bot-check / rate-limit / network) already flagged
            # this run for abort -- do NOT tally anything as completed, so a
            # video that was really skipped/blocked never counts as downloaded.
            if self._abort_raised or self._blocking_error:
                return
            # Count the parent video once, even though separate video/audio
            # streams each emit their own 'finished' event.
            info = d.get('info_dict', {})
            key = info.get('id') or info.get('display_id') or d.get('filename')
            if key not in self._counted_done:
                self._counted_done.add(key)
                self.db.increment_download(self.download_id, completed_videos=1)
            self.db.update_download(self.download_id, current_speed=None, current_eta=None)
            self._log('info', f'Finished: {info.get("title", "video")}')

        elif status == 'error':
            info = d.get('info_dict', {})
            key = info.get('id') or info.get('display_id') or d.get('filename')
            if key not in self._counted_failed:
                self._counted_failed.add(key)
                self.db.increment_download(self.download_id, failed_videos=1)

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
