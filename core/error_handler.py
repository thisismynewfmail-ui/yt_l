import threading
from datetime import datetime, timedelta

RATE_LIMIT_SIGNALS = [
    "http error 429",
    "sign in to confirm",
    "not a bot",
    "confirm you're not a bot",
    "confirm you are not a bot",
    "too many requests",
    "rate limit",
    "please slow down",
    "you are downloading too fast",
]

# Network-level failures that a proxy switch can plausibly get past. Includes
# broken-proxy signals (a dead proxy, or an HTTP-only proxy that can't tunnel
# HTTPS -> "SSL: WRONG_VERSION_NUMBER") so we rotate off it.
NETWORK_SIGNALS = [
    "http error 403",
    "connection reset",
    "connection refused",
    "connection timed out",
    "timed out",
    "temporary failure in name resolution",
    "unable to connect",
    "unable to connect to proxy",
    "cannot connect to proxy",
    "tunnel connection failed",
    "remote end closed connection",
    "unable to download webpage",
    "wrong_version_number",
    "your proxy appears",
    "proxyerror",
]

# Escalating cool-down used when we have NO fresh proxy to switch to and must
# simply wait out the rate limit.
BACKOFF_SCHEDULE = [300, 900, 1800]
# Short delay used when we just switched to a fresh proxy -- retry quickly
# rather than waiting out a limit that the new IP isn't subject to.
PROXY_SWITCH_DELAY = 20


class ErrorHandler:
    def __init__(self, db_module, log_callback=None, proxy_manager=None):
        self.db = db_module
        self.log_callback = log_callback
        self.proxy_manager = proxy_manager
        self._retry_timers = {}

    def detect_error_type(self, error_msg):
        error_lower = str(error_msg).lower()
        for signal in RATE_LIMIT_SIGNALS:
            if signal in error_lower:
                return 'rate_limit'
        for signal in NETWORK_SIGNALS:
            if signal in error_lower:
                return 'network_error'
        return 'download_error'

    def handle_error(self, download_id, error_msg, entry, proxy_switched=False, progressed=False):
        """Decide how to react to a failed run.

        ``proxy_switched`` -> we already rotated to a fresh proxy, so retry soon.
        ``progressed``     -> videos were downloaded this run, so don't spend the
                              retry budget; reset it and keep going.
        """
        error_type = self.detect_error_type(error_msg)
        if error_type in ('rate_limit', 'network_error'):
            return self._handle_blocking(download_id, error_msg, entry,
                                         error_type, proxy_switched, progressed)
        return self._handle_download_error(download_id, error_msg, entry, progressed)

    def _handle_blocking(self, download_id, error_msg, entry, error_type,
                         proxy_switched, progressed):
        retry_count = 0 if progressed else entry.get('retry_count', 0)
        max_retries = entry.get('max_retries', 3)

        if retry_count >= max_retries:
            self.db.set_status(download_id, 'failed',
                               error_message=f"Max retries exceeded: {error_msg}")
            self._log(download_id, 'error',
                      f'Blocking error exceeded max retries ({max_retries}). Marking as failed.')
            return 'failed'

        if proxy_switched:
            delay = PROXY_SWITCH_DELAY
            how = 'switched to a fresh proxy'
        else:
            delay = BACKOFF_SCHEDULE[min(retry_count, len(BACKOFF_SCHEDULE) - 1)]
            how = 'waiting out the limit (no proxy available)'

        self.db.update_download(
            download_id,
            status='rate_limited',
            error_message=error_msg,
            retry_count=retry_count + 1,
        )

        progressed_note = ' (made progress, budget reset)' if progressed else ''
        self._log(download_id, 'warning',
                  f'{error_type.replace("_", " ").title()} -- {how}; retrying in '
                  f'{delay}s (attempt {retry_count + 1}/{max_retries}){progressed_note}')

        self.cancel_retry(download_id)
        timer = threading.Timer(delay, self._retry_callback, args=[download_id])
        timer.daemon = True
        timer.start()
        self._retry_timers[download_id] = timer
        return 'rate_limited'

    def _handle_download_error(self, download_id, error_msg, entry, progressed=False):
        retry_count = 0 if progressed else entry.get('retry_count', 0)
        max_retries = entry.get('max_retries', 3)

        if retry_count >= max_retries:
            self.db.set_status(download_id, 'failed', error_message=error_msg)
            self._log(download_id, 'error', f'Download failed after {max_retries} retries: {error_msg}')
            return 'failed'

        # CRITICAL: put the item back to 'queued' so the dispatcher actually
        # re-runs it. Previously the status was left at 'downloading' with no
        # live worker, so the item became a zombie that never retried.
        self.db.update_download(
            download_id,
            status='queued',
            retry_count=retry_count + 1,
            error_message=error_msg
        )

        self._log(download_id, 'warning',
                  f'Download error (attempt {retry_count + 1}/{max_retries}); re-queued: {error_msg}')
        return 'retry'

    def _retry_callback(self, download_id):
        try:
            entry = self.db.get_download(download_id)
            if entry and entry['status'] == 'rate_limited':
                self.db.set_status(download_id, 'queued', error_message=None)
                self._log(download_id, 'info', 'Cooldown finished. Re-queued for download.')
        except Exception:
            pass
        finally:
            self._retry_timers.pop(download_id, None)

    def cancel_retry(self, download_id):
        timer = self._retry_timers.pop(download_id, None)
        if timer:
            timer.cancel()

    def _log(self, download_id, level, message):
        self.db.add_log(download_id, level, message)
        if self.log_callback:
            self.log_callback(download_id, level, message)
