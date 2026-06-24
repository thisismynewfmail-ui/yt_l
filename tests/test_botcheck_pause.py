"""When proxies are OFF and a YouTube bot-check / rate-limit is hit, the error
handler pauses the item for the configured number of seconds and then
auto-resumes, instead of burning the (small) retry budget and failing the item.

This is the proxies-off self-throttle: with no IP to rotate to, the only way
past a bot-check is to stop hammering YouTube and wait, for a duration the user
sets in the settings (``botcheck_pause_seconds``).
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.error_handler import (
    ErrorHandler,
    DEFAULT_BOTCHECK_PAUSE_SECONDS,
    PROXY_SWITCH_DELAY,
    BACKOFF_SCHEDULE,
)

# The exact bot-check line yt-dlp logs (unicode apostrophe included).
BOTCHECK_MSG = ("ERROR: [youtube] 1qUBjkSEoRk: Sign in to confirm you’re not "
                "a bot. Use --cookies-from-browser or --cookies for the authentication.")


class FakeDB:
    """Minimal DB stand-in: records row updates and logs in memory."""
    def __init__(self):
        self.rows = {}
        self.logs = []

    def update_download(self, download_id, **kwargs):
        self.rows.setdefault(download_id, {}).update(kwargs)

    def set_status(self, download_id, status, **extra):
        self.update_download(download_id, status=status, **extra)

    def get_download(self, download_id):
        return self.rows.get(download_id)

    def add_log(self, download_id, level, message):
        self.logs.append((level, message))


class FakeProxyManager:
    def __init__(self, mode='off'):
        self.mode = mode


class BotcheckPauseTest(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.scheduled = []

    def _make_handler(self, mode='off', config=None):
        h = ErrorHandler(self.db, proxy_manager=FakeProxyManager(mode), config=config)
        # Capture the scheduled delay instead of starting a real background timer.
        h._schedule_retry = lambda did, delay: self.scheduled.append((did, delay))
        return h

    def test_botcheck_with_proxies_off_pauses_for_configured_seconds(self):
        h = self._make_handler(mode='off', config={'botcheck_pause_seconds': '42'})
        entry = {'id': 1, 'retry_count': 0, 'max_retries': 3}
        result = h.handle_error(1, BOTCHECK_MSG, entry, proxy_switched=False)

        self.assertEqual(result, 'rate_limited')
        self.assertEqual(self.scheduled, [(1, 42)])
        self.assertEqual(self.db.rows[1]['status'], 'rate_limited')

    def test_pause_is_not_bounded_by_retry_budget(self):
        # Even with retry_count already over max, an off-mode bot-check pauses
        # (and keeps going) rather than marking the item failed.
        h = self._make_handler(mode='off', config={'botcheck_pause_seconds': '30'})
        entry = {'id': 2, 'retry_count': 99, 'max_retries': 3}
        result = h.handle_error(2, BOTCHECK_MSG, entry, proxy_switched=False)

        self.assertEqual(result, 'rate_limited')
        self.assertNotEqual(self.db.rows[2].get('status'), 'failed')
        self.assertEqual(self.scheduled, [(2, 30)])

    def test_default_pause_when_setting_absent(self):
        h = self._make_handler(mode='off', config={})
        entry = {'id': 3, 'retry_count': 0, 'max_retries': 3}
        h.handle_error(3, BOTCHECK_MSG, entry, proxy_switched=False)
        self.assertEqual(self.scheduled, [(3, DEFAULT_BOTCHECK_PAUSE_SECONDS)])

    def test_invalid_pause_falls_back_to_default(self):
        h = self._make_handler(mode='off', config={'botcheck_pause_seconds': 'abc'})
        entry = {'id': 4, 'retry_count': 0, 'max_retries': 3}
        h.handle_error(4, BOTCHECK_MSG, entry, proxy_switched=False)
        self.assertEqual(self.scheduled, [(4, DEFAULT_BOTCHECK_PAUSE_SECONDS)])

    def test_updated_config_changes_pause_length(self):
        # A settings save (update_config) must change the pause used next time.
        h = self._make_handler(mode='off', config={'botcheck_pause_seconds': '60'})
        entry = {'id': 5, 'retry_count': 0, 'max_retries': 3}
        h.handle_error(5, BOTCHECK_MSG, entry, proxy_switched=False)
        h.update_config({'botcheck_pause_seconds': '120'})
        h.handle_error(5, BOTCHECK_MSG, entry, proxy_switched=False)
        self.assertEqual([d for _, d in self.scheduled], [60, 120])

    def test_proxy_switched_uses_rotation_delay_not_pause(self):
        # When a proxy WAS switched, the short rotation retry is used -- the
        # proxies-off pause must not apply.
        h = self._make_handler(mode='auto', config={'botcheck_pause_seconds': '600'})
        entry = {'id': 6, 'retry_count': 0, 'max_retries': 3, 'proxy_rotations': 0}
        result = h.handle_error(6, BOTCHECK_MSG, entry, proxy_switched=True)
        self.assertEqual(result, 'rate_limited')
        self.assertEqual(self.scheduled[0][1], PROXY_SWITCH_DELAY)

    def test_proxy_on_but_none_available_uses_bounded_backoff(self):
        # Proxy mode on (auto) but no proxy was switched (e.g. pool empty): keep
        # the existing escalating backoff bounded by the retry budget, NOT the
        # proxies-off pause.
        h = self._make_handler(mode='auto', config={'botcheck_pause_seconds': '600'})
        entry = {'id': 7, 'retry_count': 0, 'max_retries': 3}
        result = h.handle_error(7, BOTCHECK_MSG, entry, proxy_switched=False)
        self.assertEqual(result, 'rate_limited')
        self.assertEqual(self.scheduled[0][1], BACKOFF_SCHEDULE[0])

    def test_no_proxy_manager_is_treated_as_proxies_off(self):
        # A handler with no proxy manager at all should still pause (not fail).
        h = ErrorHandler(self.db, proxy_manager=None,
                         config={'botcheck_pause_seconds': '15'})
        h._schedule_retry = lambda did, delay: self.scheduled.append((did, delay))
        entry = {'id': 8, 'retry_count': 0, 'max_retries': 3}
        result = h.handle_error(8, BOTCHECK_MSG, entry, proxy_switched=False)
        self.assertEqual(result, 'rate_limited')
        self.assertEqual(self.scheduled, [(8, 15)])


if __name__ == '__main__':
    unittest.main()
