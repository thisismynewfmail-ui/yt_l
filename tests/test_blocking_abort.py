"""A flagged IP fails *every* video, so a short streak of blocking (bot-check /
rate-limit) errors must abort the whole playlist run rather than churn through
all 1000+ remaining items under ``ignoreerrors`` before the worker can pause.

These tests exercise the streak detection in ``_on_engine_error`` directly.
yt-dlp isn't importable in the test environment, so ``_abort_exception`` falls
back to ``BlockingError`` -- the assertions only care that an abort is raised at
the right moment, not which concrete class it is.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.download_worker import DownloadWorker, BLOCKING_ABORT_THRESHOLD
from core.error_handler import ErrorHandler

BOTCHECK = ("ERROR: [youtube] abc: Sign in to confirm you’re not a bot. "
            "Use --cookies-from-browser or --cookies for the authentication.")
UNAVAILABLE = "ERROR: [youtube] xyz: Video unavailable. This video has been removed."


class FakeDB:
    def add_log(self, *a, **k):
        pass


class FakeEngine:
    update_in_progress = False


def make_worker():
    eh = ErrorHandler(FakeDB(), proxy_manager=None)
    entry = {'id': 1, 'url': 'https://x', 'download_dir': '/tmp'}
    return DownloadWorker(entry=entry, config={}, engine_manager=FakeEngine(),
                          error_handler=eh, db=FakeDB(), proxy_manager=None)


class BlockingAbortTest(unittest.TestCase):
    def test_no_abort_below_threshold(self):
        w = make_worker()
        for _ in range(BLOCKING_ABORT_THRESHOLD - 1):
            w._on_engine_error(BOTCHECK)  # must not raise
        self.assertFalse(w._abort_raised)
        self.assertEqual(w._consecutive_blocking, BLOCKING_ABORT_THRESHOLD - 1)
        # The first blocking error is remembered as the run's blocking reason.
        self.assertEqual(w._blocking_error, BOTCHECK)

    def test_streak_aborts_at_threshold(self):
        w = make_worker()
        for _ in range(BLOCKING_ABORT_THRESHOLD - 1):
            w._on_engine_error(BOTCHECK)
        with self.assertRaises(Exception):
            w._on_engine_error(BOTCHECK)
        self.assertTrue(w._abort_raised)
        self.assertEqual(w._blocking_error, BOTCHECK)

    def test_abort_raised_only_once(self):
        w = make_worker()
        raises = 0
        for _ in range(BLOCKING_ABORT_THRESHOLD + 3):
            try:
                w._on_engine_error(BOTCHECK)
            except Exception:
                raises += 1
        # Aborts exactly once; subsequent blocking errors are recorded silently
        # (we've already told the engine to stop).
        self.assertEqual(raises, 1)

    def test_non_blocking_error_resets_streak(self):
        w = make_worker()
        for _ in range(BLOCKING_ABORT_THRESHOLD - 1):
            w._on_engine_error(BOTCHECK)
        # A lone unavailable/private video breaks the streak...
        w._on_engine_error(UNAVAILABLE)
        self.assertEqual(w._consecutive_blocking, 0)
        self.assertFalse(w._abort_raised)
        # ...so the next single blocking error does not abort on its own.
        w._on_engine_error(BOTCHECK)
        self.assertFalse(w._abort_raised)

    def test_successful_download_resets_streak(self):
        # A finished download mid-run clears the streak so blocking errors must
        # build up again from scratch before any abort.
        w = make_worker()
        w._blocking_error = None
        for _ in range(BLOCKING_ABORT_THRESHOLD - 1):
            w._on_engine_error(BOTCHECK)
        w._on_progress({'status': 'finished',
                        'info_dict': {'id': 'v1', 'title': 'ok'},
                        'filename': 'ok.mp4'})
        self.assertEqual(w._consecutive_blocking, 0)

    def test_abort_exception_prefers_download_cancelled_when_available(self):
        # If the engine is importable, the abort must be DownloadCancelled (which
        # ignoreerrors does not swallow); otherwise BlockingError is acceptable.
        from core.download_worker import BlockingError
        exc = DownloadWorker._abort_exception('boom')
        try:
            from yt_dlp.utils import DownloadCancelled
            self.assertIsInstance(exc, DownloadCancelled)
        except Exception:
            self.assertIsInstance(exc, BlockingError)


if __name__ == '__main__':
    unittest.main()
