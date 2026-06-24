"""The download worker's match_filter skips videos already on disk by title."""
import os
import sys
import tempfile
import shutil
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.download_worker import DownloadWorker


class FakeDB:
    """Minimal stand-in: records archived increments and logs."""
    def __init__(self):
        self.archived = 0
        self.logs = []

    def increment_download(self, download_id, **kwargs):
        self.archived += kwargs.get('archived_videos', 0)

    def add_log(self, download_id, level, message):
        self.logs.append((level, message))


def make_worker(db, entry, cfg):
    # engine_manager / error_handler are unused by the match_filter path.
    return DownloadWorker(entry=entry, config=cfg, engine_manager=None,
                          error_handler=None, db=db, proxy_manager=None,
                          log_callback=None)


class SkipByTitleTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _touch(self, name, subdir=None):
        d = os.path.join(self.dir, subdir) if subdir else self.dir
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, name), 'w') as f:
            f.write('x')

    def test_single_video_already_on_disk_is_skipped_and_counted(self):
        self._touch('Existing Video.mp4')
        db = FakeDB()
        entry = {'id': 1, 'download_dir': self.dir, 'url': 'https://youtu.be/x',
                 'total_videos': 1, 'title': 'Existing Video'}
        worker = make_worker(db, entry, {'output_dir': self.dir})
        flt = worker._make_already_downloaded_filter(entry)

        # On disk -> rejected with a reason, counted once as archived.
        reason = flt({'title': 'Existing Video', 'id': 'v1'})
        self.assertIsInstance(reason, str)
        self.assertIn('title', reason)
        self.assertEqual(db.archived, 1)

        # Not on disk -> kept (None), no extra count.
        self.assertIsNone(flt({'title': 'Brand New Video', 'id': 'v2'}))
        self.assertEqual(db.archived, 1)

        # The same video seen twice (flat + full pass) is not double-counted.
        flt({'title': 'Existing Video', 'id': 'v1'})
        self.assertEqual(db.archived, 1)

    def test_playlist_scopes_to_its_own_subfolder(self):
        # File lives in the playlist's subfolder; detection must look there.
        self._touch('Track A.mp4', subdir='My Playlist')
        db = FakeDB()
        entry = {'id': 2, 'download_dir': self.dir, 'url': 'https://yt/playlist?list=X',
                 'total_videos': 3, 'title': 'My Playlist'}
        worker = make_worker(db, entry, {'output_dir': self.dir})
        flt = worker._make_already_downloaded_filter(entry)

        reason = flt({'title': 'Track A', 'id': 'a', 'playlist_title': 'My Playlist'})
        self.assertIsInstance(reason, str)
        self.assertEqual(db.archived, 1)
        # A different video in the same playlist is still downloaded.
        self.assertIsNone(flt({'title': 'Track B', 'id': 'b', 'playlist_title': 'My Playlist'}))
        self.assertEqual(db.archived, 1)

    def test_playlist_container_is_not_filtered(self):
        self._touch('Track A.mp4', subdir='My Playlist')
        db = FakeDB()
        entry = {'id': 3, 'download_dir': self.dir, 'url': 'https://yt/playlist?list=X',
                 'total_videos': 3, 'title': 'My Playlist'}
        worker = make_worker(db, entry, {'output_dir': self.dir})
        flt = worker._make_already_downloaded_filter(entry)
        # The playlist container itself must never be skipped.
        self.assertIsNone(flt({'title': 'My Playlist', '_type': 'playlist'}))
        self.assertEqual(db.archived, 0)

    def test_skip_reason_does_not_trip_archive_signal_counting(self):
        # The reject reason must NOT contain yt-dlp's "already been downloaded"
        # phrase, or the LogAdapter would count the skip a second time.
        from core.download_worker import ARCHIVE_SKIP_SIGNALS
        self._touch('Existing Video.mp4')
        db = FakeDB()
        entry = {'id': 4, 'download_dir': self.dir, 'url': 'https://youtu.be/x',
                 'total_videos': 1, 'title': 'Existing Video'}
        worker = make_worker(db, entry, {'output_dir': self.dir})
        flt = worker._make_already_downloaded_filter(entry)
        reason = flt({'title': 'Existing Video', 'id': 'v1'})
        for sig in ARCHIVE_SKIP_SIGNALS:
            self.assertNotIn(sig, reason)


if __name__ == '__main__':
    unittest.main()
