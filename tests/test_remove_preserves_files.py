"""Removing a playlist must drop the queue row but keep the downloaded files."""
import os
import sys
import tempfile
import shutil
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import db
import core.proxy_manager as proxy_manager
from core.download_manager import DownloadManager


class RemovePreservesFilesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        # Point the DB and the proxy favourites file at the temp dir so the real
        # app state in data/ is never touched. Overriding the module globals (and
        # clearing the cached connection) is import-order independent.
        db.DB_PATH = os.path.join(self.tmp, 'test.db')
        db._local.conn = None
        proxy_manager.GOOD_LIST_PATH = os.path.join(self.tmp, 'good_proxies.json')
        db.init_db()

        self.out_dir = os.path.join(self.tmp, 'downloads')
        os.makedirs(self.out_dir, exist_ok=True)
        # proxy_mode 'off' keeps the manager fully offline during the test.
        cfg = {'proxy_mode': 'off', 'output_dir': self.out_dir}
        self.manager = DownloadManager(db, cfg)

    def tearDown(self):
        try:
            self.manager.proxy_manager.stop()
        except Exception:
            pass
        db._local.conn = None
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_remove_keeps_video_files_and_archive(self):
        # Simulate a playlist that already downloaded two videos + an archive.
        video1 = os.path.join(self.out_dir, 'Video One.mp4')
        video2 = os.path.join(self.out_dir, 'Video Two.mp4')
        archive = os.path.join(self.out_dir, '.archive.txt')
        for p in (video1, video2, archive):
            with open(p, 'w') as f:
                f.write('data')

        download_id = db.add_download('https://yt/playlist?list=X', download_dir=self.out_dir)
        self.assertIsNotNone(db.get_download(download_id))

        self.manager.remove_download(download_id)

        # Queue row is gone...
        self.assertIsNone(db.get_download(download_id))
        # ...but every downloaded file (and the archive) is still on disk.
        self.assertTrue(os.path.exists(video1))
        self.assertTrue(os.path.exists(video2))
        self.assertTrue(os.path.exists(archive))

    def test_readd_after_remove_detects_existing_by_title(self):
        # After a remove, re-adding the playlist should recognise the existing
        # files by title via the worker's match_filter.
        os.makedirs(os.path.join(self.out_dir, 'My PL'), exist_ok=True)
        with open(os.path.join(self.out_dir, 'My PL', 'Kept Video.mp4'), 'w') as f:
            f.write('data')

        download_id = db.add_download('https://yt/playlist?list=X', download_dir=self.out_dir)
        self.manager.remove_download(download_id)

        # Fresh queue row (no archive history carried over).
        new_id = db.add_download('https://yt/playlist?list=X', download_dir=self.out_dir)
        entry = db.get_download(new_id)
        entry['total_videos'] = 3  # marks it as a playlist (subfolder)

        from core.download_worker import DownloadWorker
        worker = DownloadWorker(entry=entry, config={'output_dir': self.out_dir},
                                engine_manager=None, error_handler=None, db=db,
                                proxy_manager=None, log_callback=None)
        flt = worker._make_already_downloaded_filter(entry)
        reason = flt({'title': 'Kept Video', 'id': 'k', 'playlist_title': 'My PL'})
        self.assertIsInstance(reason, str)


if __name__ == '__main__':
    unittest.main()
