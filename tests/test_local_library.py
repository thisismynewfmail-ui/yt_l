"""Tests for title-based detection of already-downloaded videos."""
import os
import sys
import tempfile
import shutil
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import local_library as L


class NormalizeTitleTest(unittest.TestCase):
    def test_title_matches_yt_dlp_sanitized_filename(self):
        # The raw title and the file yt-dlp would write for it must collapse to
        # the same key, despite yt-dlp rewriting path-unsafe characters.
        pairs = [
            ('AC/DC: Live!', 'AC⧸DC： Live!'),
            ('Hello | World', 'Hello ｜ World'),
            ('Question? Yes.', 'Question？ Yes.'),
            ('Episode 1. The Beginning', 'Episode 1. The Beginning'),
            ('Café déjà vu', 'Café déjà vu'),
        ]
        for raw, on_disk in pairs:
            self.assertEqual(L.normalize_title(raw), L.normalize_title(on_disk),
                             msg=f'{raw!r} should match {on_disk!r}')

    def test_distinct_titles_stay_distinct(self):
        self.assertNotEqual(L.normalize_title('Episode 1'), L.normalize_title('Episode 2'))
        self.assertNotEqual(L.normalize_title('Intro'), L.normalize_title('Outro'))

    def test_empty_and_none(self):
        self.assertEqual(L.normalize_title(None), '')
        self.assertEqual(L.normalize_title(''), '')
        self.assertEqual(L.normalize_title('   '), '')


class ScanTitlesTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _touch(self, name):
        with open(os.path.join(self.dir, name), 'w') as f:
            f.write('x')

    def test_only_media_files_counted(self):
        self._touch('Real Video.mp4')
        self._touch('Audio Track.m4a')
        self._touch('partial.mp4.part')      # in-progress: ignored
        self._touch('Metadata.info.json')    # sidecar: ignored (not a media ext)
        self._touch('.archive.txt')          # archive log: ignored
        self._touch('notes.txt')             # unrelated: ignored
        titles = L.scan_titles(self.dir)
        self.assertIn(L.normalize_title('Real Video'), titles)
        self.assertIn(L.normalize_title('Audio Track'), titles)
        self.assertNotIn(L.normalize_title('partial'), titles)
        self.assertNotIn(L.normalize_title('Metadata.info'), titles)
        self.assertEqual(len(titles), 2)

    def test_missing_directory_is_empty(self):
        self.assertEqual(L.scan_titles(os.path.join(self.dir, 'nope')), set())
        self.assertEqual(L.scan_titles(None), set())

    def test_is_downloaded_roundtrip(self):
        self._touch('My Great Video.mp4')
        present = L.scan_titles(self.dir)
        # Matches even though the lookup uses the raw (unsanitized) title.
        self.assertTrue(L.is_downloaded('My Great Video', present))
        self.assertTrue(L.is_downloaded('  my great video  ', present))
        self.assertFalse(L.is_downloaded('Some Other Video', present))
        self.assertFalse(L.is_downloaded('', present))


if __name__ == '__main__':
    unittest.main()
