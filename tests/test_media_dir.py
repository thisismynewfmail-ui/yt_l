"""Tests for config.get_media_dir (where a item's videos are written/scanned)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


class GetMediaDirTest(unittest.TestCase):
    def test_single_video_uses_output_dir_directly(self):
        entry = {'download_dir': None, 'url': 'https://youtu.be/abc',
                 'total_videos': 1, 'title': 'My Video'}
        cfg = {'output_dir': '/base'}
        self.assertEqual(config.get_media_dir(entry, cfg, playlist_title='My Video'), '/base')

    def test_playlist_uses_subfolder(self):
        entry = {'download_dir': None, 'url': 'https://yt/playlist?list=X',
                 'total_videos': 12, 'title': 'Cool Playlist'}
        cfg = {'output_dir': '/base'}
        # No special chars -> the fallback sanitiser leaves the name intact.
        self.assertEqual(config.get_media_dir(entry, cfg, playlist_title='Cool Playlist'),
                         os.path.join('/base', 'Cool Playlist'))

    def test_per_item_download_dir_overrides_output_dir(self):
        entry = {'download_dir': '/custom', 'url': 'https://youtu.be/abc',
                 'total_videos': 1, 'title': 'V'}
        cfg = {'output_dir': '/base'}
        self.assertEqual(config.get_media_dir(entry, cfg, playlist_title='V'), '/custom')

    def test_trailing_slash_url_treated_as_playlist(self):
        # get_ydl_opts uses the same rule, so a channel/playlist URL ending in
        # '/' gets a subfolder even before total_videos is known.
        entry = {'download_dir': None, 'url': 'https://yt/@channel/',
                 'total_videos': 0, 'title': 'Chan'}
        cfg = {'output_dir': '/base'}
        self.assertEqual(config.get_media_dir(entry, cfg, playlist_title='Chan'),
                         os.path.join('/base', 'Chan'))

    def test_media_dir_matches_ydl_opts_outtmpl(self):
        # The directory we scan must be the directory yt-dlp writes into.
        cfg = {'output_dir': '/base'}
        for entry in (
            {'download_dir': None, 'url': 'https://youtu.be/abc',
             'total_videos': 1, 'title': 'Single'},
            {'download_dir': None, 'url': 'https://yt/playlist?list=X',
             'total_videos': 5, 'title': 'PL'},
        ):
            media_dir = config.get_media_dir(entry, cfg, playlist_title=entry['title'])
            outtmpl_dir = os.path.dirname(config.get_ydl_opts(entry, cfg)['outtmpl'])
            # For single videos both are the base dir; for playlists outtmpl has a
            # %(playlist_title)s placeholder whose sanitized value is the folder.
            if config._uses_playlist_subfolder(entry):
                self.assertTrue(outtmpl_dir.endswith('%(playlist_title)s'))
                self.assertEqual(os.path.dirname(media_dir), os.path.dirname(outtmpl_dir))
            else:
                self.assertEqual(media_dir, outtmpl_dir)


if __name__ == '__main__':
    unittest.main()
