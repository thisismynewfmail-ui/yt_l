"""Title-based detection of already-downloaded videos.

When a playlist is removed from the queue its downloaded files are intentionally
kept on disk (see :meth:`DownloadManager.remove_download`). If the same playlist
is added again we want to recognise the videos that are already present so they
are not downloaded a second time -- even when yt-dlp's ID archive
(``.archive.txt``) has been lost, disabled, or the item was re-added under a
fresh queue row with no archive history.

The only durable signal we can rely on in that case is the *title*, which yt-dlp
uses to name the file on disk. Matching is therefore done on a normalised form
of the title: the same normaliser is applied to the candidate title and to the
on-disk filename, so cosmetic differences in how yt-dlp rewrote path-unsafe
characters (e.g. ``/`` -> ``⧸``, ``:`` -> ``：``) never cause a false
"not downloaded". This module deliberately does not depend on yt-dlp internals
so the detection is self-contained and easy to test.
"""

import os
import re

# Container/media extensions a finished download can have. Sidecar files
# (``.part``/``.ytdl`` temp files, the ``.archive.txt`` log, ``.info.json``,
# thumbnails, ...) are deliberately excluded so a half-finished download or a
# metadata-only file is never mistaken for a completed video.
MEDIA_EXTS = {
    '.mp4', '.mkv', '.webm', '.m4a', '.mp3', '.opus', '.flac', '.aac', '.ogg',
    '.oga', '.wav', '.avi', '.mov', '.flv', '.ts', '.m4v', '.3gp', '.wmv',
    '.mpg', '.mpeg', '.vob', '.f4v', '.mka', '.weba',
}

# Collapse runs of whitespace, and strip everything that is not a unicode word
# character (letters/digits/underscore across scripts) or whitespace. Punctuation
# and symbols are exactly what filename sanitisation rewrites inconsistently, so
# dropping them keeps both sides comparable while preserving accented letters and
# digits (so "Episode 1" and "Episode 2" stay distinct).
_DROP = re.compile(r'[^\w\s]', re.UNICODE)
_WS = re.compile(r'\s+', re.UNICODE)


def normalize_title(title):
    """Reduce a title to a comparison key.

    Lower-cases, removes punctuation/symbols, and collapses whitespace so that
    e.g. ``"AC/DC: Live!"`` and the file yt-dlp writes for it
    (``"AC⧸DC： Live!.mp4"``) both reduce to ``"acdc live"``.
    """
    if not title:
        return ''
    s = _DROP.sub(' ', str(title))
    s = _WS.sub(' ', s).strip().lower()
    return s


def scan_titles(directory):
    """Return the set of normalised title keys for finished media files that
    live directly in *directory*.

    A missing or unreadable directory yields an empty set, so callers can treat
    "nothing downloaded yet" uniformly. The scan is shallow on purpose: each
    playlist has its own folder, so scanning that folder avoids matching a
    same-titled video that belongs to a different playlist.
    """
    titles = set()
    if not directory or not os.path.isdir(directory):
        return titles
    try:
        names = os.listdir(directory)
    except OSError:
        return titles
    for name in names:
        root, ext = os.path.splitext(name)
        if ext.lower() not in MEDIA_EXTS:
            continue
        try:
            if not os.path.isfile(os.path.join(directory, name)):
                continue
        except OSError:
            continue
        key = normalize_title(root)
        if key:
            titles.add(key)
    return titles


def is_downloaded(title, scanned_titles):
    """True if *title* matches one of the keys returned by :func:`scan_titles`."""
    key = normalize_title(title)
    return bool(key) and key in scanned_titles
