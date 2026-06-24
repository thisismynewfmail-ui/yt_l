"""Autonomous proxy rotation system.

Goals (see project requirements):
  * A selectable proxy *mode* (off / auto / always) exposed to the UI.
  * Auto-trigger: when downloads start hitting rate-limits / network errors the
    proxy engages itself without user intervention.
  * A configurable timeout (in seconds) after which the proxy system switches
    itself back *off* again -- so we only route through proxies while we
    actually need to, then return to direct connections.
  * Autonomous switching: rotate through a pool of candidates, skipping any
    that fail, so a single dead proxy never wedges the queue.
  * A wide variety of *known-good* proxies: a pool is assembled from several
    frequently-updated public proxy lists (plus a static seed fallback) and
    validated with a fast reachability check.
  * A persistent *favourites* list: every proxy that actually validates good is
    saved to disk and reloaded on startup. Favourites are merged into the pool
    on every refresh and tried *first* during rotation, so the hard-won good
    proxies are reused with priority instead of being rediscovered each time.

The module is intentionally dependency-free: remote lists are fetched with
urllib and candidates are validated by actually reaching Google through them
(HTTP/HTTPS proxies) or, for SOCKS proxies, with a TCP reachability check.
Validation is run in parallel (thread pool) so checking dozens of candidates
takes ~one timeout instead of dozens of them.

Reality check: free public proxies are mostly datacenter IPs that Google /
YouTube block on sight (the "Sign in to confirm you're not a bot" wall). So we
do these things to maximise the odds of a *working* proxy:
  1. Validate candidates against Google and prefer ones that pass.
  2. Save the passers to a persistent favourites list and reuse them first.
  3. Treat any proxy that triggers a YouTube bot-check as bad, drop it from the
     favourites, and never reuse it (the strongest signal of a flagged IP).
  4. Honour a user-supplied proxy list first -- residential/paid proxies the
     user pastes in are far more likely to work than scraped public ones.
"""

import json
import os
import random
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from urllib.request import build_opener, ProxyHandler, Request, urlopen


# Candidate proxy lists, ordered best-first. proxyscrape's ``ssl=yes`` returns
# HTTP proxies that can *tunnel* HTTPS targets (via CONNECT) -- the proxy
# endpoint itself still speaks plain HTTP, so these are 'http' proxies, NOT
# 'https'. (Labelling them 'https' makes urllib/yt-dlp attempt a TLS handshake
# with a plain HTTP proxy, which fails with "SSL: WRONG_VERSION_NUMBER".)
DEFAULT_PROXY_SOURCES = [
    # --- proxyscrape (v4 is current; v2 kept as a backup) ---------------------
    ('http', 'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text&timeout=7000'),
    ('socks5', 'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=socks5&proxy_format=ipport&format=text&timeout=7000'),
    ('socks4', 'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=socks4&proxy_format=ipport&format=text&timeout=7000'),
    ('http', 'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=7000&ssl=yes&anonymity=elite'),
    ('socks5', 'https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=7000'),
    # --- monosans (frequently updated & pre-validated) -----------------------
    ('socks5', 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt'),
    ('socks4', 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt'),
    ('http', 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt'),
    # --- TheSpeedX/PROXY-List ------------------------------------------------
    ('socks5', 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt'),
    ('socks4', 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt'),
    ('http', 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt'),
    # --- proxifly (scheme-prefixed lines; normalize_proxy handles them) ------
    ('http', 'https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt'),
    ('socks5', 'https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt'),
    # --- hookzof (socks5) ----------------------------------------------------
    ('socks5', 'https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt'),
    # --- jetkai (online-checked) ---------------------------------------------
    ('http', 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt'),
    ('socks5', 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt'),
    # --- ShiftyTR ------------------------------------------------------------
    ('http', 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt'),
    ('socks5', 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt'),
    # --- mmpx12 --------------------------------------------------------------
    ('http', 'https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt'),
    ('socks5', 'https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt'),
]

# Last-resort static seed used only when no user list is configured and the
# remote sources cannot be reached. Public proxies are volatile by nature, so
# every candidate is validated before use and rotated away from on failure.
DEFAULT_SEED_PROXIES = [
    'socks5://98.178.72.21:10919',
    'socks5://184.178.172.25:15291',
    'socks5://72.206.181.105:64935',
    'socks5://184.178.172.18:15280',
    'socks5://174.77.111.197:4145',
    'socks5://192.111.137.35:18302',
    'socks4://199.187.210.54:4145',
    'http://51.158.169.52:29976',
    'http://8.219.97.248:80',
    'http://47.74.152.29:8888',
    'http://158.255.77.166:80',
]

MODES = ('off', 'auto', 'always')

# How many consecutive failures before a proxy is considered bad and skipped.
FAIL_THRESHOLD = 2
# Cap the working pool so refreshes/health-checks stay fast. A larger cap gives
# the rotator far more candidates to fall back on when free proxies get flagged.
MAX_POOL = 500
# Per-candidate connect timeout (seconds) for the reachability/validation check.
PROBE_TIMEOUT = 7
# How many candidates to validate at once. Probing is network-bound, so a wide
# thread pool turns an N x timeout serial scan into roughly a single timeout.
PROBE_CONCURRENCY = 60
# Tiny Google endpoint that returns HTTP 204. Reaching it through a proxy is a
# necessary condition for that proxy to work with YouTube.
PROXY_TEST_URL = 'https://www.google.com/generate_204'

# Persistent "favourites" list of proxies that have validated good. Survives
# restarts and is reused with priority. Overridable for tests via env var.
MAX_FAVORITES = 64
GOOD_LIST_PATH = os.environ.get(
    'YTDLP_GOOD_PROXIES_PATH',
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 'data', 'good_proxies.json'),
)


def normalize_proxy(raw, default_scheme='http'):
    """Return a canonical ``scheme://host:port`` string, or None if unparseable."""
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if '://' not in raw:
        raw = f'{default_scheme}://{raw}'
    parsed = urlparse(raw)
    if not parsed.hostname or not parsed.port:
        return None
    scheme = parsed.scheme or default_scheme
    return f'{scheme}://{parsed.hostname}:{parsed.port}'


class ProxyManager:
    def __init__(self, config=None, log_callback=None, sources=None, good_list_path=None):
        self._lock = threading.RLock()
        self.log_callback = log_callback
        self._sources = sources if sources is not None else DEFAULT_PROXY_SOURCES
        self._good_list_path = good_list_path or GOOD_LIST_PATH

        # Pool entries: {'url', 'status': unknown|good|bad, 'fails': int, 'fav': bool}
        self._pool = []
        self._index = 0

        # Persistent favourites: url -> {'successes': int, 'last_ok': epoch}.
        # Loaded before configure() so the pool is seeded with them immediately.
        self._favorites = {}
        self._fav_dirty = False
        self._load_favorites()

        self.mode = 'off'
        self.active_seconds = 600
        self.active = False
        self.current = None
        self._deactivate_at = 0.0
        self._user_list_raw = None
        self._refreshed = False
        self._validating = False  # guards against spawning many probe threads

        if config is not None:
            self.configure(config)

        # Autonomous auto-off watchdog (also flushes favourites to disk).
        self._running = True
        self._thread = threading.Thread(target=self._watchdog, daemon=True)
        self._thread.start()

    # ---- configuration -------------------------------------------------
    def configure(self, config):
        """Sync runtime state with the persisted config dict."""
        with self._lock:
            mode = str(config.get('proxy_mode', 'off')).lower()
            self.mode = mode if mode in MODES else 'off'
            try:
                self.active_seconds = max(1, int(config.get('proxy_active_seconds', 600)))
            except (TypeError, ValueError):
                self.active_seconds = 600

            user_list = config.get('proxy_list', '') or ''
            if user_list != self._user_list_raw:
                self._user_list_raw = user_list
                proxies = self._parse_user_list(user_list)
                if proxies:
                    self._set_pool(proxies)
                    self._refreshed = True  # explicit list overrides remote fetch

            if self.mode == 'off':
                self._deactivate_locked('mode set to off')

        # Pre-warm the candidate pool in the background so 'always' mode has a
        # proxy ready immediately and 'auto' mode can engage without a stall.
        if self.mode in ('auto', 'always'):
            self.ensure_pool_async()

    def _parse_user_list(self, raw):
        out = []
        for token in str(raw).replace(',', '\n').splitlines():
            url = normalize_proxy(token)
            if url:
                out.append(url)
        return out

    def _set_pool(self, urls, add_missing_favorites=False):
        """Rebuild the pool. Favourites are marked good + flagged so rotation
        tries them first; ``add_missing_favorites`` also injects saved favourites
        that aren't in ``urls`` (used for public sourcing so we never lose them).
        Caller holds the lock."""
        urls = list(urls)
        if add_missing_favorites and self._favorites:
            present = set(urls)
            urls = [u for u in self._favorites if u not in present] + urls

        seen = set()
        favs, rest = [], []
        for url in urls:
            if not url or url in seen:
                continue
            seen.add(url)
            is_fav = url in self._favorites
            entry = {
                'url': url,
                'status': 'good' if is_fav else 'unknown',
                'fails': 0,
                'fav': is_fav,
            }
            (favs if is_fav else rest).append(entry)

        # Keep favourites at the front (priority); shuffle only the fresh tail so
        # load still spreads across new candidates.
        random.shuffle(rest)
        self._pool = (favs + rest)[:MAX_POOL]
        self._index = 0

    # ---- pool sourcing -------------------------------------------------
    def ensure_pool(self):
        """Populate the pool lazily on first real use."""
        with self._lock:
            if self._pool:
                return
        self.refresh_pool()

    def refresh_pool(self, test=False, test_sample=60):
        """(Re)build the candidate pool from remote sources, with seed fallback.

        Saved favourites are always merged in and, when ``test`` is set, are
        validated first so a known-good proxy is ready almost immediately.
        """
        # If the user supplied an explicit list, that is authoritative.
        with self._lock:
            user_proxies = self._parse_user_list(self._user_list_raw or '')
        if user_proxies:
            with self._lock:
                self._set_pool(user_proxies)
            if test:
                self.health_check(sample=test_sample)
            return len(user_proxies)

        fetched = []
        for scheme, url in self._sources:
            fetched.extend(self._fetch_source(scheme, url))

        if not fetched and not self._favorites:
            self._log('warning', 'Could not fetch remote proxy lists; using static seed pool.')
            fetched = list(DEFAULT_SEED_PROXIES)

        with self._lock:
            self._set_pool(fetched, add_missing_favorites=True)
            self._refreshed = True
            size = len(self._pool)
            fav_n = sum(1 for p in self._pool if p['fav'])
        self._log('info', f'Proxy pool refreshed: {size} candidates '
                          f'({fav_n} saved favourite(s) merged in).')

        if test:
            self.health_check(sample=test_sample)
        return size

    def _fetch_source(self, scheme, url):
        out = []
        try:
            with urlopen(url, timeout=15) as resp:
                text = resp.read().decode('utf-8', 'replace')
            for line in text.splitlines():
                normalized = normalize_proxy(line, default_scheme=scheme)
                if normalized:
                    out.append(normalized)
        except Exception as e:
            self._log('debug', f'Proxy source failed ({url}): {e}')
        return out

    # ---- health checks -------------------------------------------------
    def health_check(self, sample=60):
        """Probe candidates in parallel; mark each good/bad. Returns # good.

        Saved favourites and already-good proxies are always probed (so the
        favourites list self-heals); the remainder of the budget is a random
        sample of untested candidates.
        """
        with self._lock:
            candidates = list(self._pool)
        if not candidates:
            return 0

        priority = [e for e in candidates if e.get('fav') or e['status'] == 'good']
        prio_urls = {e['url'] for e in priority}
        rest = [e for e in candidates if e['url'] not in prio_urls]
        random.shuffle(rest)
        fill = max(0, sample - len(priority))
        sample_set = priority + rest[:fill]

        results = self._probe_many([e['url'] for e in sample_set])

        good = 0
        with self._lock:
            for entry in sample_set:
                if results.get(entry['url']):
                    entry['status'] = 'good'
                    entry['fails'] = 0
                    entry['fav'] = True
                    self._mark_favorite_locked(entry['url'])
                    good += 1
                else:
                    entry['fails'] += 1
                    if entry['fails'] >= FAIL_THRESHOLD:
                        entry['status'] = 'bad'
                        if entry.get('fav'):
                            entry['fav'] = False
                            self._unmark_favorite_locked(entry['url'])
        self._flush_favorites()
        self._log('info', f'Proxy health check: {good}/{len(sample_set)} reachable '
                          f'(parallel); {len(self._favorites)} favourite(s) saved.')
        return good

    def _probe_many(self, urls, timeout=None):
        """Validate many proxy URLs concurrently -> {url: ok_bool}."""
        results = {}
        urls = [u for u in urls if u]
        if not urls:
            return results
        workers = min(PROBE_CONCURRENCY, len(urls))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            future_map = {ex.submit(self._probe, u, timeout): u for u in urls}
            for fut in as_completed(future_map):
                url = future_map[fut]
                try:
                    results[url] = fut.result()
                except Exception:
                    results[url] = False
        return results

    @staticmethod
    def _probe(url, timeout=None):
        """Return True if the proxy looks usable for Google/YouTube.

        HTTP/HTTPS proxies are validated by actually fetching a tiny Google
        endpoint through them (the real test that matters). SOCKS proxies can't
        be driven by urllib without extra deps, so they fall back to a TCP
        reachability check.
        """
        timeout = timeout or PROBE_TIMEOUT
        parsed = urlparse(url)
        if not parsed.hostname or not parsed.port:
            return False
        scheme = parsed.scheme
        if scheme in ('http', 'https'):
            try:
                handler = ProxyHandler({'http': url, 'https': url})
                opener = build_opener(handler)
                req = Request(PROXY_TEST_URL, headers={'User-Agent': 'Mozilla/5.0'})
                with opener.open(req, timeout=timeout) as resp:
                    return resp.status in (200, 204)
            except Exception:
                return False
        # SOCKS (or unknown): best-effort liveness check on the proxy port.
        try:
            with socket.create_connection((parsed.hostname, parsed.port), timeout=timeout):
                return True
        except OSError:
            return False

    # ---- favourites (persistent good-proxy list) -----------------------
    def _load_favorites(self):
        """Load the saved good-proxy list from disk (best-first), tolerating a
        missing or corrupt file."""
        try:
            with open(self._good_list_path, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            return
        except Exception as e:
            self._log('debug', f'Could not load favourites: {e}')
            return

        items = data.get('proxies', []) if isinstance(data, dict) else data
        cleaned = []
        for it in items or []:
            if isinstance(it, str):
                url, rec = normalize_proxy(it), {'successes': 1, 'last_ok': 0.0}
            elif isinstance(it, dict) and it.get('url'):
                url = normalize_proxy(it['url'])
                rec = {
                    'successes': int(it.get('successes', 1) or 1),
                    'last_ok': float(it.get('last_ok', 0) or 0),
                }
            else:
                continue
            if url:
                cleaned.append((url, rec))
        cleaned.sort(key=lambda kr: (kr[1]['successes'], kr[1]['last_ok']), reverse=True)
        for url, rec in cleaned[:MAX_FAVORITES]:
            self._favorites[url] = rec
        if self._favorites:
            self._log('info', f'Loaded {len(self._favorites)} saved good proxy(ies) '
                              f'from the favourites list.')

    def _mark_favorite_locked(self, url, successes=1):
        """Add/refresh a favourite. Caller holds the lock."""
        if not url:
            return
        rec = self._favorites.get(url)
        if rec:
            rec['successes'] = rec.get('successes', 0) + successes
            rec['last_ok'] = time.time()
        else:
            if len(self._favorites) >= MAX_FAVORITES:
                worst = min(self._favorites.items(),
                            key=lambda kv: (kv[1].get('successes', 0), kv[1].get('last_ok', 0)))
                self._favorites.pop(worst[0], None)
            self._favorites[url] = {'successes': successes, 'last_ok': time.time()}
        self._fav_dirty = True

    def _unmark_favorite_locked(self, url):
        """Drop a favourite that no longer works. Caller holds the lock."""
        if url in self._favorites:
            self._favorites.pop(url, None)
            self._fav_dirty = True

    def _flush_favorites(self, force=False):
        """Persist the favourites list to disk if it changed (debounced)."""
        with self._lock:
            if not self._fav_dirty and not force:
                return
            items = sorted(self._favorites.items(),
                           key=lambda kv: (kv[1].get('successes', 0), kv[1].get('last_ok', 0)),
                           reverse=True)
            payload = {'proxies': [
                {'url': u, 'successes': r.get('successes', 1), 'last_ok': r.get('last_ok', 0)}
                for u, r in items
            ]}
            self._fav_dirty = False
        try:
            os.makedirs(os.path.dirname(self._good_list_path), exist_ok=True)
            tmp = self._good_list_path + '.tmp'
            with open(tmp, 'w') as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, self._good_list_path)
        except Exception as e:
            self._log('debug', f'Could not save favourites: {e}')

    def list_favorites(self, limit=100):
        with self._lock:
            items = sorted(self._favorites.items(),
                           key=lambda kv: (kv[1].get('successes', 0), kv[1].get('last_ok', 0)),
                           reverse=True)
            return [{'url': u, 'successes': r.get('successes', 1), 'last_ok': r.get('last_ok', 0)}
                    for u, r in items[:limit]]

    # ---- rotation / selection -----------------------------------------
    def _rotate_locked(self):
        """Advance ``current`` to the next usable proxy (round-robin).

        Priority tiers, best first: saved favourites, then proxies validated
        good this session, then untested ('unknown'). Bad proxies are skipped.
        Caller holds the lock.
        """
        n = len(self._pool)
        if n == 0:
            self.current = None
            return None
        tiers = (
            lambda e: e.get('fav') and e['status'] != 'bad',
            lambda e: e['status'] == 'good',
            lambda e: e['status'] == 'unknown',
        )
        for usable in tiers:
            for step in range(1, n + 1):
                idx = (self._index + step) % n
                if usable(self._pool[idx]):
                    self._index = idx
                    self.current = self._pool[idx]['url']
                    return self.current
        # Everything is marked bad -- give the non-favourites another chance
        # rather than wedge the queue with no usable proxy.
        for entry in self._pool:
            if not entry.get('fav'):
                entry['status'] = 'unknown'
            entry['fails'] = 0
        self._index = 0
        self.current = self._pool[0]['url']
        return self.current

    # ---- public lifecycle ----------------------------------------------
    def trigger(self, reason='error', failed_proxy=None, hard=False):
        """Auto-engage the proxy system and switch to a fresh proxy.

        Called when a download hits a rate-limit / bot-check / network error.
        ``failed_proxy`` (the proxy that was in use) is penalised so we rotate
        *off* it; ``hard=True`` marks it bad immediately and removes it from the
        favourites (used for YouTube bot-checks, where the IP is clearly flagged).
        """
        if self.mode not in ('auto', 'always'):
            return None
        self.ensure_pool()
        with self._lock:
            if failed_proxy:
                self._penalise_locked(failed_proxy, hard=hard)
            was_active = self.active
            self.active = True
            self._deactivate_at = time.time() + self.active_seconds
            proxy = self._rotate_locked()
            need_validation = (proxy is not None and not self._validating
                               and not any(p['status'] == 'good' for p in self._pool))
            if need_validation:
                self._validating = True
        if proxy:
            verb = 'rotation' if was_active else 'system engaged'
            self._log('warning', f'Proxy {verb} ({reason}): now using {proxy}. '
                                 f'Auto-off in {self.active_seconds}s.')
            # No proxy has been validated against Google yet -- kick a single
            # background check so future rotations prefer ones that actually work.
            if need_validation:
                threading.Thread(target=self._validate_async, daemon=True).start()
        else:
            self._log('error', 'Proxy trigger requested but no candidates are available.')
        if failed_proxy and hard:
            self._flush_favorites()
        return proxy

    def _validate_async(self):
        try:
            self.health_check(sample=PROBE_CONCURRENCY)
        finally:
            self._validating = False

    def _penalise_locked(self, proxy_url, hard=False):
        for entry in self._pool:
            if entry['url'] == proxy_url:
                entry['fails'] += 1
                if hard or entry['fails'] >= FAIL_THRESHOLD:
                    entry['status'] = 'bad'
                    if entry.get('fav'):
                        entry['fav'] = False
                        self._unmark_favorite_locked(proxy_url)
                return
        # Not in the pool (e.g. user removed it). A hard failure (flagged IP)
        # should still evict it from the saved favourites.
        if hard:
            self._unmark_favorite_locked(proxy_url)

    def get_proxy(self):
        """Return the proxy URL to use right now, or None for a direct connection."""
        with self._lock:
            if self.mode == 'off':
                return None
            if self.mode == 'always':
                if not self.active or not self.current:
                    self.active = True
                    self._deactivate_at = 0.0  # always-on never auto-expires
                    if not self.current:
                        # rotate outside lock would deadlock; pool may be empty
                        if self._pool:
                            self._rotate_locked()
                    return self.current
                return self.current
            # auto mode
            if self.active:
                if self._deactivate_at and time.time() >= self._deactivate_at:
                    self._deactivate_locked('cool-down elapsed')
                    return None
                return self.current
            return None

    def report_failure(self, proxy_url, hard=False):
        """Record that a proxy failed mid-use and rotate to the next one."""
        if not proxy_url:
            return None
        with self._lock:
            self._penalise_locked(proxy_url, hard=hard)
            if self.mode == 'off' or not self.active:
                new_proxy = None
            else:
                new_proxy = self._rotate_locked()
        self._flush_favorites()
        if new_proxy:
            self._log('warning', f'Proxy {proxy_url} failed; switched to {new_proxy}.')
        return new_proxy

    def report_success(self, proxy_url):
        """Record that a proxy worked: mark it good and save it as a favourite."""
        if not proxy_url:
            return
        with self._lock:
            for entry in self._pool:
                if entry['url'] == proxy_url:
                    entry['status'] = 'good'
                    entry['fails'] = 0
                    entry['fav'] = True
                    break
            self._mark_favorite_locked(proxy_url)
        self._flush_favorites()

    def deactivate(self, reason='manual'):
        with self._lock:
            self._deactivate_locked(reason)

    def _deactivate_locked(self, reason):
        if self.active or self.current:
            self._log('info', f'Proxy system disengaged ({reason}); returning to direct connection.')
        self.active = False
        self.current = None
        self._deactivate_at = 0.0

    def set_mode(self, mode):
        mode = str(mode).lower()
        if mode not in MODES:
            return False
        with self._lock:
            self.mode = mode
            if mode == 'off':
                self._deactivate_locked('mode set to off')
            elif mode == 'always':
                self.ensure_pool_async()
        return True

    def ensure_pool_async(self):
        if not self._pool:
            threading.Thread(target=self.ensure_pool, daemon=True).start()

    # ---- introspection -------------------------------------------------
    def status(self):
        with self._lock:
            remaining = 0
            if self.active and self._deactivate_at:
                remaining = max(0, int(self._deactivate_at - time.time()))
            good = sum(1 for p in self._pool if p['status'] == 'good')
            bad = sum(1 for p in self._pool if p['status'] == 'bad')
            return {
                'mode': self.mode,
                'active': self.active,
                'current': self.current,
                'active_seconds': self.active_seconds,
                'seconds_remaining': remaining,
                'pool_size': len(self._pool),
                'good': good,
                'bad': bad,
                'favorites': len(self._favorites),
                'refreshed': self._refreshed,
            }

    def list_pool(self, limit=100):
        with self._lock:
            return [dict(p) for p in self._pool[:limit]]

    # ---- internals -----------------------------------------------------
    def _watchdog(self):
        """Autonomously flips the proxy back off once the cool-down elapses, and
        flushes any pending favourites to disk (debounced)."""
        while self._running:
            try:
                with self._lock:
                    if (self.mode == 'auto' and self.active and self._deactivate_at
                            and time.time() >= self._deactivate_at):
                        self._deactivate_locked('cool-down elapsed')
                self._flush_favorites()
            except Exception:
                pass
            time.sleep(2)

    def stop(self):
        self._running = False
        self._flush_favorites(force=True)

    def _log(self, level, message):
        if self.log_callback:
            try:
                self.log_callback(None, level, f'[proxy] {message}')
            except Exception:
                pass
