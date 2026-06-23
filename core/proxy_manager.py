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

The module is intentionally dependency-free: remote lists are fetched with
urllib and candidates are validated by actually reaching Google through them
(HTTP/HTTPS proxies) or, for SOCKS proxies, with a TCP reachability check.

Reality check: free public proxies are mostly datacenter IPs that Google /
YouTube block on sight (the "Sign in to confirm you're not a bot" wall). So we
do three things to maximise the odds of a *working* proxy:
  1. Validate candidates against Google and prefer ones that pass.
  2. Treat any proxy that triggers a YouTube bot-check as bad and never reuse
     it (the strongest, real-world signal of a flagged IP).
  3. Honour a user-supplied proxy list first -- residential/paid proxies the
     user pastes in are far more likely to work than scraped public ones.
"""

import random
import socket
import threading
import time
from urllib.parse import urlparse
from urllib.request import build_opener, ProxyHandler, Request, urlopen


# Candidate proxy lists, ordered best-first. proxyscrape's ``ssl=yes`` returns
# HTTP proxies that can *tunnel* HTTPS targets (via CONNECT) -- the proxy
# endpoint itself still speaks plain HTTP, so these are 'http' proxies, NOT
# 'https'. (Labelling them 'https' makes urllib/yt-dlp attempt a TLS handshake
# with a plain HTTP proxy, which fails with "SSL: WRONG_VERSION_NUMBER".)
DEFAULT_PROXY_SOURCES = [
    ('http', 'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=7000&ssl=yes&anonymity=elite'),
    ('socks5', 'https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=7000'),
    ('socks5', 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt'),
    ('http', 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt'),
    ('socks5', 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt'),
    ('http', 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt'),
]

# Last-resort static seed used only when no user list is configured and the
# remote sources cannot be reached. Public proxies are volatile by nature, so
# every candidate is validated before use and rotated away from on failure.
DEFAULT_SEED_PROXIES = [
    'socks5://98.178.72.21:10919',
    'socks5://184.178.172.25:15291',
    'socks5://72.206.181.105:64935',
    'http://51.158.169.52:29976',
    'http://8.219.97.248:80',
]

MODES = ('off', 'auto', 'always')

# How many consecutive failures before a proxy is considered bad and skipped.
FAIL_THRESHOLD = 2
# Cap the working pool so refreshes/health-checks stay fast.
MAX_POOL = 250
# Per-candidate connect timeout (seconds) for the reachability/validation check.
PROBE_TIMEOUT = 7
# Tiny Google endpoint that returns HTTP 204. Reaching it through a proxy is a
# necessary condition for that proxy to work with YouTube.
PROXY_TEST_URL = 'https://www.google.com/generate_204'


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
    def __init__(self, config=None, log_callback=None, sources=None):
        self._lock = threading.RLock()
        self.log_callback = log_callback
        self._sources = sources if sources is not None else DEFAULT_PROXY_SOURCES

        # Pool entries: {'url', 'status': unknown|good|bad, 'fails': int}
        self._pool = []
        self._index = 0

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

        # Autonomous auto-off watchdog.
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

    def _set_pool(self, urls):
        seen = set()
        pool = []
        for url in urls:
            if url and url not in seen:
                seen.add(url)
                pool.append({'url': url, 'status': 'unknown', 'fails': 0})
        random.shuffle(pool)
        self._pool = pool[:MAX_POOL]
        self._index = 0

    # ---- pool sourcing -------------------------------------------------
    def ensure_pool(self):
        """Populate the pool lazily on first real use."""
        with self._lock:
            if self._pool:
                return
        self.refresh_pool()

    def refresh_pool(self, test=False, test_sample=40):
        """(Re)build the candidate pool from remote sources, with seed fallback."""
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

        if not fetched:
            self._log('warning', 'Could not fetch remote proxy lists; using static seed pool.')
            fetched = list(DEFAULT_SEED_PROXIES)

        with self._lock:
            self._set_pool(fetched)
            self._refreshed = True
            size = len(self._pool)
        self._log('info', f'Proxy pool refreshed: {size} candidates.')

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
    def health_check(self, sample=40):
        """Probe a sample of candidates; mark each good/bad. Returns # good."""
        with self._lock:
            candidates = list(self._pool)
        if not candidates:
            return 0
        sample_set = candidates if len(candidates) <= sample else random.sample(candidates, sample)

        good = 0
        for entry in sample_set:
            ok = self._probe(entry['url'])
            with self._lock:
                if ok:
                    entry['status'] = 'good'
                    entry['fails'] = 0
                    good += 1
                else:
                    entry['fails'] += 1
                    if entry['fails'] >= FAIL_THRESHOLD:
                        entry['status'] = 'bad'
        self._log('info', f'Proxy health check: {good}/{len(sample_set)} reachable.')
        return good

    @staticmethod
    def _probe(url):
        """Return True if the proxy looks usable for Google/YouTube.

        HTTP/HTTPS proxies are validated by actually fetching a tiny Google
        endpoint through them (the real test that matters). SOCKS proxies can't
        be driven by urllib without extra deps, so they fall back to a TCP
        reachability check.
        """
        parsed = urlparse(url)
        if not parsed.hostname or not parsed.port:
            return False
        scheme = parsed.scheme
        if scheme in ('http', 'https'):
            try:
                handler = ProxyHandler({'http': url, 'https': url})
                opener = build_opener(handler)
                req = Request(PROXY_TEST_URL, headers={'User-Agent': 'Mozilla/5.0'})
                with opener.open(req, timeout=PROBE_TIMEOUT) as resp:
                    return resp.status in (200, 204)
            except Exception:
                return False
        # SOCKS (or unknown): best-effort liveness check on the proxy port.
        try:
            with socket.create_connection((parsed.hostname, parsed.port), timeout=PROBE_TIMEOUT):
                return True
        except OSError:
            return False

    # ---- rotation / selection -----------------------------------------
    def _rotate_locked(self):
        """Advance ``current`` to the next usable proxy (round-robin).

        Prefers proxies validated against Google ('good'), then untested
        ('unknown'), skipping ones marked bad. Caller holds the lock.
        """
        n = len(self._pool)
        if n == 0:
            self.current = None
            return None
        # First pass: only validated-good proxies. Second pass: good or unknown.
        for allowed in (('good',), ('good', 'unknown')):
            for step in range(1, n + 1):
                idx = (self._index + step) % n
                if self._pool[idx]['status'] in allowed:
                    self._index = idx
                    self.current = self._pool[idx]['url']
                    return self.current
        # Everything is marked bad -- give them all another chance rather than
        # wedge the queue with no usable proxy.
        for entry in self._pool:
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
        *off* it; ``hard=True`` marks it bad immediately (used for YouTube
        bot-checks, where the IP is clearly flagged and not worth retrying).
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
        return proxy

    def _validate_async(self):
        try:
            self.health_check(sample=30)
        finally:
            self._validating = False

    def _penalise_locked(self, proxy_url, hard=False):
        for entry in self._pool:
            if entry['url'] == proxy_url:
                entry['fails'] += 1
                if hard or entry['fails'] >= FAIL_THRESHOLD:
                    entry['status'] = 'bad'
                return
        # Not in the pool (e.g. user removed it) -- nothing to penalise.

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
                return None
            new_proxy = self._rotate_locked()
        if new_proxy:
            self._log('warning', f'Proxy {proxy_url} failed; switched to {new_proxy}.')
        return new_proxy

    def report_success(self, proxy_url):
        if not proxy_url:
            return
        with self._lock:
            for entry in self._pool:
                if entry['url'] == proxy_url:
                    entry['status'] = 'good'
                    entry['fails'] = 0
                    break

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
                'refreshed': self._refreshed,
            }

    def list_pool(self, limit=100):
        with self._lock:
            return [dict(p) for p in self._pool[:limit]]

    # ---- internals -----------------------------------------------------
    def _watchdog(self):
        """Autonomously flips the proxy back off once the cool-down elapses."""
        while self._running:
            try:
                with self._lock:
                    if (self.mode == 'auto' and self.active and self._deactivate_at
                            and time.time() >= self._deactivate_at):
                        self._deactivate_locked('cool-down elapsed')
            except Exception:
                pass
            time.sleep(2)

    def stop(self):
        self._running = False

    def _log(self, level, message):
        if self.log_callback:
            try:
                self.log_callback(None, level, f'[proxy] {message}')
            except Exception:
                pass
