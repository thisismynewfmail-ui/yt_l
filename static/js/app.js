// Main UI controller: wiring, polling, and event handling.
(() => {
  const $ = (id) => document.getElementById(id);
  const POLL_MS = 2000;

  let emptyEl = null;
  let settingsOpen = false;

  // ---- polling ------------------------------------------------------
  async function refresh() {
    try {
      const [downloads, stats] = await Promise.all([API.listDownloads(), API.getStats()]);
      Components.renderQueue(downloads, $('download-queue'), emptyEl);
      Components.renderStats(stats);
      if (stats.proxy) {
        Components.renderProxyBadge(stats.proxy);
        if (settingsOpen) Components.renderProxyDetail(stats.proxy);
      }
    } catch (e) { /* transient; next tick retries */ }
  }

  async function refreshEngine() {
    try {
      const s = await API.engineStatus();
      const el = $('engine-version');
      if (el) el.textContent = 'v' + (s.version || '?');
    } catch (e) { /* ignore */ }
  }

  // ---- add / queue-wide actions ------------------------------------
  async function addFromInputs() {
    const urlEl = $('input-url'), dirEl = $('input-dir');
    const url = urlEl.value.trim();
    if (!url) { urlEl.focus(); return; }
    try {
      await API.addDownload(url, dirEl.value.trim() || null);
      urlEl.value = '';
      refresh();
    } catch (e) { alert('Could not add: ' + e.message); }
  }

  // ---- per-item actions (event delegation) -------------------------
  async function onQueueClick(ev) {
    const btn = ev.target.closest('button[data-action]');
    if (!btn) return;
    const id = parseInt(btn.dataset.id, 10);
    const action = btn.dataset.action;
    btn.disabled = true;
    try {
      if (action === 'remove') {
        if (!confirm('Remove this item from the queue?')) { btn.disabled = false; return; }
        await API.removeDownload(id);
      } else if (action === 'pause') {
        await API.pauseDownload(id);
      } else if (action === 'resume') {
        await API.resumeDownload(id);
      } else if (action === 'retry') {
        await API.retryDownload(id);
      }
      await refresh();
    } catch (e) {
      alert(`Action failed: ${e.message}`);
      btn.disabled = false;
    }
  }

  // ---- settings modal ----------------------------------------------
  async function openSettings() {
    try {
      const c = await API.getConfig();
      $('cfg-output-dir').value = c.output_dir || '';
      $('cfg-sleep-interval').value = c.sleep_interval || '';
      $('cfg-max-sleep-interval').value = c.max_sleep_interval || '';
      $('cfg-format').value = c.format || '';
      $('cfg-max-concurrent').value = c.max_concurrent || '1';
      $('cfg-restart-delay').value = c.restart_delay || '300';
      $('cfg-archive-enabled').checked = String(c.archive_enabled) === 'true';
      $('cfg-proxy-mode').value = c.proxy_mode || 'off';
      $('cfg-proxy-seconds').value = c.proxy_active_seconds || '600';
      $('cfg-botcheck-pause').value = c.botcheck_pause_seconds || '600';
      $('cfg-proxy-list').value = c.proxy_list || '';
      $('cfg-scheduled-enabled').checked = String(c.scheduled_restart_enabled) === 'true';
      $('cfg-restart-hour').value = c.scheduled_restart_hour || '3';
      $('cfg-restart-minute').value = c.scheduled_restart_minute || '0';
    } catch (e) { /* show stale form */ }
    settingsOpen = true;
    $('settings-modal').classList.remove('hidden');
  }

  function closeSettings() {
    settingsOpen = false;
    $('settings-modal').classList.add('hidden');
  }

  async function applySettings() {
    const cfg = {
      output_dir: $('cfg-output-dir').value.trim(),
      sleep_interval: $('cfg-sleep-interval').value,
      max_sleep_interval: $('cfg-max-sleep-interval').value,
      format: $('cfg-format').value.trim(),
      max_concurrent: $('cfg-max-concurrent').value,
      restart_delay: $('cfg-restart-delay').value,
      archive_enabled: $('cfg-archive-enabled').checked ? 'true' : 'false',
      proxy_mode: $('cfg-proxy-mode').value,
      proxy_active_seconds: $('cfg-proxy-seconds').value,
      botcheck_pause_seconds: $('cfg-botcheck-pause').value,
      proxy_list: $('cfg-proxy-list').value.trim(),
      scheduled_restart_enabled: $('cfg-scheduled-enabled').checked ? 'true' : 'false',
      scheduled_restart_hour: $('cfg-restart-hour').value,
      scheduled_restart_minute: $('cfg-restart-minute').value,
    };
    try {
      await API.putConfig(cfg);
      closeSettings();
      refresh();
    } catch (e) { alert('Could not save settings: ' + e.message); }
  }

  // ---- proxy controls ----------------------------------------------
  async function proxyAction(fn, note) {
    try {
      await fn();
      const detail = $('proxy-detail');
      if (detail && note) detail.textContent = note;
      // Status updates flow in via the stats poll.
      setTimeout(refresh, 500);
    } catch (e) { alert('Proxy action failed: ' + e.message); }
  }

  // ---- wiring -------------------------------------------------------
  function wire() {
    emptyEl = $('empty-queue');

    $('btn-add').addEventListener('click', addFromInputs);
    $('input-url').addEventListener('keydown', (e) => { if (e.key === 'Enter') addFromInputs(); });
    $('input-dir').addEventListener('keydown', (e) => { if (e.key === 'Enter') addFromInputs(); });

    $('download-queue').addEventListener('click', onQueueClick);

    $('btn-pause-all').addEventListener('click', () => proxyActionSafe(API.pauseAll));
    $('btn-resume-all').addEventListener('click', () => proxyActionSafe(API.resumeNext));
    $('btn-restart-all').addEventListener('click', () => {
      if (confirm('Re-queue every finished item from the top?')) proxyActionSafe(API.restartAll);
    });

    $('btn-settings').addEventListener('click', openSettings);
    $('btn-close-settings').addEventListener('click', closeSettings);
    $('btn-cancel-settings').addEventListener('click', closeSettings);
    $('btn-apply-settings').addEventListener('click', applySettings);
    document.querySelector('#settings-modal .modal-backdrop').addEventListener('click', closeSettings);

    $('btn-proxy-refresh').addEventListener('click', () =>
      proxyAction(API.proxyRefresh, 'Refreshing proxy pool in the background…'));
    $('btn-proxy-test').addEventListener('click', () =>
      proxyAction(API.proxyTest, 'Health-checking proxies in the background…'));
    $('btn-proxy-disengage').addEventListener('click', () =>
      proxyAction(API.proxyDeactivate, 'Proxy disengaged.'));

    $('btn-update-engine').addEventListener('click', async () => {
      if (!confirm('Update the yt-dlp engine now?')) return;
      try { await API.updateEngine(); } catch (e) { alert(e.message); }
      setTimeout(refreshEngine, 3000);
    });

    $('btn-clear-log').addEventListener('click', () => LogStream.clear());

    LogStream.init($('log-output'), $('auto-scroll'));
  }

  // small helper: run an API call then refresh, surfacing errors
  async function proxyActionSafe(fn) {
    try { await fn(); refresh(); } catch (e) { alert(e.message); }
  }

  document.addEventListener('DOMContentLoaded', () => {
    wire();
    refresh();
    refreshEngine();
    setInterval(refresh, POLL_MS);
    setInterval(refreshEngine, 30000);
  });
})();
