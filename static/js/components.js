// Pure rendering helpers: turn API data into DOM updates. No network here.
const Components = (() => {
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function fmtSpeed(bytesPerSec) {
    if (!bytesPerSec || bytesPerSec <= 0) return '';
    const mb = bytesPerSec / 1024 / 1024;
    return mb >= 1 ? `${mb.toFixed(1)} MB/s` : `${(bytesPerSec / 1024).toFixed(0)} KB/s`;
  }

  function fmtEta(sec) {
    if (sec == null || sec < 0) return '';
    const m = Math.floor(sec / 60), s = Math.floor(sec % 60);
    return m > 0 ? `${m}m${s}s` : `${s}s`;
  }

  // status -> human label
  const LABELS = {
    queued: 'Queued', extracting: 'Extracting', downloading: 'Downloading',
    paused: 'Paused', completed: 'Completed', failed: 'Failed',
    rate_limited: 'Rate-limited',
  };

  function actionButtons(d) {
    const btn = (action, label, cls) =>
      `<button class="btn btn-small ${cls}" data-action="${action}" data-id="${d.id}">${label}</button>`;
    const out = [];
    const s = d.status;
    if (s === 'downloading' || s === 'extracting' || s === 'queued') {
      out.push(btn('pause', 'Pause', 'btn-warning'));
    }
    if (s === 'paused' || s === 'rate_limited') {
      out.push(btn('resume', 'Resume', 'btn-success'));
    }
    if (s === 'failed' || s === 'paused') {
      out.push(btn('retry', 'Retry', 'btn-secondary'));
    }
    if (s === 'completed') {
      out.push(btn('retry', 'Re-check', 'btn-secondary'));
    }
    out.push(btn('remove', 'Remove', 'btn-danger'));
    return out.join('');
  }

  function renderQueueItem(d) {
    const total = d.total_videos || 0;
    const downloaded = d.completed_videos || 0;   // freshly downloaded this pass
    const archived = d.archived_videos || 0;      // skipped: already in archive
    const accounted = downloaded + archived;      // everything done / not pending
    const pct = total > 0 ? Math.min(100, (accounted / total) * 100) : (accounted > 0 ? 100 : 0);

    // "accounted/total" drives the bar; downloaded vs archived are shown apart so
    // a video that was skipped (already archived) never reads as freshly downloaded.
    const counts = [`${accounted}/${total || '?'} videos`];
    if (downloaded) counts.push(`${downloaded} downloaded`);
    if (archived) counts.push(`${archived} already archived`);
    if (d.failed_videos) counts.push(`${d.failed_videos} failed`);
    if (d.recheck_count) counts.push(`re-check #${d.recheck_count}`);

    let current = '';
    if (d.status === 'downloading' && d.current_video) {
      const extras = [fmtSpeed(d.current_speed), fmtEta(d.current_eta) && `ETA ${fmtEta(d.current_eta)}`]
        .filter(Boolean).join(' · ');
      current = `<div class="qi-current">▶ ${esc(d.current_video)}${extras ? ' · ' + extras : ''}</div>`;
    }

    const error = d.error_message
      ? `<div class="qi-error" title="${esc(d.error_message)}">${esc(d.error_message)}</div>` : '';

    const title = esc(d.title || d.url);

    return `
      <div class="queue-item status-${esc(d.status)}" data-id="${d.id}">
        <div class="qi-main">
          <div class="qi-head">
            <span class="badge badge-status badge-${esc(d.status)}">${LABELS[d.status] || esc(d.status)}</span>
            <span class="qi-title" title="${esc(d.url)}">${title}</span>
          </div>
          <div class="qi-progress"><div class="qi-bar" style="width:${pct}%"></div></div>
          <div class="qi-meta">${counts.map(c => `<span>${esc(c)}</span>`).join('')}</div>
          ${current}
          ${error}
        </div>
        <div class="qi-actions">${actionButtons(d)}</div>
      </div>`;
  }

  function renderQueue(downloads, containerEl, emptyEl) {
    if (!downloads || downloads.length === 0) {
      containerEl.innerHTML = '';
      if (emptyEl) containerEl.appendChild(emptyEl);
      return;
    }
    containerEl.innerHTML = downloads.map(renderQueueItem).join('');
  }

  function renderStats(stats) {
    const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    const active = (stats.downloading || 0) + (stats.extracting || 0);
    set('stat-active', active);
    set('stat-queued', stats.queued || 0);
    set('stat-completed', stats.completed || 0);
    set('stat-failed', stats.failed || 0);
    set('stat-total', stats.total || 0);
    set('stat-videos', stats.total_completed_videos || 0);
    set('stat-archived', stats.total_archived_videos || 0);
    set('stat-rechecks', stats.total_rechecks || 0);
  }

  function renderProxyBadge(p) {
    const el = document.getElementById('proxy-status');
    if (!el || !p) return;
    el.classList.remove('proxy-on', 'proxy-auto', 'proxy-off');
    let text;
    if (p.mode === 'off') { text = 'proxy: off'; el.classList.add('proxy-off'); }
    else if (p.active && p.current) {
      const host = p.current.replace(/^[a-z0-9]+:\/\//, '');
      text = `proxy: ${host}`;
      if (p.mode === 'auto' && p.seconds_remaining) text += ` (${p.seconds_remaining}s)`;
      el.classList.add('proxy-on');
    } else {
      text = `proxy: ${p.mode} (idle)`;
      el.classList.add('proxy-auto');
    }
    el.textContent = text;
    el.title = `Mode: ${p.mode} · pool: ${p.pool_size} (good ${p.good}, bad ${p.bad})`;
  }

  function renderProxyDetail(p) {
    const el = document.getElementById('proxy-detail');
    if (!el || !p) return;
    const parts = [
      `mode: ${p.mode}`,
      `pool: ${p.pool_size} (good ${p.good}, bad ${p.bad})`,
      p.active ? `ACTIVE → ${p.current || 'n/a'}` : 'inactive',
    ];
    if (p.active && p.seconds_remaining) parts.push(`auto-off in ${p.seconds_remaining}s`);
    el.textContent = 'Proxy ' + parts.join(' · ');
  }

  return { renderQueue, renderStats, renderProxyBadge, renderProxyDetail };
})();
