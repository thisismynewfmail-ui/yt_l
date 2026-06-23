// Live log viewer. Prefers Server-Sent Events; falls back to polling
// /api/logs/recent if the stream cannot be established.
const LogStream = (() => {
  let container = null;
  let autoScrollEl = null;
  let source = null;
  let pollTimer = null;
  let lastKey = null;
  const MAX_LINES = 1000;

  function fmt(entry) {
    const ts = (entry.timestamp || '').replace('T', ' ').split('.')[0];
    const line = document.createElement('div');
    line.className = `log-line log-${entry.level || 'info'}`;
    const idTag = entry.download_id ? `#${entry.download_id} ` : '';
    line.textContent = `[${ts}] ${idTag}${entry.message}`;
    return line;
  }

  function append(entry) {
    if (!container) return;
    container.appendChild(fmt(entry));
    while (container.childElementCount > MAX_LINES) {
      container.removeChild(container.firstChild);
    }
    if (!autoScrollEl || autoScrollEl.checked) {
      container.scrollTop = container.scrollHeight;
    }
    lastKey = `${entry.timestamp}|${entry.message}`;
  }

  function startSSE() {
    try {
      source = new EventSource('/api/logs/stream');
      source.onmessage = (ev) => {
        if (!ev.data) return;
        try { append(JSON.parse(ev.data)); } catch (e) { /* keepalive */ }
      };
      source.onerror = () => {
        // Connection dropped — fall back to polling until it recovers.
        if (source) { source.close(); source = null; }
        startPolling();
      };
    } catch (e) {
      startPolling();
    }
  }

  function startPolling() {
    if (pollTimer) return;
    const tick = async () => {
      try {
        const logs = await API.recentLogs(200);
        let seenLast = lastKey === null;
        for (const entry of logs) {
          const key = `${entry.timestamp}|${entry.message}`;
          if (seenLast) append(entry);
          else if (key === lastKey) seenLast = true;
        }
        if (!seenLast) logs.forEach(append); // couldn't locate cursor; show all
      } catch (e) { /* ignore */ }
    };
    tick();
    pollTimer = setInterval(tick, 3000);
  }

  return {
    init(containerEl, autoScrollCheckbox) {
      container = containerEl;
      autoScrollEl = autoScrollCheckbox;
      if (window.EventSource) startSSE();
      else startPolling();
    },
    clear() { if (container) container.innerHTML = ''; },
  };
})();
