// Thin wrapper around the backend REST API. Every call returns a Promise that
// resolves to parsed JSON (or throws on a non-2xx response).
const API = (() => {
  async function request(method, path, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    const resp = await fetch(`/api${path}`, opts);
    let data = null;
    try { data = await resp.json(); } catch (e) { /* empty body */ }
    if (!resp.ok) {
      const msg = (data && (data.error || data.message)) || `HTTP ${resp.status}`;
      throw new Error(msg);
    }
    return data;
  }

  return {
    // Queue
    listDownloads: () => request('GET', '/downloads'),
    addDownload: (url, download_dir) => request('POST', '/downloads', { url, download_dir }),
    removeDownload: (id) => request('DELETE', `/downloads/${id}`),
    pauseDownload: (id) => request('PATCH', `/downloads/${id}`, { action: 'pause' }),
    resumeDownload: (id) => request('PATCH', `/downloads/${id}`, { action: 'resume' }),
    retryDownload: (id) => request('PATCH', `/downloads/${id}`, { action: 'retry' }),
    setDownloadDir: (id, download_dir) => request('PATCH', `/downloads/${id}`, { download_dir }),

    // Queue-wide
    pauseAll: () => request('POST', '/scheduler/pause-all'),
    resumeNext: () => request('POST', '/scheduler/resume-all'),
    restartAll: () => request('POST', '/queue/restart-all'),

    // Stats / config
    getStats: () => request('GET', '/stats'),
    getConfig: () => request('GET', '/config'),
    putConfig: (cfg) => request('PUT', '/config', cfg),

    // Engine
    engineStatus: () => request('GET', '/engine/status'),
    updateEngine: () => request('POST', '/engine/update'),

    // Proxy
    proxyStatus: () => request('GET', '/proxy/status'),
    setProxyMode: (mode) => request('POST', '/proxy/mode', { mode }),
    proxyRefresh: () => request('POST', '/proxy/refresh'),
    proxyTest: () => request('POST', '/proxy/test'),
    proxyDeactivate: () => request('POST', '/proxy/deactivate'),

    // Logs (fallback when SSE is unavailable)
    recentLogs: (limit = 200) => request('GET', `/logs/recent?limit=${limit}`),
  };
})();
