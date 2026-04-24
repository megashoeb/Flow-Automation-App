/**
 * G-Labs Studio Helper — Grok MAIN-world header capture
 *
 * Registered in manifest.json as a content_script at run_at:
 * "document_start", world: "MAIN", matches: "https://grok.com/*".
 * This runs BEFORE any of Grok's own JavaScript, so our fetch patch is
 * the one Grok's SDK sees and uses — every subsequent /rest/ call
 * flows through our capture.
 *
 * We don't forward or block anything — we just peek at the headers
 * Grok attaches (x-statsig-id, x-xai-auth etc.) and cache the most
 * recent snapshot on window. grok.js reads that snapshot at dispatch
 * time and replays the anti-bot headers on its own fetch calls.
 */
(() => {
  if (window.__grokFetchPatchInstalled) return;
  window.__grokFetchPatchInstalled = true;
  window.__grokLastHeaders = {};
  window.__grokHeadersCapturedAt = 0;
  window.__grokCaptureStats = { totalCalls: 0, restCalls: 0 };

  const snapshotHeaders = (hdrs) => {
    const snap = {};
    if (!hdrs) return snap;
    try {
      if (hdrs instanceof Headers) {
        hdrs.forEach((v, k) => (snap[k.toLowerCase()] = v));
      } else if (Array.isArray(hdrs)) {
        hdrs.forEach(([k, v]) => (snap[String(k).toLowerCase()] = v));
      } else if (typeof hdrs === "object") {
        Object.keys(hdrs).forEach((k) => (snap[k.toLowerCase()] = hdrs[k]));
      }
    } catch (e) {}
    return snap;
  };

  const installFetch = () => {
    const current = window.fetch;
    if (!current || current.__grokPatched) return;
    const orig = current;

    const patched = async function (input, init) {
      try {
        window.__grokCaptureStats.totalCalls++;
        const url =
          typeof input === "string"
            ? input
            : input && input.url
            ? input.url
            : "";
        if (url && url.includes("/rest/")) {
          window.__grokCaptureStats.restCalls++;
          const snap = snapshotHeaders(init && init.headers);
          // Merge with prior snapshots so partial-header calls still
          // contribute to a full picture. x-statsig-id is stable per
          // session so an old capture still works until a new one
          // arrives.
          window.__grokLastHeaders = {
            ...window.__grokLastHeaders,
            ...snap,
          };
          if (Object.keys(snap).length) {
            window.__grokHeadersCapturedAt = Date.now();
          }
        }
      } catch (e) {
        // Never let instrumentation break the real app.
      }
      return orig.apply(this, arguments);
    };
    patched.__grokPatched = true;
    try {
      window.fetch = patched;
    } catch (e) {
      // Some pages freeze window.fetch — nothing we can do.
    }
  };

  installFetch();
  // Grok uses Next.js + Statsig client SDK; in rare cases the SDK or
  // a bundler will replace window.fetch after our patch. Re-install
  // periodically so we stay on top of any replacement. Cheap — just
  // a property check every second.
  setInterval(installFetch, 1000);

  // Also patch XMLHttpRequest in case Grok has legacy code paths
  // (future-proofing). Same capture shape.
  try {
    const OrigXHR = window.XMLHttpRequest;
    if (OrigXHR && !OrigXHR.__grokPatched) {
      const origOpen = OrigXHR.prototype.open;
      const origSetHeader = OrigXHR.prototype.setRequestHeader;
      OrigXHR.prototype.open = function (method, url) {
        this.__grokUrl = url;
        this.__grokHeaders = {};
        return origOpen.apply(this, arguments);
      };
      OrigXHR.prototype.setRequestHeader = function (name, value) {
        try {
          if (this.__grokHeaders) {
            this.__grokHeaders[String(name).toLowerCase()] = String(value);
          }
          if (
            this.__grokUrl &&
            String(this.__grokUrl).includes("/rest/") &&
            this.__grokHeaders
          ) {
            window.__grokLastHeaders = {
              ...window.__grokLastHeaders,
              ...this.__grokHeaders,
            };
            window.__grokHeadersCapturedAt = Date.now();
            window.__grokCaptureStats.restCalls++;
          }
        } catch (e) {}
        return origSetHeader.apply(this, arguments);
      };
      OrigXHR.__grokPatched = true;
    }
  } catch (e) {}
})();
