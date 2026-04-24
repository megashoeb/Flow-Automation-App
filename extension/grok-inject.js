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

  // When the extension kicks off a click-based automation, it sets
  // __grokAutomationActive = true. The fetch patch then tees the
  // response stream of /rest/app-chat/conversations/new, parses the
  // NDJSON chunks for videoUrl / videoId / progress, and stores them
  // in __grokAutomationVideoUrl (and friends). When flag flips off,
  // we stop teeing to avoid cost on real user activity.
  window.__grokAutomationActive = false;
  window.__grokAutomationVideoUrl = "";
  window.__grokAutomationVideoId = "";
  window.__grokAutomationProgress = 0;
  window.__grokAutomationError = "";

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
      let isConvoNew = false;
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
          window.__grokLastHeaders = {
            ...window.__grokLastHeaders,
            ...snap,
          };
          if (Object.keys(snap).length) {
            window.__grokHeadersCapturedAt = Date.now();
          }
          if (url.includes("/rest/app-chat/conversations/new")) {
            isConvoNew = true;
          }
        }
      } catch (e) {
        // Never let instrumentation break the real app.
      }

      const response = await orig.apply(this, arguments);

      // If automation is active and this was the video-gen call, tee
      // the response body so we can parse progress/URL while the real
      // app's code also drains its own copy.
      if (isConvoNew && window.__grokAutomationActive && response.body && response.ok) {
        try {
          const [forApp, forUs] = response.body.tee();
          // Async-drain our copy, parsing NDJSON line by line.
          (async () => {
            const reader = forUs.getReader();
            const decoder = new TextDecoder();
            let buffer = "";
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                let nl;
                while ((nl = buffer.indexOf("\n")) !== -1) {
                  const line = buffer.slice(0, nl).trim();
                  buffer = buffer.slice(nl + 1);
                  if (!line) continue;
                  let obj;
                  try { obj = JSON.parse(line); } catch { continue; }
                  const resp = obj?.result?.response;
                  if (!resp) continue;
                  const svgr = resp.streamingVideoGenerationResponse;
                  if (svgr) {
                    if (typeof svgr.progress === "number") {
                      window.__grokAutomationProgress = svgr.progress;
                    }
                    if (svgr.videoId) window.__grokAutomationVideoId = svgr.videoId;
                    if (svgr.videoUrl) window.__grokAutomationVideoUrl = svgr.videoUrl;
                    if (svgr.moderated) window.__grokAutomationError = "moderated";
                  }
                  const fin = resp.finalMetadataMap?.videoGenModelConfig;
                  if (fin?.videoUrl) window.__grokAutomationVideoUrl = fin.videoUrl;
                }
              }
            } catch (e) {
              // Don't crash if the stream was aborted mid-read.
            }
          })();
          // Return a Response that wraps the app's side of the tee —
          // Grok's own code drains this, gets identical bytes.
          return new Response(forApp, {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
          });
        } catch (e) {
          // Fall through — hand back original response so app still works.
        }
      }

      return response;
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
