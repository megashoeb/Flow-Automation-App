/**
 * G-Labs Studio Helper — Grok Module
 *
 * Standalone module loaded via importScripts() in background.js.
 * Talks to GrokBridge at http://127.0.0.1:18926 — completely separate
 * from Flow (18924) and Genspark (18925). All three modes can run side by
 * side without interfering.
 *
 * Grok Imagine pipeline (from HAR reverse-engineering, Apr 2026):
 *
 *   1. (Optional) Upload reference image:
 *        POST /rest/app-chat/upload-file
 *        Body: { fileName, fileMimeType, fileSource, content (base64) }
 *        → returns fileMetadataId + fileUri
 *
 *   2. Create post (wraps image or text prompt):
 *        POST /rest/media/post/create
 *        - Image mode: { mediaType:"MEDIA_POST_TYPE_IMAGE",
 *                        mediaUrl:"https://assets.grok.com/<fileUri>" }
 *        - Text  mode: { mediaType:"MEDIA_POST_TYPE_VIDEO", prompt:"cat" }
 *        → returns post.id
 *
 *   3. Animate → video (streaming NDJSON):
 *        POST /rest/app-chat/conversations/new
 *        Body has videoGenModelConfig with aspect/length/resolution.
 *        Response streams progress 1→100 then emits videoUrl.
 *
 * No reCAPTCHA involved — auth is cookie-based. We execute every fetch
 * inside the grok.com tab via chrome.scripting.executeScript (world:"MAIN")
 * so the page's Statsig/XAI headers + cookies are auto-attached.
 */

const GROK_BRIDGE_URL = "http://127.0.0.1:18926";
const GROK_POLL_INTERVAL = 1500;
const GROK_ACCOUNT_DETECT_INTERVAL = 15000;
const GROK_HEADER_CAPTURE_INTERVAL = 20000;  // re-inject capture if lost
const GROK_ORIGIN = "https://grok.com";
const GROK_ASSETS_ORIGIN = "https://assets.grok.com";

// Parallel concurrency ceiling — UI caps at ~15 for Grok, this is defence
// in depth so a misconfigured Python side can't blow past Grok's fair-use
// throttle and trigger a soft cap.
const GROK_MAX_PARALLEL = 15;

// ─── State ───
let grokBridgeConnected = false;
let grokAccounts = {};  // email → { email, userId, tab_id, last_seen }
let grokLastPollError = "";
let grokActiveCount = 0;

// Dedupe — bridge shouldn't re-dispatch but belt-and-suspenders.
const grokInFlightRequestIds = new Set();
const grokSubmittedRequestIds = new Set();

// ═══════════════════════════════════════════════════════════════════
// Header capture — Grok's backend requires x-statsig-id and similar
// anti-bot headers on /rest/app-chat/conversations/new. These headers
// are added by Grok's own app code, NOT by a fetch interceptor, so a
// plain fetch() from our executeScript misses them and gets rejected
// with {code: 7, message: "Request rejected by anti-bot rules"}.
//
// Workaround: install a fetch monkey-patch in the grok.com tab's MAIN
// world that captures headers from any real Grok API call. Our
// automation reads the captured headers and replays them.
//
// The patch persists across subsequent chrome.scripting.executeScript
// calls because MAIN world shares window with the page. We re-inject
// periodically in case the page navigated/reloaded.
// ═══════════════════════════════════════════════════════════════════

// The actual fetch monkey-patch is installed by grok-inject.js as a
// manifest content_script at document_start (world: MAIN). That way
// it runs BEFORE Grok's own bundler grabs fetch, so every /rest/ call
// — including the ones Grok makes on page load — is captured.
//
// This function is a belt-and-suspenders fallback: if the content
// script somehow didn't install (e.g. older Chrome without MAIN-world
// content_scripts), we install the same patch via executeScript. The
// patch is idempotent so it's cheap to call on every detection cycle.
async function grokInstallHeaderCapture(tabId) {
  try {
    const result = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: () => {
        if (window.__grokFetchPatchInstalled) {
          return {
            ok: true,
            already: true,
            captured: !!window.__grokHeadersCapturedAt,
            stats: window.__grokCaptureStats || null,
          };
        }
        window.__grokFetchPatchInstalled = true;
        window.__grokLastHeaders = {};
        window.__grokHeadersCapturedAt = 0;
        window.__grokCaptureStats = { totalCalls: 0, restCalls: 0 };
        const origFetch = window.fetch;
        window.fetch = async function (input, init) {
          try {
            window.__grokCaptureStats.totalCalls++;
            const url =
              typeof input === "string" ? input : input && input.url ? input.url : "";
            if (url && url.includes("/rest/")) {
              window.__grokCaptureStats.restCalls++;
              const hdrs = init && init.headers;
              const snap = {};
              if (hdrs) {
                if (hdrs instanceof Headers) {
                  hdrs.forEach((v, k) => (snap[k.toLowerCase()] = v));
                } else if (Array.isArray(hdrs)) {
                  hdrs.forEach(([k, v]) => (snap[String(k).toLowerCase()] = v));
                } else if (typeof hdrs === "object") {
                  Object.keys(hdrs).forEach(
                    (k) => (snap[k.toLowerCase()] = hdrs[k])
                  );
                }
              }
              window.__grokLastHeaders = {
                ...window.__grokLastHeaders,
                ...snap,
              };
              if (Object.keys(snap).length) {
                window.__grokHeadersCapturedAt = Date.now();
              }
            }
          } catch (e) {}
          return origFetch.apply(this, arguments);
        };
        return { ok: true, installed: true };
      },
    });
    return result?.[0]?.result || null;
  } catch (e) {
    console.warn("[Grok] installHeaderCapture failed:", e.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════
// Click-based dispatch — sets the textarea value, clicks send, and
// lets Grok's own code fire the /rest/app-chat/conversations/new
// call. Grok's SDK attaches a fresh x-statsig-id at call time (we
// can't replicate that from a replay — each call has its own value),
// which is why direct fetch() always hits anti-bot. Click-based is
// HTTP at its core: we just trigger Grok's in-page code path.
//
// Returns { ok, buttonInfo } on success or { error } on failure.
// ═══════════════════════════════════════════════════════════════════

async function grokClickSend(tabId, prompt) {
  return grokExecInTab(tabId, async (args) => {
    // Arm automation flags so the fetch wrapper captures the
    // conversations/new response stream.
    window.__grokAutomationActive = true;
    window.__grokAutomationVideoUrl = "";
    window.__grokAutomationVideoId = "";
    window.__grokAutomationProgress = 0;
    window.__grokAutomationError = "";
    window.__grokAutomationStartedAt = Date.now();

    // Find the prompt textarea. Grok's /imagine page uses a single
    // visible textarea; we grab the first one that's actually in the
    // layout (offsetParent != null means not display:none).
    const textareas = Array.from(document.querySelectorAll("textarea"));
    const textarea =
      textareas.find((t) => t.offsetParent !== null) || textareas[0];
    if (!textarea) {
      window.__grokAutomationActive = false;
      return { error: "no_textarea_found" };
    }

    // Set the value via the native setter so React's onChange fires
    // (using textarea.value = x directly is swallowed by React's
    // SyntheticEvent dedup).
    const nativeSetter = Object.getOwnPropertyDescriptor(
      HTMLTextAreaElement.prototype,
      "value"
    ).set;
    nativeSetter.call(textarea, String(args.prompt || ""));
    textarea.dispatchEvent(new Event("input", { bubbles: true }));
    textarea.focus();

    // Wait for React to re-render and enable the send button.
    await new Promise((r) => setTimeout(r, 600));

    // Find the send button. Multiple strategies:
    // A) form submit button
    // B) enabled button with SVG nearest textarea (the arrow-up icon)
    let sendBtn = null;
    const form = textarea.closest("form");
    if (form) {
      sendBtn = form.querySelector("button[type='submit']");
      if (sendBtn && sendBtn.disabled) sendBtn = null;
    }
    if (!sendBtn) {
      const scope =
        form ||
        textarea.closest("[class*='chat']") ||
        textarea.parentElement?.parentElement?.parentElement ||
        document.body;
      const btns = Array.from(scope.querySelectorAll("button"));
      // Prefer the LAST enabled button with an SVG — that's usually
      // the arrow-send icon in modern chat UIs.
      const candidates = btns.filter(
        (b) => !b.disabled && b.querySelector("svg")
      );
      sendBtn = candidates[candidates.length - 1] || null;
    }
    if (!sendBtn) {
      window.__grokAutomationActive = false;
      return { error: "no_send_button_found" };
    }

    // Click it!
    sendBtn.click();

    const label =
      sendBtn.getAttribute("aria-label") ||
      sendBtn.title ||
      sendBtn.textContent.trim().slice(0, 40) ||
      "svg-btn";
    return { ok: true, buttonLabel: label };
  }, { prompt });
}

async function grokReadAutomationState(tabId) {
  try {
    const result = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: () => ({
        active: !!window.__grokAutomationActive,
        videoUrl: window.__grokAutomationVideoUrl || "",
        videoId: window.__grokAutomationVideoId || "",
        progress: window.__grokAutomationProgress || 0,
        error: window.__grokAutomationError || "",
        startedAt: window.__grokAutomationStartedAt || 0,
        // Also scan for a <video> element with assets.grok.com src
        // (fallback when fetch stream capture somehow misses).
        domVideoUrl: (() => {
          try {
            const vs = Array.from(document.querySelectorAll("video"));
            for (const v of vs) {
              if (v.src && v.src.includes("assets.grok.com/users")) {
                return v.src;
              }
              const srcs = v.querySelectorAll("source");
              for (const s of srcs) {
                if (s.src && s.src.includes("assets.grok.com/users")) {
                  return s.src;
                }
              }
            }
          } catch {}
          return "";
        })(),
      }),
    });
    return (
      result?.[0]?.result || {
        active: false,
        videoUrl: "",
        videoId: "",
        progress: 0,
        error: "",
        startedAt: 0,
        domVideoUrl: "",
      }
    );
  } catch {
    return {
      active: false,
      videoUrl: "",
      videoId: "",
      progress: 0,
      error: "",
      startedAt: 0,
      domVideoUrl: "",
    };
  }
}

async function grokClearAutomationFlag(tabId) {
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: () => {
        window.__grokAutomationActive = false;
      },
    });
  } catch {}
}

async function grokReadCapturedHeaders(tabId) {
  try {
    const result = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: () => {
        return {
          headers: window.__grokLastHeaders || {},
          capturedAt: window.__grokHeadersCapturedAt || 0,
          stats: window.__grokCaptureStats || { totalCalls: 0, restCalls: 0 },
          patchInstalled: !!window.__grokFetchPatchInstalled,
        };
      },
    });
    return (
      result?.[0]?.result || {
        headers: {},
        capturedAt: 0,
        stats: { totalCalls: 0, restCalls: 0 },
        patchInstalled: false,
      }
    );
  } catch {
    return {
      headers: {},
      capturedAt: 0,
      stats: { totalCalls: 0, restCalls: 0 },
      patchInstalled: false,
    };
  }
}

// Replay EVERY captured header — Grok's anti-bot uses a combination
// of headers that we can't enumerate reliably (the v1.6.4 whitelist
// was too narrow: we sent only baggage/sentry-trace/traceparent/
// x-statsig-id and still got anti-bot rejection). The safer strategy
// is: replay anything the page attached, except the handful of
// headers the browser itself is going to add or override.
//
// Browser-forbidden / auto-set headers that must be excluded (setting
// them explicitly causes the fetch to throw or the value to be
// silently overwritten, so we drop them):
//   - content-length      (browser auto-computes from body)
//   - host, connection    (hop-by-hop, browser controls)
//   - cookie              (attached via credentials:"include")
//   - content-type        (we set our own application/json)
const GROK_BLOCKED_REPLAY_HEADERS = new Set([
  "content-length",
  "host",
  "connection",
  "cookie",
  "content-type",
  "transfer-encoding",
  "upgrade",
  "keep-alive",
]);

function grokPickReplayHeaders(captured) {
  const out = {};
  if (!captured) return out;
  for (const key of Object.keys(captured)) {
    const k = key.toLowerCase();
    if (GROK_BLOCKED_REPLAY_HEADERS.has(k)) continue;
    const v = captured[key];
    if (v !== undefined && v !== null && String(v).length > 0) {
      out[k] = String(v);
    }
  }
  return out;
}

// Generate a fresh UUIDv4 for x-xai-request-id on every dispatch.
// Reusing a captured request-id can trigger idempotency dedupe at
// Grok's backend. Browsers that don't expose crypto.randomUUID fall
// back to a Math.random-based generator (good enough for this use —
// the value just needs to be unique per request, not cryptographically
// strong).
function grokFreshRequestId() {
  try {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
  } catch {}
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// ═══════════════════════════════════════════════════════════════════
// Account detection
// ═══════════════════════════════════════════════════════════════════

async function grokDetectAccounts() {
  try {
    const tabs = await chrome.tabs.query({ url: `${GROK_ORIGIN}/*` });
    if (!tabs.length) {
      grokAccounts = {};
      return;
    }

    const fresh = {};
    for (const tab of tabs) {
      try {
        // Account detection — Grok's /rest/* endpoints for user identity
        // aren't public, so probing them produces a bunch of noisy 404s
        // in the user's DevTools console. Instead, we rely on signals
        // already present in the page:
        //   1. HTML body — Grok's UI embeds asset URLs that contain the
        //      user's UUID: https://assets.grok.com/users/<uuid>/<...>
        //   2. localStorage — Grok's Next.js bootstrap sometimes leaves
        //      userId under a predictable key
        // Either path gives us a user handle without any network noise.
        const result = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          world: "MAIN",
          func: async () => {
            let userId = "";
            let email = "";
            // Strategy 1: HTML body contains asset URLs with userId
            try {
              const html = document.documentElement?.innerHTML || "";
              const m = html.match(
                /assets\.grok\.com\/users\/([0-9a-f]{8}-[0-9a-f-]{27,})/i
              );
              if (m) userId = m[1];
            } catch {}
            // Strategy 2: localStorage scan for Grok-ish keys
            if (!userId) {
              try {
                for (let i = 0; i < localStorage.length; i++) {
                  const k = localStorage.key(i) || "";
                  const lk = k.toLowerCase();
                  if (lk.includes("user") || lk.includes("auth") || lk.includes("session")) {
                    const v = localStorage.getItem(k) || "";
                    const uidMatch = v.match(/"(?:id|userId|user_id)"\s*:\s*"([0-9a-f-]{36})"/i);
                    if (uidMatch) { userId = uidMatch[1]; break; }
                  }
                }
              } catch {}
            }
            // Strategy 3: page scripts may leave __NEXT_DATA__ with user info
            if (!userId) {
              try {
                const nd = document.getElementById("__NEXT_DATA__");
                if (nd && nd.textContent) {
                  const m = nd.textContent.match(
                    /"userId"\s*:\s*"([0-9a-f]{8}-[0-9a-f-]{27,})"/i
                  );
                  if (m) userId = m[1];
                }
              } catch {}
            }
            if (!userId) return { error: "not_logged_in" };
            // Look for email too (nice-to-have, rarely available)
            try {
              const html = document.documentElement?.innerHTML || "";
              const em = html.match(
                /"email"\s*:\s*"([^"<>\s]+@[^"<>\s]+)"/i
              );
              if (em) email = em[1];
            } catch {}
            // Subscription — look for common markers
            let sub = "";
            try {
              const html = document.documentElement?.innerHTML || "";
              if (/supergrok/i.test(html)) sub = "SuperGrok";
              else if (/premium\s*\+/i.test(html)) sub = "Premium+";
              else if (/premium/i.test(html)) sub = "Premium";
            } catch {}
            return {
              email: email || `grok_user_${userId.slice(0, 8)}`,
              userId,
              subscription: sub,
            };
          },
        });
        const info = result?.[0]?.result;
        if (info && !info.error) {
          fresh[info.email] = {
            email: info.email,
            userId: info.userId,
            subscription: info.subscription,
            tab_id: tab.id,
            last_seen: Date.now(),
          };
          // Install the fetch header capture into this tab's MAIN world.
          // Idempotent — re-runs are no-ops once installed, so safe to
          // call on every detection cycle.
          try {
            await grokInstallHeaderCapture(tab.id);
          } catch {}
        }
      } catch (e) {
        // Tab closed mid-probe or no access — skip silently.
      }
    }
    grokAccounts = fresh;

    // Report to bridge so Python side sees the accounts too
    if (Object.keys(fresh).length && grokBridgeConnected) {
      try {
        await fetch(`${GROK_BRIDGE_URL}/grok/accounts`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            accounts: Object.values(fresh).map((a) => ({
              email: a.email,
              userId: a.userId,
              subscription: a.subscription,
            })),
          }),
        });
      } catch {}
    }
  } catch (e) {
    console.warn("[Grok] Account detection failed:", e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════
// Bridge polling
// ═══════════════════════════════════════════════════════════════════

async function grokPollBridge() {
  try {
    const emails = Object.keys(grokAccounts);
    const accountsParam = emails.length
      ? `?accounts=${encodeURIComponent(emails.join(","))}`
      : "";

    const resp = await fetch(`${GROK_BRIDGE_URL}/grok/poll${accountsParam}`, {
      method: "GET",
      headers: { "Accept": "application/json" },
    });
    if (!resp.ok) {
      grokBridgeConnected = false;
      grokLastPollError = `HTTP ${resp.status}`;
      return;
    }
    grokBridgeConnected = true;
    grokLastPollError = "";
    const data = await resp.json();

    if (data.work && grokActiveCount < GROK_MAX_PARALLEL) {
      const rid = data.work.request_id;
      if (grokSubmittedRequestIds.has(rid) || grokInFlightRequestIds.has(rid)) {
        return;
      }
      grokInFlightRequestIds.add(rid);
      grokActiveCount++;
      (async () => {
        try {
          await grokHandleWork(data.work);
        } catch (e) {
          console.warn("[Grok] handleWork threw:", e.message);
          try {
            await grokSubmitResult(rid, { error: `handler_crash: ${e.message}` });
          } catch {}
        } finally {
          grokActiveCount--;
          grokInFlightRequestIds.delete(rid);
        }
      })();
    }
  } catch (e) {
    grokBridgeConnected = false;
    grokLastPollError = e.message || "fetch failed";
  }
}

// ═══════════════════════════════════════════════════════════════════
// Work execution — end-to-end video generation
// ═══════════════════════════════════════════════════════════════════

async function grokHandleWork(work) {
  const {
    request_id,
    account,
    prompt,
    // Reference image for image→video (Phase 2; text→video is fine for MVP)
    reference_image_base64,
    reference_image_filename,
  } = work;

  if (grokSubmittedRequestIds.has(request_id)) return;

  const info = grokAccounts[account];
  if (!info) {
    await grokSubmitResult(request_id, { error: "account_tab_not_found" });
    return;
  }
  const tabId = info.tab_id;

  try {
    await chrome.tabs.get(tabId);
  } catch {
    await grokSubmitResult(request_id, { error: "tab_closed" });
    return;
  }

  await grokReportProgress(request_id, "started", `account=${account}`);

  const hasReference = !!(reference_image_base64 && reference_image_filename);
  if (hasReference) {
    // Image→video via click-based flow would need to simulate the
    // "+ upload" picker which can't be done headlessly. Report a
    // clean error for now; Phase 2 will wire it up via a paste
    // event once we've confirmed text→video works end-to-end.
    await grokSubmitResult(request_id, {
      error: "image_to_video_not_supported_yet",
      detail:
        "Grok's file picker can't be automated headlessly. Text-to-video " +
        "works via click-based dispatch — retry without a reference image.",
    });
    return;
  }

  // ─────────────────────────────────────────────────────────────
  // Click-based dispatch — type prompt into textarea and click send.
  // Grok's own JS then fires /rest/app-chat/conversations/new with a
  // FRESH x-statsig-id (which is what the backend's anti-bot checks).
  // We tee the response stream via the content-script fetch patch and
  // poll for the resulting videoUrl.
  // ─────────────────────────────────────────────────────────────
  const userPrompt = String(prompt || "").trim();
  if (!userPrompt) {
    await grokSubmitResult(request_id, { error: "empty_prompt" });
    return;
  }

  await grokReportProgress(request_id, "clicking_send", "");
  const clickResult = await grokClickSend(tabId, userPrompt);
  if (!clickResult || clickResult.error) {
    await grokSubmitResult(request_id, {
      error: `click_failed_${clickResult?.error || "unknown"}`,
      detail:
        "Could not locate textarea or send button on grok.com/imagine. " +
        "Make sure the tab is open to /imagine and Video mode is selected.",
    });
    return;
  }
  await grokReportProgress(
    request_id,
    "clicked",
    clickResult.buttonLabel || "send"
  );

  // Poll for videoUrl — either from the fetch-wrapper's stream capture
  // (__grokAutomationVideoUrl) or from the DOM (<video> element src).
  // Grok typically finishes a 10s/720p video in 60-180 seconds.
  let videoUrl = "";
  let videoId = "";
  let progressPct = 0;
  let domFallbackUrl = "";
  let errorSeen = "";
  const pollDeadline = Date.now() + 240000; // 4 min safety cap
  let lastReportedProgress = -1;

  while (Date.now() < pollDeadline) {
    const state = await grokReadAutomationState(tabId);
    if (state.error) {
      errorSeen = state.error;
      break;
    }
    if (state.videoUrl) {
      videoUrl = state.videoUrl;
      videoId = state.videoId;
      break;
    }
    if (state.domVideoUrl && !domFallbackUrl) {
      domFallbackUrl = state.domVideoUrl;
    }
    if (typeof state.progress === "number" && state.progress !== lastReportedProgress) {
      progressPct = state.progress;
      lastReportedProgress = state.progress;
      if (state.progress > 0 && state.progress % 25 === 0) {
        await grokReportProgress(
          request_id,
          "progress",
          `${state.progress}%`
        );
      }
    }
    await new Promise((r) => setTimeout(r, 2000));
  }

  await grokClearAutomationFlag(tabId);

  if (errorSeen) {
    await grokSubmitResult(request_id, {
      error: `grok_${errorSeen}`,
      progress: progressPct,
    });
    return;
  }
  if (!videoUrl && domFallbackUrl) {
    videoUrl = domFallbackUrl;
    await grokReportProgress(request_id, "video_ready_from_dom", videoUrl);
  }
  if (!videoUrl) {
    await grokSubmitResult(request_id, {
      error: "video_timeout",
      progress: progressPct,
      detail:
        `Waited 4 min but no video URL appeared. Last progress: ${progressPct}%. ` +
        "Grok may be slow or rate-limited. Try again.",
    });
    return;
  }

  await grokReportProgress(request_id, "video_ready", videoUrl);

  // Capture the postId (parentPostId) from DOM if available — useful
  // for debugging, not required for download.
  const postId = videoId || ""; // fallback to videoId
  const animateResult = { videoUrl, videoId, mediaName: "", progress: progressPct };

  // ─────────────────────────────────────────────────────────────
  // Step 4: Fetch the video bytes (done from the tab so cookies work)
  //        Grok serves videos from assets.grok.com — credential same-site
  // ─────────────────────────────────────────────────────────────
  // The URL returned may be relative ("users/...") or absolute. Normalize.
  let fullVideoUrl = animateResult.videoUrl;
  if (!/^https?:\/\//i.test(fullVideoUrl)) {
    fullVideoUrl = `${GROK_ASSETS_ORIGIN}/${fullVideoUrl.replace(/^\/+/, "")}`;
  }

  const downloadResult = await grokExecInTab(tabId, async (args) => {
    try {
      const r = await fetch(args.url, {
        method: "GET",
        credentials: "include",
      });
      if (!r.ok) return { status: r.status, error: "download_http" };
      const buf = await r.arrayBuffer();
      // Convert ArrayBuffer → base64 (chunked to avoid call-stack limits)
      const bytes = new Uint8Array(buf);
      let binary = "";
      const CHUNK = 0x8000;
      for (let i = 0; i < bytes.length; i += CHUNK) {
        binary += String.fromCharCode.apply(
          null, bytes.subarray(i, i + CHUNK)
        );
      }
      const b64 = btoa(binary);
      return { status: 200, size: bytes.length, content_base64: b64 };
    } catch (e) {
      return { status: 0, error: `fetch_exc_${e?.message || e}` };
    }
  }, { url: fullVideoUrl });

  if (!downloadResult || downloadResult.status !== 200 || !downloadResult.content_base64) {
    await grokSubmitResult(request_id, {
      error: `download_failed_${downloadResult?.status || "?"}`,
      video_url: fullVideoUrl,
    });
    return;
  }

  // Success — send video bytes back to bridge
  await grokSubmitResult(request_id, {
    success: true,
    video_url: fullVideoUrl,
    video_id: animateResult.videoId,
    post_id: postId,
    size_bytes: downloadResult.size,
    content_base64: downloadResult.content_base64,
    mime_type: "video/mp4",
  });
  grokSubmittedRequestIds.add(request_id);
}

// ═══════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════

// Run a function inside the grok.com tab's MAIN world so that page-bound
// cookies + Grok's own fetch overrides (statsig header etc.) apply
// automatically.
async function grokExecInTab(tabId, fn, args) {
  try {
    const out = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: fn,
      args: args !== undefined ? [args] : [],
    });
    return out?.[0]?.result ?? null;
  } catch (e) {
    console.warn("[Grok] exec in tab failed:", e.message);
    return null;
  }
}

async function grokReportProgress(request_id, stage, detail) {
  try {
    await fetch(`${GROK_BRIDGE_URL}/grok/progress`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request_id, stage, detail: String(detail || "") }),
    });
  } catch {}
}

async function grokSubmitResult(request_id, payload) {
  try {
    await fetch(`${GROK_BRIDGE_URL}/grok/work-result`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request_id, ...payload }),
    });
  } catch (e) {
    console.warn("[Grok] submit result failed:", e.message);
  }
}

// Public status getter (consumed by popup.js)
function grokGetStatus() {
  return {
    connected: grokBridgeConnected,
    accounts: Object.values(grokAccounts).map((a) => ({
      email: a.email,
      subscription: a.subscription || "",
    })),
    lastError: grokLastPollError,
    active: grokActiveCount,
  };
}

// Exposed so background.js can start the module after importScripts
function grokStart() {
  console.log("[Grok] Module starting — bridge:", GROK_BRIDGE_URL);
  setInterval(grokPollBridge, GROK_POLL_INTERVAL);
  setInterval(grokDetectAccounts, GROK_ACCOUNT_DETECT_INTERVAL);
  // Initial detect after tabs load
  setTimeout(grokDetectAccounts, 2500);
}

// Expose on self so background.js can call these
self.grokStart = grokStart;
self.grokGetStatus = grokGetStatus;
