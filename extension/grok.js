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

// Headers that Grok checks at the backend for its anti-bot system. Only
// these are replayed — we deliberately do NOT replay Content-Type (we
// set our own), Content-Length (browser auto-computes), or cookies
// (already attached via credentials:"include").
const GROK_REPLAY_HEADER_KEYS = [
  "x-statsig-id",
  "x-xai-request-id",
  "x-xai-auth",
  "sentry-trace",
  "baggage",
  "accept-language",
];

function grokPickReplayHeaders(captured) {
  const out = {};
  if (!captured) return out;
  for (const key of GROK_REPLAY_HEADER_KEYS) {
    const v = captured[key];
    if (v !== undefined && v !== null && String(v).length > 0) {
      out[key] = String(v);
    }
  }
  return out;
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
    // Optional — if user supplied a reference image, bridge sends bytes
    // as base64 + filename + mime so we can POST to /upload-file.
    reference_image_base64,
    reference_image_filename,
    reference_image_mime,
    // Video settings
    aspect_ratio = "16:9",
    video_length = 10,
    resolution = "720p",
    mode = "custom",
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

  // Make sure the fetch header capture is installed, then read the
  // latest snapshot of anti-bot headers Grok's own code has attached.
  try {
    await grokInstallHeaderCapture(tabId);
  } catch {}

  let captured = await grokReadCapturedHeaders(tabId);
  if (!captured.headers["x-statsig-id"]) {
    // No x-statsig-id yet — Grok only attaches this on /rest/app-chat
    // or /rest/media calls, and those happen only when the user takes
    // an action. On a freshly-loaded /imagine page with no interaction
    // we won't see one. Wait briefly in case the page is still booting
    // (Statsig SDK fires a /initialize call on load in some versions)
    // then read again.
    await new Promise((r) => setTimeout(r, 1500));
    captured = await grokReadCapturedHeaders(tabId);
  }

  const replayHeaders = grokPickReplayHeaders(captured.headers);
  const hasAntiBotHeaders = !!replayHeaders["x-statsig-id"];
  const stats = captured.stats || { totalCalls: 0, restCalls: 0 };
  const diagDetail = hasAntiBotHeaders
    ? `ok (from ${stats.restCalls || 0} rest calls)`
    : `MISSING — patch=${captured.patchInstalled ? "yes" : "no"}, ` +
      `fetches=${stats.totalCalls || 0}, rest=${stats.restCalls || 0}. ` +
      `→ generate 1 video manually on grok.com/imagine first, then retry`;
  await grokReportProgress(request_id, "headers_captured", diagDetail);

  if (!hasAntiBotHeaders) {
    // Fail fast with an actionable error — no point running upload +
    // create post just to be rejected at animate.
    await grokSubmitResult(request_id, {
      error: "no_antibot_headers_captured",
      detail:
        "Open grok.com/imagine and manually generate 1 video (any prompt) " +
        "so the extension can capture Grok's anti-bot headers. Then click " +
        "Start Automation again. This warmup is only needed once per tab.",
    });
    return;
  }

  // ─────────────────────────────────────────────────────────────
  // Step 1 (optional): Upload reference image
  // ─────────────────────────────────────────────────────────────
  let fileMetadataId = "";
  let mediaUrl = "";
  const hasReference = !!(reference_image_base64 && reference_image_filename);

  if (hasReference) {
    await grokReportProgress(request_id, "uploading_image", reference_image_filename);
    const uploadResult = await grokExecInTab(tabId, async (args) => {
      const r = await fetch("/rest/app-chat/upload-file", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json", ...(args.replayHeaders || {}) },
        body: JSON.stringify({
          fileName: args.fileName,
          fileMimeType: args.fileMimeType,
          fileSource: "IMAGINE_SELF_UPLOAD_FILE_SOURCE",
          content: args.content,
        }),
      });
      const status = r.status;
      const text = await r.text();
      let parsed = null;
      try { parsed = JSON.parse(text); } catch {}
      return { status, text: parsed ? null : text, data: parsed };
    }, {
      fileName: reference_image_filename,
      fileMimeType: reference_image_mime || "image/jpeg",
      content: reference_image_base64,
      replayHeaders,
    });

    if (!uploadResult || uploadResult.status !== 200 || !uploadResult.data) {
      const errBody = uploadResult?.text || JSON.stringify(uploadResult?.data || {});
      await grokSubmitResult(request_id, {
        error: `upload_failed_${uploadResult?.status || "?"}`,
        response_body: String(errBody).slice(0, 500),
      });
      return;
    }
    fileMetadataId = uploadResult.data.fileMetadataId || "";
    const fileUri = uploadResult.data.fileUri || "";
    if (!fileMetadataId || !fileUri) {
      await grokSubmitResult(request_id, {
        error: "upload_no_id",
        response_body: JSON.stringify(uploadResult.data).slice(0, 500),
      });
      return;
    }
    mediaUrl = `${GROK_ASSETS_ORIGIN}/${fileUri}`;
    await grokReportProgress(request_id, "image_uploaded", fileMetadataId);
  }

  // ─────────────────────────────────────────────────────────────
  // Step 2: Create post
  // ─────────────────────────────────────────────────────────────
  await grokReportProgress(request_id, "creating_post", hasReference ? "image" : "video");
  const createBody = hasReference
    ? { mediaType: "MEDIA_POST_TYPE_IMAGE", mediaUrl }
    : { mediaType: "MEDIA_POST_TYPE_VIDEO", prompt: String(prompt || "") };

  const createResult = await grokExecInTab(tabId, async (args) => {
    const r = await fetch("/rest/media/post/create", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json", ...(args.replayHeaders || {}) },
      body: JSON.stringify(args.body),
    });
    const status = r.status;
    const text = await r.text();
    let parsed = null;
    try { parsed = JSON.parse(text); } catch {}
    return { status, text: parsed ? null : text, data: parsed };
  }, { body: createBody, replayHeaders });

  if (!createResult || createResult.status !== 200 || !createResult.data) {
    const errBody = createResult?.text || JSON.stringify(createResult?.data || {});
    await grokSubmitResult(request_id, {
      error: `create_post_failed_${createResult?.status || "?"}`,
      response_body: String(errBody).slice(0, 500),
    });
    return;
  }
  const postId = createResult.data?.post?.id || fileMetadataId || "";
  if (!postId) {
    await grokSubmitResult(request_id, {
      error: "create_post_no_id",
      response_body: JSON.stringify(createResult.data).slice(0, 500),
    });
    return;
  }
  await grokReportProgress(request_id, "post_created", postId);

  // ─────────────────────────────────────────────────────────────
  // Step 3: Animate — POST /rest/app-chat/conversations/new
  //        Response is streaming NDJSON with progress + final URL
  // ─────────────────────────────────────────────────────────────
  const userPrompt = String(prompt || "").trim();
  let messageField;
  if (hasReference) {
    // Image-to-video — URL + prompt (or "animate" fallback) + mode flag
    const verb = userPrompt || "animate";
    messageField = `${mediaUrl}  ${verb} --mode=${mode}`;
  } else {
    messageField = `${userPrompt} --mode=${mode}`;
  }

  const convoBody = {
    temporary: true,
    modelName: "imagine-video-gen",
    message: messageField,
    ...(hasReference ? { fileAttachments: [postId] } : {}),
    enableSideBySide: true,
    responseMetadata: {
      experiments: [],
      modelConfigOverride: {
        modelMap: {
          videoGenModelConfig: {
            parentPostId: postId,
            aspectRatio: String(aspect_ratio),
            videoLength: Number(video_length),
            resolutionName: String(resolution),
          },
        },
      },
    },
  };

  await grokReportProgress(request_id, "animating", `${aspect_ratio} ${resolution} ${video_length}s`);

  // Refresh captured headers right before the expensive call — Grok
  // rotates some per-request values and we want the freshest snapshot.
  try {
    const freshCap = await grokReadCapturedHeaders(tabId);
    if (freshCap && freshCap.capturedAt) {
      const merged = grokPickReplayHeaders(freshCap.headers);
      Object.assign(replayHeaders, merged);
    }
  } catch {}

  // Execute the streaming fetch inside the tab. The result: we read the
  // whole NDJSON stream (it's bounded — Grok finishes within 60-180s) and
  // parse out the final videoUrl.
  const animateResult = await grokExecInTab(tabId, async (args) => {
    // Strip any x-xai-request-id / sentry-trace / baggage from the
    // replay headers — those are per-request and stale values fail
    // anti-bot validation. Keep only the stable ones (x-statsig-id,
    // x-xai-auth, accept-language).
    const replay = { ...(args.replayHeaders || {}) };
    delete replay["x-xai-request-id"];
    delete replay["sentry-trace"];
    delete replay["baggage"];

    const r = await fetch("/rest/app-chat/conversations/new", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json", ...replay },
      body: JSON.stringify(args.body),
    });
    const status = r.status;
    if (!r.ok) {
      const errText = await r.text();
      return { status, error: errText.slice(0, 1000) };
    }
    // Stream the NDJSON body
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let progressPct = 0;
    let videoUrl = "";
    let videoId = "";
    let mediaName = "";
    let workflowFinal = false;
    let errorSeen = "";

    // Safety: max 4 minutes of streaming
    const deadline = Date.now() + 240000;

    while (true) {
      if (Date.now() > deadline) {
        return { status, error: "stream_timeout", progress: progressPct };
      }
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
        // Extract progress
        const svgr = resp.streamingVideoGenerationResponse;
        if (svgr) {
          if (typeof svgr.progress === "number") progressPct = svgr.progress;
          if (svgr.videoId) videoId = svgr.videoId;
          if (svgr.videoUrl) videoUrl = svgr.videoUrl;
          if (svgr.moderated) errorSeen = "moderated";
        }
        // Fallback extraction
        if (resp.modelResponse?.responseId) {
          workflowFinal = true;
        }
        if (resp.finalMetadataMap) {
          const m = resp.finalMetadataMap?.videoGenModelConfig;
          if (m?.videoUrl) videoUrl = m.videoUrl;
        }
      }
      if (videoUrl && workflowFinal) break;
    }
    return { status, progress: progressPct, videoUrl, videoId, mediaName, error: errorSeen };
  }, { body: convoBody });

  if (!animateResult) {
    await grokSubmitResult(request_id, { error: "animate_no_result" });
    return;
  }
  if (animateResult.error && !animateResult.videoUrl) {
    await grokSubmitResult(request_id, {
      error: `animate_${animateResult.error}`,
      http_status: animateResult.status,
    });
    return;
  }
  if (!animateResult.videoUrl) {
    await grokSubmitResult(request_id, {
      error: "no_video_url",
      progress: animateResult.progress,
    });
    return;
  }

  await grokReportProgress(request_id, "video_ready", animateResult.videoUrl);

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
