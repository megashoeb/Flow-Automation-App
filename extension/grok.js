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
        // Grok's user info lives in /rest/rate-limits or the page's
        // bootstrap data. Simplest reliable probe: hit /rest/rate-limits
        // which returns the logged-in user context.
        const result = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          world: "MAIN",
          func: async () => {
            // Probe multiple likely endpoints — API names rotate and we
            // want a cheap way to confirm login + extract userId.
            const endpoints = [
              "/rest/user/me",
              "/rest/auth/me",
              "/rest/users/me",
              "/rest/app-chat/users/me",
              "/rest/account/me",
            ];
            let userInfo = null;
            for (const ep of endpoints) {
              try {
                const r = await fetch(ep, {
                  method: "GET",
                  credentials: "include",
                  headers: { "Accept": "application/json" },
                });
                if (!r.ok) continue;
                const ctype = r.headers.get("content-type") || "";
                if (!ctype.includes("json")) continue;
                const data = await r.json().catch(() => null);
                if (!data) continue;
                const u = data?.user || data?.data || data;
                const email = u?.email || u?.userEmail || u?.username || "";
                const userId = u?.id || u?.userId || u?.user_id || "";
                if (email || userId) {
                  userInfo = {
                    email: email || `grok_user_${String(userId).slice(0, 8)}`,
                    userId: String(userId || ""),
                    subscription: String(u?.subscription || u?.tier || u?.plan || ""),
                  };
                  break;
                }
              } catch {}
            }
            // Fallback — parse userId from any recent asset URL in the page
            if (!userInfo) {
              try {
                const match = document.body?.innerHTML?.match(
                  /assets\.grok\.com\/users\/([0-9a-f-]{36})/i
                );
                if (match) {
                  userInfo = {
                    email: `grok_user_${match[1].slice(0, 8)}`,
                    userId: match[1],
                    subscription: "",
                  };
                }
              } catch {}
            }
            return userInfo || { error: "not_logged_in" };
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
        headers: { "Content-Type": "application/json" },
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
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args.body),
    });
    const status = r.status;
    const text = await r.text();
    let parsed = null;
    try { parsed = JSON.parse(text); } catch {}
    return { status, text: parsed ? null : text, data: parsed };
  }, { body: createBody });

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

  // Execute the streaming fetch inside the tab. The result: we read the
  // whole NDJSON stream (it's bounded — Grok finishes within 60-180s) and
  // parse out the final videoUrl.
  const animateResult = await grokExecInTab(tabId, async (args) => {
    const r = await fetch("/rest/app-chat/conversations/new", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
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
