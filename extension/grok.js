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
// grokAccounts is keyed by email. Each account may have ONE OR MORE
// grok.com/imagine tabs open — that's how the user scales
// parallelism (one tab per concurrent slot). The structure now holds
// a `tab_ids` array; `tab_id` is kept as a deprecated alias for
// backwards compat with any old dispatcher code paths.
let grokAccounts = {};
let grokLastPollError = "";
let grokActiveCount = 0;

// Per-tab busy lock. When a job is mid-click/polling on a tab, set
// grokTabBusy[tabId] = true so concurrent worker slots don't stomp on
// each other's window.__grokAutomationVideoUrl / __grokAutomationActive
// flags. Cleared after the job completes or times out.
const grokTabBusy = {};

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

// ═══════════════════════════════════════════════════════════════════
// Page navigation — each successful generation leaves the tab at
// /imagine/post/<id> (the post detail view). That page has a
// different composer — the Compose Post button targets a REPLY flow,
// not a fresh /rest/app-chat/conversations/new. Before every dispatch
// we make sure the tab is on the bare /imagine page so the next
// prompt starts a clean new generation.
//
// Returns { ok, changed, url } or { error }.
// ═══════════════════════════════════════════════════════════════════

async function grokEnsureOnImaginePage(tabId) {
  const pathOf = (url) => {
    try { return new URL(url).pathname; } catch { return ""; }
  };
  const isImagineHome = (url) => {
    const p = pathOf(url);
    return p === "/imagine" || p === "/imagine/";
  };

  try {
    const tab = await chrome.tabs.get(tabId);
    const url = tab.url || "";
    if (isImagineHome(url)) {
      return { ok: true, changed: false, url };
    }
    if (!url.startsWith(GROK_ORIGIN)) {
      return { error: `unexpected_url: ${url.slice(0, 80)}` };
    }

    // Strategy 1: script-based hard navigation via window.location.
    // This is the most reliable for SPAs — it forces a full page
    // reload instead of Next.js client-side routing, so our content
    // script reinjects cleanly and there's no stale React state.
    let navStarted = false;
    try {
      const scriptRes = await grokExecInTab(tabId, () => {
        try {
          window.location.replace("/imagine");
          return { ok: true };
        } catch (e) {
          return { error: String(e?.message || e) };
        }
      });
      if (scriptRes?.ok) navStarted = true;
    } catch {}

    // Strategy 2: fallback to chrome.tabs.update if the script-based
    // nav didn't register (e.g. tab was in a weird state).
    if (!navStarted) {
      try {
        await chrome.tabs.update(tabId, { url: `${GROK_ORIGIN}/imagine` });
        navStarted = true;
      } catch (e) {
        return {
          error: `nav_kick_failed: ${String(e?.message || e).slice(0, 120)}`,
        };
      }
    }

    // Poll for the URL to reflect /imagine. We accept ANY URL whose
    // pathname is /imagine even if status is still "loading" — on
    // SPAs the URL can update before status flips, and on hard
    // reloads we'll get another round once the new page finishes.
    // 30-second budget (prior 20s was tight for SPAs under cold cache).
    const deadline = Date.now() + 30000;
    let lastUrl = url;
    let urlReachedImagine = false;
    let completeAfterImagine = false;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 400));
      let refreshed;
      try {
        refreshed = await chrome.tabs.get(tabId);
      } catch {
        return { error: "tab_closed_during_nav" };
      }
      lastUrl = refreshed.url || lastUrl;
      if (isImagineHome(lastUrl)) {
        urlReachedImagine = true;
        if (refreshed.status === "complete") {
          completeAfterImagine = true;
          break;
        }
      }
    }

    if (!urlReachedImagine) {
      return {
        error: `navigation_timeout (last_url=${lastUrl.slice(0, 80)})`,
      };
    }

    // Extra settle time — React hydration + content-script re-install
    // + any Statsig SDK warm-up. Even if status never hit "complete"
    // (rare, but possible when DevTools is closed), the URL is on
    // /imagine so the composer should be usable after a beat.
    await new Promise((r) =>
      setTimeout(r, completeAfterImagine ? 2000 : 3000)
    );
    return { ok: true, changed: true, url: lastUrl };
  } catch (e) {
    return { error: `nav_err: ${String(e?.message || e).slice(0, 120)}` };
  }
}

// ═══════════════════════════════════════════════════════════════════
// Image attachment — injects a File into Grok's composer UI so the
// subsequent send click produces a proper image-to-video request (one
// with fileAttachments set in the conversations/new body).
//
// Two strategies, tried in order:
//   1. File-input injection — find <input type="file">, set files via
//      DataTransfer, fire change event. Works for every React
//      dropzone library since they all hang their "+ upload" button
//      off a hidden file input under the hood.
//   2. DragEvent drop — construct a drop event with DataTransfer and
//      dispatch on the composer. Fallback if no hidden input found.
//
// After injection, polls the DOM for the attachment thumbnail to
// appear (confirms Grok's UI state registered the file) before
// returning. Caller should then set the prompt text and click send.
// ═══════════════════════════════════════════════════════════════════

async function grokAttachImage(tabId, base64, filename, mime) {
  return grokExecInTab(tabId, async (args) => {
    // Rebuild the File object from base64 bytes.
    const bin = atob(args.base64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    const file = new File([bytes], args.filename, {
      type: args.mime || "image/jpeg",
    });

    // Find a visible composer input so we can scope our search.
    const composerInput =
      document.querySelector('textarea') ||
      document.querySelector('[contenteditable="true"]') ||
      document.querySelector('[contenteditable=""]');

    // ─── Strategy 1: hidden <input type="file"> ───
    const fileInputs = Array.from(
      document.querySelectorAll('input[type="file"]')
    );
    let targetInput = null;
    if (fileInputs.length === 1) {
      targetInput = fileInputs[0];
    } else if (fileInputs.length > 1) {
      // Prefer the one closest to the composer (in the DOM)
      if (composerInput) {
        let best = null;
        let bestDist = Infinity;
        for (const fi of fileInputs) {
          let d = 0;
          let node = composerInput;
          while (node) {
            if (node.contains(fi)) break;
            node = node.parentElement;
            d++;
          }
          if (node && d < bestDist) {
            bestDist = d;
            best = fi;
          }
        }
        targetInput = best || fileInputs[0];
      } else {
        targetInput = fileInputs[0];
      }
    }

    let strategy = "";
    if (targetInput) {
      try {
        const dt = new DataTransfer();
        dt.items.add(file);
        // The `files` property on HTMLInputElement has a non-writable
        // descriptor; use the native setter to bypass.
        const setter = Object.getOwnPropertyDescriptor(
          HTMLInputElement.prototype,
          "files"
        )?.set;
        if (setter) {
          setter.call(targetInput, dt.files);
        } else {
          targetInput.files = dt.files;
        }
        targetInput.dispatchEvent(new Event("input", { bubbles: true }));
        targetInput.dispatchEvent(new Event("change", { bubbles: true }));
        strategy = "file_input";
      } catch (e) {
        strategy = `file_input_failed: ${String(e?.message || e).slice(0, 120)}`;
      }
    }

    // ─── Strategy 2: DragEvent drop (fallback) ───
    if (strategy !== "file_input") {
      try {
        const dt = new DataTransfer();
        dt.items.add(file);
        const target =
          composerInput?.closest("form") ||
          composerInput?.closest("[class*='dropzone']") ||
          composerInput?.closest("[class*='compose']") ||
          composerInput?.parentElement ||
          document.body;
        for (const type of ["dragenter", "dragover", "drop"]) {
          target.dispatchEvent(
            new DragEvent(type, {
              bubbles: true,
              cancelable: true,
              dataTransfer: dt,
            })
          );
        }
        strategy = strategy ? `${strategy}->drop` : "drop";
      } catch (e) {
        return {
          error: "both_strategies_failed",
          detail: String(e?.message || e).slice(0, 200),
        };
      }
    }

    // ─── Wait for the UI to acknowledge the attachment ───
    // Heuristics: look for a new <img> with blob: URL (thumbnail
    // preview), an asset URL, or the filename text appearing anywhere
    // in the composer region. Up to 12 seconds (upload time + UI tick).
    const startTs = Date.now();
    const deadline = startTs + 12000;
    let attached = false;
    let attachSignal = "";
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 400));
      // 1) Blob-URL preview image (most dropzones create these)
      const blobImg = Array.from(document.querySelectorAll("img")).find(
        (img) =>
          img.src &&
          (img.src.startsWith("blob:") ||
            img.src.includes("/users/0"))
      );
      if (blobImg) {
        attached = true;
        attachSignal = "blob_img";
        break;
      }
      // 2) Any new element containing the filename (stripped of ext)
      const baseName = (args.filename || "").replace(/\.[^.]+$/, "");
      if (baseName && baseName.length > 3) {
        const bodyText = (document.body.innerText || "").slice(0, 10000);
        if (bodyText.includes(baseName)) {
          attached = true;
          attachSignal = "filename_text";
          break;
        }
      }
    }

    return {
      ok: attached,
      strategy,
      attachSignal,
      waited_ms: Date.now() - startTs,
      fileInputsFound: fileInputs.length,
    };
  }, { base64, filename, mime });
}

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

    // ─── Find the prompt input ───
    // Grok's /imagine may use a <textarea>, a contenteditable div
    // (ProseMirror / Lexical style), or a text <input>. We try all
    // three, preferring visible inputs whose placeholder / aria-label
    // / data-placeholder matches "imagine" or "prompt".
    const candidates = [];
    for (const el of document.querySelectorAll("textarea")) {
      candidates.push({ el, type: "textarea" });
    }
    for (const el of document.querySelectorAll('[contenteditable="true"],[contenteditable=""]')) {
      candidates.push({ el, type: "contenteditable" });
    }
    for (const el of document.querySelectorAll('input[type="text"],input:not([type])')) {
      candidates.push({ el, type: "input" });
    }

    const visible = candidates.filter(({ el }) => {
      try {
        if (el.disabled) return false;
        if (el.offsetParent === null) return false;
        const r = el.getBoundingClientRect();
        return r.width > 40 && r.height > 15;
      } catch {
        return false;
      }
    });

    // Score each candidate — prefer placeholder/aria match.
    const scored = visible.map((c) => {
      const el = c.el;
      const ph = (
        el.placeholder ||
        el.getAttribute("data-placeholder") ||
        el.getAttribute("aria-label") ||
        el.getAttribute("aria-placeholder") ||
        ""
      ).toLowerCase();
      let score = 0;
      if (/imagine/.test(ph)) score += 10;
      if (/prompt|type|message|ask/.test(ph)) score += 5;
      if (c.type === "contenteditable") score += 2;
      if (c.type === "textarea") score += 1;
      return { ...c, score, placeholder: ph };
    });
    scored.sort((a, b) => b.score - a.score);
    const chosen = scored[0];

    if (!chosen) {
      window.__grokAutomationActive = false;
      return {
        error: "no_input_found",
        debug: {
          totalCandidates: candidates.length,
          visibleCount: visible.length,
          sampleTags: candidates.slice(0, 3).map((c) => `${c.type}`).join(","),
        },
      };
    }

    const inputEl = chosen.el;
    const inputType = chosen.type;

    // ─── Set the value ───
    try {
      inputEl.focus();
      if (inputType === "textarea" || inputType === "input") {
        const proto =
          inputType === "textarea" ? HTMLTextAreaElement : HTMLInputElement;
        const setter = Object.getOwnPropertyDescriptor(
          proto.prototype,
          "value"
        ).set;
        setter.call(inputEl, String(args.prompt || ""));
        inputEl.dispatchEvent(new Event("input", { bubbles: true }));
      } else {
        // contenteditable — clear + insertText via execCommand which
        // produces a real InputEvent that React/ProseMirror listen to.
        // This is the robust way to feed text into modern chat inputs.
        const sel = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(inputEl);
        sel.removeAllRanges();
        sel.addRange(range);
        try {
          document.execCommand("delete", false);
        } catch {}
        try {
          document.execCommand("insertText", false, String(args.prompt || ""));
        } catch {
          // Fallback if execCommand is disabled
          inputEl.textContent = String(args.prompt || "");
          inputEl.dispatchEvent(
            new InputEvent("beforeinput", {
              bubbles: true,
              cancelable: true,
              inputType: "insertText",
              data: String(args.prompt || ""),
            })
          );
          inputEl.dispatchEvent(
            new InputEvent("input", {
              bubbles: true,
              inputType: "insertText",
              data: String(args.prompt || ""),
            })
          );
        }
      }
    } catch (e) {
      window.__grokAutomationActive = false;
      return {
        error: "set_input_failed",
        detail: String(e?.message || e).slice(0, 200),
      };
    }

    // Wait longer so React re-renders AND the send button transitions
    // from disabled (empty composer) to enabled (prompt + optional
    // attachment). Upload post-processing can take another beat after
    // the thumbnail appears.
    await new Promise((r) => setTimeout(r, 1500));

    // ─── Find the send button ───
    // Grok's /imagine composer has many buttons clustered near the
    // input: "+ upload", "Image/Video" mode toggle, "480p/720p"
    // resolution, "6s/10s" duration, aspect ratio picker, and the
    // send arrow (rightmost). Earlier versions picked the last SVG
    // button in DOM order — that turned out to be the "Video" mode
    // toggle, not the send arrow, because mode toggles appear AFTER
    // the send button in source order on some layouts.
    //
    // Better strategy: score every enabled button in the composer
    // region by likelihood of being the SEND button.
    const scope =
      inputEl.closest("form") ||
      inputEl.closest("[class*='chat']") ||
      inputEl.closest("[class*='compose']") ||
      inputEl.parentElement?.parentElement?.parentElement?.parentElement ||
      document.body;

    // Widen the candidate pool. Include <button>, divs with role=button
    // (Grok's composer may use either), and also disabled buttons so we
    // can SEE them in debug — they're penalized but visible, which
    // turned out to matter when the real send button was disabled at
    // detection time on earlier runs.
    const btnSelector =
      'button,[role="button"],div[tabindex]:not([tabindex="-1"])';
    const allBtnsRaw = Array.from(scope.querySelectorAll(btnSelector));
    const inputRect = inputEl.getBoundingClientRect();
    const docBtnsRaw = Array.from(document.querySelectorAll(btnSelector));
    const nearbyBtns = docBtnsRaw.filter((b) => {
      try {
        const r = b.getBoundingClientRect();
        return (
          Math.abs(r.top - inputRect.top) < 300 ||
          Math.abs(r.bottom - inputRect.bottom) < 300
        );
      } catch {
        return false;
      }
    });
    const btnsUnique = Array.from(new Set([...allBtnsRaw, ...nearbyBtns]));

    // ─── Priority pass: explicit aria-label match ───
    // If any element's aria-label / title is EXACTLY one of these
    // send-intent names, prefer it immediately (unless disabled).
    // This short-circuits the scoring and handles the case where the
    // heuristics get confused by a cluster of icon-only action
    // buttons (Save/Share/Download) near the composer.
    const SEND_INTENT_NAMES = [
      "submit",
      "send",
      "send message",
      "create video",
      "create",
      "generate",
      "generate video",
      "imagine",
      "imagine it",
      "go",
      "post",
    ];
    let explicitSend = null;
    for (const b of btnsUnique) {
      if (
        b.disabled ||
        b.getAttribute("aria-disabled") === "true" ||
        b.getAttribute("disabled") !== null
      )
        continue;
      const a = (b.getAttribute("aria-label") || "").toLowerCase().trim();
      const t = (b.title || "").toLowerCase().trim();
      if (!a && !t) continue;
      if (SEND_INTENT_NAMES.includes(a) || SEND_INTENT_NAMES.includes(t)) {
        explicitSend = b;
        break;
      }
    }

    const MODE_TOKENS = /\b(image|video|photo|audio|480p|720p|1080p|4k|\d+s\b|6s|10s|\d+:\d+|ratio|square|landscape|portrait|widescreen|vertical|horizontal)\b/i;

    // Strong negatives — buttons whose label matches any of these are
    // definitely NOT the send button. Dropped word-boundary suffix so
    // "saved", "saving", "liked", "sharing", "downloaded" etc. ALSO
    // match (1.8.2's \b...\b let "saved" sneak through as the winner).
    const NOT_SEND_TOKENS =
      /\b(sav|bookmark|favorit|heart|lik|shar|download|cop|past|edit|renam|delet|remov|trash|cancel|clos|dismiss|setting|option|menu|more|help|info|profil|account|login|logout|sign|register|feedback|language|locale|theme|dark|light|attach|upload|file|pick|choose|brows|preview|fullscreen|mute|play|paus|stop|back|forward|next|prev|retry|refresh|reload|expand|collaps|sidebar|drawer|toggle|avatar|notification|noti)[a-z]*\b/i;

    const scoredBtns = btnsUnique.map((b) => {
      const text = (b.textContent || "").trim();
      const ariaLabel = (b.getAttribute("aria-label") || "").toLowerCase();
      const title = (b.title || "").toLowerCase();
      const hasSvg = !!b.querySelector("svg");
      const type = (b.getAttribute("type") || "").toLowerCase();
      const isDisabled =
        b.disabled === true ||
        b.getAttribute("aria-disabled") === "true" ||
        b.getAttribute("disabled") !== null;
      let rect = { right: 0, bottom: 0, width: 0, height: 0 };
      try { rect = b.getBoundingClientRect(); } catch {}

      let score = 0;
      if (type === "submit") score += 25;
      // Disabled penalty (include for visibility but rarely click)
      if (isDisabled) score -= 35;

      const combinedLabel = ariaLabel + " " + title + " " + text.toLowerCase();
      // Strong reward for explicit send/submit/generate labels
      if (/\b(send|submit|generate|create|imagine|post|enter)\b/.test(combinedLabel)) {
        score += 30;
      }
      // STRONG negative for obvious non-send action labels — this is
      // the fix for 1.8.1 picking "Save" on the image-attached UI.
      if (NOT_SEND_TOKENS.test(combinedLabel)) {
        score -= 50;
      }
      // Icon-only button (no text, has SVG) is VERY likely the send
      // arrow in a modern chat composer — bump the reward so it beats
      // any random "Submit" text button elsewhere on the page.
      if (hasSvg && text.length === 0) score += 25;
      if (text.length > 0) {
        if (MODE_TOKENS.test(text)) score -= 40;
        else score -= 3;
      }
      if (MODE_TOKENS.test(combinedLabel)) score -= 40;
      score += ((rect.right || 0) / Math.max(1, window.innerWidth)) * 5;
      if (rect.width > 0 && rect.width < 60 && rect.height < 60 && hasSvg && text.length === 0) {
        score += 10; // was +3 — small icon in composer is very diagnostic
      }
      if (rect.width < 16 || rect.height < 16) score -= 50;

      // STRONG reward for being on the same horizontal row as the input
      // AND to its right — that's exactly where the send arrow lives
      // in a chat composer. Previously +8; a "Submit" text button
      // elsewhere on the page with +30 label match could still beat
      // the real send. Bumping to +20 puts the composer-row icon
      // firmly ahead.
      let inComposerRow = false;
      try {
        const inputCenterY = (inputRect.top + inputRect.bottom) / 2;
        const btnCenterY = (rect.top + rect.bottom) / 2;
        if (Math.abs(inputCenterY - btnCenterY) < 60 && rect.left >= inputRect.left) {
          score += 20;
          inComposerRow = true;
        }
      } catch {}

      // Extra "this is almost certainly the send" signal: icon-only
      // AND in the composer row. Real send buttons always tick both.
      if (inComposerRow && hasSvg && text.length === 0) {
        score += 15;
      }

      return { btn: b, score, text, ariaLabel, title, hasSvg, type, rect };
    });

    scoredBtns.sort((a, b) => b.score - a.score);

    // Debug: show top-8 (not 5) + also include tag + disabled info so
    // we can see if the real send button was in the pool but disabled.
    const topCandidates = scoredBtns.slice(0, 8).map((s) => ({
      text: s.text.slice(0, 30),
      ariaLabel: s.ariaLabel.slice(0, 30),
      score: Math.round(s.score * 10) / 10,
      hasSvg: s.hasSvg,
      tag: s.btn.tagName.toLowerCase(),
      pos: `${Math.round(s.rect.right)},${Math.round(s.rect.bottom)}`,
    }));

    // Winner selection: explicit aria-label match wins (if the priority
    // pass found one). Otherwise pick the top-scored button.
    const sendBtn = explicitSend || scoredBtns[0]?.btn || null;

    if (!sendBtn) {
      window.__grokAutomationActive = false;
      return {
        error: "no_send_button_found",
        debug: {
          inputType,
          inputPlaceholder: chosen.placeholder,
          scopeTag: scope?.tagName,
          buttonsInScope: allBtns.length,
          nearbyCount: nearbyBtns.length,
          topCandidates,
        },
      };
    }

    sendBtn.click();

    const label =
      sendBtn.getAttribute("aria-label") ||
      sendBtn.title ||
      sendBtn.textContent.trim().slice(0, 40) ||
      "svg-btn";
    return {
      ok: true,
      inputType,
      buttonLabel: label,
      buttonScore: explicitSend ? 999 : scoredBtns[0].score,
      explicitMatch: !!explicitSend,
      topCandidates,
    };
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
          const existing = fresh[info.email];
          if (existing) {
            // Same account, additional tab — user wants parallelism.
            if (!existing.tab_ids.includes(tab.id)) {
              existing.tab_ids.push(tab.id);
            }
          } else {
            fresh[info.email] = {
              email: info.email,
              userId: info.userId,
              subscription: info.subscription,
              tab_ids: [tab.id],
              // Deprecated alias — first tab — keeps older code paths
              // working until they're migrated to tab_ids.
              tab_id: tab.id,
              last_seen: Date.now(),
            };
          }
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
    // Clean up busy flags for tabs that no longer exist (e.g. user
    // closed one mid-run). Keeps grokTabBusy from growing unbounded.
    const liveTabIds = new Set();
    for (const acc of Object.values(fresh)) {
      for (const tid of acc.tab_ids || []) liveTabIds.add(tid);
    }
    for (const k of Object.keys(grokTabBusy)) {
      if (!liveTabIds.has(Number(k))) delete grokTabBusy[k];
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
    // Reference image — for image→video (Approach 2: HTTP upload +
    // URL-in-text + click send)
    reference_image_base64,
    reference_image_filename,
    reference_image_mime,
  } = work;

  if (grokSubmittedRequestIds.has(request_id)) return;

  const info = grokAccounts[account];
  if (!info) {
    await grokSubmitResult(request_id, { error: "account_tab_not_found" });
    return;
  }

  // ─── Pick a free tab ───
  // Account may have multiple grok.com/imagine tabs open — each tab
  // is an independent dispatch slot (1 video generation at a time
  // per tab, due to Grok's UI lock during render). Pick the first
  // tab that isn't already handling a job; if all are busy, wait.
  const tabPool = Array.isArray(info.tab_ids) && info.tab_ids.length
    ? info.tab_ids
    : info.tab_id
    ? [info.tab_id]
    : [];
  if (!tabPool.length) {
    await grokSubmitResult(request_id, { error: "account_tab_not_found" });
    return;
  }

  let tabId = null;
  const waitStart = Date.now();
  while (tabId === null) {
    // Pick the first non-busy tab that's still alive.
    for (const candidate of tabPool) {
      if (grokTabBusy[candidate]) continue;
      try {
        const t = await chrome.tabs.get(candidate);
        if (t) { tabId = candidate; break; }
      } catch { /* tab closed — skip */ }
    }
    if (tabId !== null) break;
    // No free tab — wait, respecting the bridge's 6-min dispatch
    // lock. Report "tab_wait" so the user can see the hold happening.
    if (Date.now() - waitStart > 330000) {
      await grokSubmitResult(request_id, {
        error: "all_tabs_busy_timeout",
        detail:
          `All ${tabPool.length} tab(s) for ${account} stayed busy >5min. ` +
          "Either increase tab count (open more grok.com/imagine tabs) " +
          "or reduce concurrent slots in app settings.",
      });
      return;
    }
    if ((Date.now() - waitStart) % 10000 < 2100) {
      await grokReportProgress(
        request_id,
        "tab_wait",
        `${tabPool.length} tab(s) busy, waiting...`
      );
    }
    await new Promise((r) => setTimeout(r, 2000));
  }

  grokTabBusy[tabId] = true;

  // Wrap the entire job body in try/finally so the busy flag is
  // ALWAYS released — even on unexpected errors — so subsequent
  // jobs don't stall on a phantom lock.
  try {
    try {
      await chrome.tabs.get(tabId);
    } catch {
      await grokSubmitResult(request_id, { error: "tab_closed" });
      return;
    }

    await grokReportProgress(
      request_id,
      "started",
      `account=${account} tab=${tabId} pool=${tabPool.length}`
    );

    // After each completed generation Grok lands the tab on
    // /imagine/post/<id>. The post view has a different composer
    // (Compose Post → reply flow) that doesn't fire a fresh
    // /rest/app-chat/conversations/new. Bounce back to /imagine
    // before every dispatch so each job starts from a clean
    // compose state.
    const navResult = await grokEnsureOnImaginePage(tabId);
    if (navResult.error) {
      await grokSubmitResult(request_id, {
        error: `navigation_failed_${navResult.error}`,
        detail:
          "Could not return to grok.com/imagine. Check the tab URL and " +
          "make sure Grok is reachable.",
      });
      return;
    }
    if (navResult.changed) {
      await grokReportProgress(request_id, "navigated", "→ /imagine");
    }

  const userPrompt = String(prompt || "").trim();
  const hasReference = !!(reference_image_base64 && reference_image_filename);

  // ─────────────────────────────────────────────────────────────
  // Approach 1 (image→video): inject the File object straight into
  // Grok's hidden <input type="file"> (the one the "+ upload"
  // button targets). Grok's own UI then uploads, registers the
  // attachment in React state, and the subsequent click→send fires
  // /rest/app-chat/conversations/new with fileAttachments correctly
  // populated — which is what actually makes Grok treat it as
  // image-to-video. Approach 2 (URL-in-message) was fielded in
  // 1.7.2 but produced text-to-video because Grok's backend
  // requires fileAttachments to trigger the image pipeline.
  //
  // DragEvent-drop is kept as a fallback for the rare case Grok's
  // dropzone doesn't ship a hidden file input.
  // ─────────────────────────────────────────────────────────────
  let messageForClick = userPrompt;

  if (hasReference) {
    await grokReportProgress(
      request_id,
      "attaching_image",
      reference_image_filename
    );

    const attachResult = await grokAttachImage(
      tabId,
      reference_image_base64,
      reference_image_filename,
      reference_image_mime || "image/jpeg"
    );

    if (!attachResult || !attachResult.ok) {
      const detail = attachResult
        ? `strategy=${attachResult.strategy || "?"}, ` +
          `fileInputs=${attachResult.fileInputsFound ?? "?"}, ` +
          `waited=${attachResult.waited_ms ?? "?"}ms`
        : "no_response";
      await grokSubmitResult(request_id, {
        error: "image_attach_failed",
        detail,
      });
      return;
    }

    await grokReportProgress(
      request_id,
      "image_attached",
      `via ${attachResult.strategy}${
        attachResult.attachSignal ? " (" + attachResult.attachSignal + ")" : ""
      }`
    );

    // For image-to-video Grok's UI auto-prepends the media URL +
    // double-space when the user types. We just type the prompt text;
    // Grok handles the rest. Fall back to "animate" for empty prompts.
    messageForClick = userPrompt || "animate";
  } else if (!userPrompt) {
    await grokSubmitResult(request_id, { error: "empty_prompt" });
    return;
  }

  // ─────────────────────────────────────────────────────────────
  // Click-based dispatch — type the composed message into the
  // textarea / contenteditable and click send. Grok's own JS then
  // fires /rest/app-chat/conversations/new with a FRESH x-statsig-id,
  // which is what the backend's anti-bot check requires. We tee the
  // response stream via the content-script fetch patch and poll for
  // the resulting videoUrl.
  // ─────────────────────────────────────────────────────────────
  await grokReportProgress(
    request_id,
    "clicking_send",
    hasReference ? "image_mode" : "text_mode"
  );
  const clickResult = await grokClickSend(tabId, messageForClick);
  if (!clickResult || clickResult.error) {
    const dbgStr = clickResult?.debug
      ? ` | debug: ${JSON.stringify(clickResult.debug).slice(0, 300)}`
      : "";
    await grokSubmitResult(request_id, {
      error: `click_failed_${clickResult?.error || "unknown"}`,
      detail:
        "Could not locate input or send button on grok.com/imagine. " +
        "Make sure the tab is open to /imagine and Video mode is selected." +
        dbgStr,
    });
    return;
  }
  const scoreInfo =
    typeof clickResult.buttonScore === "number"
      ? ` score=${clickResult.buttonScore.toFixed(1)}`
      : "";
  await grokReportProgress(
    request_id,
    "clicked",
    `${clickResult.buttonLabel || "send"} (input=${clickResult.inputType || "?"}${scoreInfo})`
  );
  // If we DIDN'T hit the explicit-match short-circuit, surface the
  // top-5 candidates so the log shows what else was competing. Helps
  // debug when image-mode or other UI variants pick a wrong button
  // (the "Submit" text button with score 49.9 bug from v1.8.6).
  if (!clickResult.explicitMatch && clickResult.topCandidates) {
    const cands = clickResult.topCandidates
      .slice(0, 5)
      .map(
        (c) =>
          `[${c.score}|${c.tag || "?"}${c.hasSvg ? "+svg" : ""} "${
            (c.ariaLabel || c.text || "-").slice(0, 20)
          }"]`
      )
      .join(" ");
    await grokReportProgress(request_id, "click_candidates", cands);
  }

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

  // Click-effect verification — within 25 seconds Grok's own code
  // should either (a) start streaming progress (progress > 0), (b)
  // navigate the tab to /imagine/post/<id>, or (c) populate a DOM
  // video element. If NONE of those happen, the click landed on the
  // wrong button (common issue in image-mode when the send button's
  // aria-label is empty and a text-"Submit" button wins the
  // heuristic). Fail fast with a clear error so the job doesn't
  // tie up the tab for 4 minutes.
  const clickEffectDeadline = Date.now() + 25000;

  let urlNavigated = false;
  const initialUrl = (await (async () => {
    try { const t = await chrome.tabs.get(tabId); return t.url || ""; }
    catch { return ""; }
  })()) || "";

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

    // Click-effect check. Within 25s we expect SOME signal: progress
    // > 0, URL change to /post/<id>, or a <video> in the DOM. If
    // none yet, keep waiting up to the deadline; after the deadline
    // fail fast so the tab is released for the next job.
    if (!urlNavigated) {
      try {
        const t = await chrome.tabs.get(tabId);
        if (
          t.url &&
          t.url !== initialUrl &&
          t.url.includes("/imagine/post/")
        ) {
          urlNavigated = true;
          await grokReportProgress(
            request_id,
            "post_page_reached",
            t.url.slice(0, 100)
          );
        }
      } catch {}
    }
    const clickHadEffect =
      urlNavigated ||
      progressPct > 0 ||
      !!domFallbackUrl ||
      !!videoUrl;
    if (!clickHadEffect && Date.now() > clickEffectDeadline) {
      errorSeen = "click_no_effect";
      break;
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
  } finally {
    // Always release the tab lock so the next job on this tab can
    // proceed. Also clear the automation flag on the page so stale
    // capture state from this run doesn't leak into the next.
    grokTabBusy[tabId] = false;
    try {
      await grokClearAutomationFlag(tabId);
    } catch {}
  }
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
