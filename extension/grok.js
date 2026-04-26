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
// Anti-throttle — Chrome heavily throttles background tabs (timers
// quantized to 1Hz, CPU budget ~1%/min, low-priority network, paused
// rAF). For Grok this makes background tabs 3-5x slower per video.
//
// Workaround: play a silent Web Audio loop so Chrome marks the tab as
// "audible". Audible tabs skip most background throttling — this is
// the same trick Discord, Slack, and Meet use to stay responsive when
// backgrounded. We also override document.hidden/visibilityState so
// any page code that self-pauses based on visibility keeps running.
//
// Idempotent — safe to call on every detection cycle. AudioContext
// may start in `suspended` state if the page never got a user gesture,
// so we also attempt resume(). If autoplay policy blocks it, the
// visibility override alone still helps.
// ═══════════════════════════════════════════════════════════════════
async function grokInstallAntiThrottle(tabId) {
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: () => {
        if (window.__grokAntiThrottleInstalled) return { already: true };
        window.__grokAntiThrottleInstalled = true;

        // ─── Silent audio loop to keep tab "audible" ───
        try {
          const Ctor = window.AudioContext || window.webkitAudioContext;
          if (Ctor) {
            const ctx = new Ctor();
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            gain.gain.value = 0;            // fully silent
            osc.frequency.value = 20;       // sub-audible anyway
            osc.connect(gain).connect(ctx.destination);
            osc.start();
            window.__grokAntiThrottleCtx = ctx;
            // May be suspended until user gesture — try resume. Retry
            // on click/visibilitychange so we grab the first gesture.
            const tryResume = () => {
              if (ctx.state !== "running") ctx.resume().catch(() => {});
            };
            tryResume();
            ["click", "keydown", "touchstart", "visibilitychange"].forEach(
              (ev) => document.addEventListener(ev, tryResume, {
                capture: true, passive: true,
              })
            );
          }
        } catch (e) { /* audio failed — visibility override still helps */ }

        // ─── Override visibility so page JS doesn't self-pause ───
        try {
          Object.defineProperty(document, "hidden", {
            configurable: true,
            get: () => false,
          });
          Object.defineProperty(document, "visibilityState", {
            configurable: true,
            get: () => "visible",
          });
          Object.defineProperty(document, "webkitHidden", {
            configurable: true,
            get: () => false,
          });
          Object.defineProperty(document, "webkitVisibilityState", {
            configurable: true,
            get: () => "visible",
          });
          document.dispatchEvent(new Event("visibilitychange"));
        } catch (e) { /* properties already overridden */ }

        return { ok: true };
      },
    });
    return true;
  } catch (e) {
    console.warn("[Grok] installAntiThrottle failed:", e?.message || e);
    return false;
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

async function grokEnsureOnImaginePage(tabId, opts) {
  // opts.force=true forces a hard navigation even if the tab is
  // already on /imagine — needed by the click-retry path because the
  // composer keeps its attached-image state across nav-less calls,
  // and re-running grokAttachImage would stack a second/third image
  // onto the same composer instead of replacing the original.
  const force = !!(opts && opts.force);
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
    if (isImagineHome(url) && !force) {
      return { ok: true, changed: false, url };
    }
    if (!url.startsWith(GROK_ORIGIN) && !isImagineHome(url)) {
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
// Mode toggle — Grok's /imagine composer has Image / Video tabs.
// On fresh devices it defaults to Image; submitting there generates an
// IMAGE (image-to-image edit) instead of a video. We force Video mode.
//
// Detection strategy — SVG path matching (most stable):
// Each mode button contains a Lucide icon. The icon's <path> element
// has fixed `d=` data that survives className refactors, aria-label
// changes, theme switches, and React component restructures. We look
// for:
//
//   Video icon: starts with "M12 4C14.4853 4 16.5 6.01472 16.5 8.5V15.5"
//   Image icon: starts with "M14.0996 2.5C15.2032 2.5"
//
// (Path data comes from the Lucide icon set Grok uses. See competitor
// extension's remote config — same approach.)
//
// Idempotency: clicking an already-selected radio is a no-op in
// Grok's UI, so we always click. Simpler than tracking active state.
// ═══════════════════════════════════════════════════════════════════
async function grokEnsureVideoMode(tabId) {
  return grokExecInTab(tabId, async () => {
    // Walk up the DOM from a path element to its containing button.
    // The Lucide icon SVG sits inside the toggle button — sometimes
    // wrapped in spans/divs, so we walk up rather than hard-coding
    // a specific parent depth.
    const buttonContaining = (pathEl) => {
      let el = pathEl;
      for (let i = 0; i < 6 && el; i++) {
        if (el.tagName === "BUTTON") return el;
        el = el.parentElement;
      }
      return null;
    };

    const visible = (el) => {
      if (!el) return false;
      if (el.offsetParent === null) return false;
      const r = el.getBoundingClientRect();
      return r.width > 4 && r.height > 4;
    };

    // Find Image/Video buttons by SVG path data. Using prefix match
    // (^=) instead of exact match because Grok occasionally tweaks
    // the trailing decimal precision of path coordinates without
    // changing the icon shape — prefix survives those tweaks.
    const findByPathPrefix = (prefix) => {
      const sel = `path[d^="${prefix}"]`;
      const candidates = document.querySelectorAll(sel);
      for (const p of candidates) {
        const btn = buttonContaining(p);
        if (btn && visible(btn)) return btn;
      }
      return null;
    };

    const videoBtn = findByPathPrefix(
      "M12 4C14.4853 4 16.5 6.01472 16.5 8.5V15.5"
    );
    const imageBtn = findByPathPrefix("M14.0996 2.5C15.2032 2.5");

    const debug = {
      video_found: !!videoBtn,
      image_found: !!imageBtn,
      // Diagnostic counts so a failed match tells us the page state
      total_paths: document.querySelectorAll("path").length,
      total_buttons: document.querySelectorAll("button").length,
    };

    // Pure SVG-path failure. The composer either hasn't loaded or
    // Grok swapped icon set. Fall back caller will log a warning.
    if (!videoBtn) {
      return { ok: true, no_video_btn: true, debug };
    }

    // Detect active state via background darkness. Grok's selected
    // mode pill renders with a dark fill on a lighter parent. We
    // walk into descendants to find the first opaque background
    // (the styled fill often lives on a child div, not the button).
    const getEffectiveBg = (el) => {
      const isOpaque = (col) => {
        const m = col && col.match(/rgba?\(([^)]+)\)/);
        if (!m) return false;
        const p = m[1].split(",").map((s) => parseFloat(s.trim()));
        const alpha = p.length === 4 ? p[3] : 1;
        return alpha >= 0.1;
      };
      try {
        const own = window.getComputedStyle(el).backgroundColor || "";
        if (isOpaque(own)) return own;
        const queue = [[el, 0]];
        while (queue.length) {
          const [node, depth] = queue.shift();
          if (depth > 0) {
            const bg = window.getComputedStyle(node).backgroundColor || "";
            if (isOpaque(bg)) return bg;
          }
          if (depth < 3) {
            for (const child of node.children || []) {
              queue.push([child, depth + 1]);
            }
          }
        }
      } catch {}
      return null;
    };

    // Compare luma of the two pills — selected has lower (darker) luma.
    const lumaOf = (el) => {
      const bg = getEffectiveBg(el);
      if (!bg) return Infinity;
      const m = bg.match(/rgba?\(([^)]+)\)/);
      if (!m) return Infinity;
      const p = m[1].split(",").map((s) => parseFloat(s.trim()));
      return (p[0] || 0) + (p[1] || 0) + (p[2] || 0);
    };

    const videoLuma = lumaOf(videoBtn);
    const imageLuma = imageBtn ? lumaOf(imageBtn) : Infinity;
    debug.video_luma = videoLuma === Infinity ? null : videoLuma;
    debug.image_luma = imageLuma === Infinity ? null : imageLuma;

    // Already on Video? (video pill darker than image by luma>30 margin)
    if (videoLuma + 30 < imageLuma) {
      return { ok: true, already_video: true, debug };
    }

    // Click strategy: native + synthetic. React handlers sometimes
    // bind to a parent or SVG child, so a single .click() can miss.
    const clickTarget = (el) => {
      try { el.click(); } catch {}
      try {
        for (const type of ["mousedown", "mouseup", "click"]) {
          el.dispatchEvent(new MouseEvent(type, {
            bubbles: true, cancelable: true, view: window, button: 0,
          }));
        }
      } catch {}
    };
    clickTarget(videoBtn);
    await new Promise((r) => setTimeout(r, 600));

    // Verify post-click. If image is still darker, retry once.
    let finalVideoLuma = lumaOf(videoBtn);
    let finalImageLuma = imageBtn ? lumaOf(imageBtn) : Infinity;
    if (!(finalVideoLuma + 30 < finalImageLuma) && imageBtn) {
      clickTarget(videoBtn);
      await new Promise((r) => setTimeout(r, 600));
      finalVideoLuma = lumaOf(videoBtn);
      finalImageLuma = lumaOf(imageBtn);
    }

    return {
      ok: true,
      switched: true,
      verified_video_active: finalVideoLuma + 30 < finalImageLuma,
      debug,
    };
  });
}

// ═══════════════════════════════════════════════════════════════════
// Resolution / duration / aspect-ratio toggles — Grok remembers the
// last-used values per device, so a fresh login generates 480p / 6s
// videos even when the app config asks for 720p / 10s. We push the
// requested values onto the composer's toggle row before submit.
//
// Selector strategy (mirrors competitor's chrome ext + remote config):
//   - Resolution / duration pills:
//       button:has(span:contains("720p")), etc.
//     We find the button containing a <span> whose textContent === token.
//     Always-click (radio buttons are idempotent on click-already-active).
//   - Aspect ratio:
//       Trigger: button whose textContent matches /^\d+:\d+$/ in composer
//       Popover option: any element whose text contains \d+:\d+ matching
//
// We DON'T detect "active" state for pills — instead we click
// unconditionally because Grok's radio buttons handle re-selection as
// no-ops. This eliminates the false-positive "(already)" bug from
// 1.9.x where transparent button bg confused the luma comparison.
//
// Returns a summary object so the caller can log what changed.
// ═══════════════════════════════════════════════════════════════════
async function grokEnsureMediaSettings(tabId, opts) {
  return grokExecInTab(tabId, async (args) => {
    const norm = (s) => (s || "").toLowerCase().trim();
    const stripWs = (s) => norm(s).replace(/\s+/g, "");
    const resWanted = stripWs(args.resolution);
    const lenWanted = stripWs(args.video_length);
    const aspectWanted = stripWs(args.aspect_ratio);

    const visible = (el) => {
      if (!el) return false;
      if (el.offsetParent === null) return false;
      const r = el.getBoundingClientRect();
      return r.width > 4 && r.height > 4;
    };

    // Click via native + synthetic events — React handlers sometimes
    // bind to a parent or SVG child, so a single .click() on the
    // wrong target misses. Three event types cover most patterns.
    const clickTarget = (el) => {
      try { el.click(); } catch {}
      for (const type of ["mousedown", "mouseup", "click"]) {
        try {
          el.dispatchEvent(new MouseEvent(type, {
            bubbles: true, cancelable: true, view: window, button: 0,
          }));
        } catch {}
      }
    };

    // ─── Pill finder: button containing a span with EXACT text ───
    // Mirrors competitor's `button:has(span:contains("720p"))` selector.
    // We walk all buttons and check their descendant spans because
    // native CSS `:contains` doesn't exist (it's a jQuery extension).
    //
    // Strict equality on the span text (after whitespace strip) prevents
    // matching things like "10s timer" or "in 720p mode" — only standalone
    // pill labels match. Position guard restricts to lower half of
    // viewport (where the composer always sits) to avoid menu items
    // from a popover above the composer.
    const composerYThreshold = window.innerHeight * 0.45;
    const findPillByText = (token) => {
      const target = stripWs(token);
      if (!target) return null;
      const buttons = document.querySelectorAll(
        'button,[role="button"],[role="radio"]'
      );
      for (const b of buttons) {
        if (!visible(b)) continue;
        try {
          const r = b.getBoundingClientRect();
          if (r.top < composerYThreshold) continue;
        } catch { continue; }
        // Check direct span descendants. We also accept the button's
        // own textContent as a fallback for the case where the label
        // sits directly in the button (no span wrapper).
        const spans = b.querySelectorAll("span");
        let matched = false;
        for (const s of spans) {
          if (stripWs(s.textContent) === target) { matched = true; break; }
        }
        if (!matched && stripWs(b.textContent) === target) matched = true;
        if (matched) return b;
      }
      return null;
    };

    // Setting a pill: simply click it. Grok's radio buttons treat a
    // click-on-already-selected as a no-op, so we don't need to
    // detect "active" state — that's where v1.9.x got tangled in
    // false-positive luma comparisons. Always-click is the
    // competitor's pattern and it's strictly safer.
    const clickPill = async (token) => {
      if (!token) return { skipped: true };
      const btn = findPillByText(token);
      if (!btn) return { token, found: false };
      clickTarget(btn);
      await new Promise((r) => setTimeout(r, 250));
      return { token, found: true, switched: true };
    };

    // ─── Aspect ratio dropdown ───
    // Trigger button's textContent is just the ratio (e.g. "16:9").
    // Click → popover opens → option element contains "X:Y" possibly
    // alongside a label word ("16:9 Widescreen") — extract via regex.
    const setAspectRatio = async (wanted) => {
      if (!wanted) return { skipped: true };
      const triggerRe = /^\d+:\d+$/;
      const triggers = Array.from(
        document.querySelectorAll('button,[role="button"],[role="combobox"]')
      ).filter((b) => {
        if (!visible(b)) return false;
        try {
          const r = b.getBoundingClientRect();
          if (r.top < composerYThreshold) return false;
        } catch { return false; }
        return triggerRe.test(stripWs(b.textContent));
      });
      if (!triggers.length) return { token: wanted, found: false };
      const trigger = triggers[0];
      const currentRatio = stripWs(trigger.textContent);
      if (currentRatio === wanted) {
        return { token: wanted, found: true, already: true };
      }
      // Open popover
      clickTarget(trigger);
      await new Promise((r) => setTimeout(r, 700));
      // Find option — extract X:Y from text (handles "○ 16:9 Widescreen")
      const optionRe = /(\d+:\d+)/;
      const opts = Array.from(
        document.querySelectorAll(
          '[role="menuitem"],[role="menuitemradio"],[role="option"],button,[role="button"],li'
        )
      ).filter((el) => {
        if (!visible(el)) return false;
        const m = stripWs(el.textContent).match(optionRe);
        return !!(m && m[1] === wanted);
      });
      if (!opts.length) {
        clickTarget(trigger); // close popover
        return { token: wanted, found: true, option_not_found: true };
      }
      // Smallest matching = innermost clickable row
      opts.sort((a, b) => {
        const ar = a.getBoundingClientRect();
        const br = b.getBoundingClientRect();
        return ar.width * ar.height - br.width * br.height;
      });
      clickTarget(opts[0]);
      await new Promise((r) => setTimeout(r, 500));
      const newRatio = stripWs(trigger.textContent);
      return {
        token: wanted, found: true, switched: true,
        verified_active: newRatio === wanted,
      };
    };

    // Dispatch in sequence: aspect → resolution → duration. Aspect
    // first because opening/closing the popover may briefly hide
    // the resolution/duration pills, so we settle that DOM-altering
    // step before touching the others.
    const summary = {};
    if (aspectWanted) summary.aspect = await setAspectRatio(aspectWanted);
    if (resWanted)    summary.res    = await clickPill(resWanted);
    if (lenWanted) {
      // Try with "s" suffix first (matches "6s"/"10s" pills), fall
      // back to bare number ("6"/"10") if Grok ever drops the suffix.
      const withS = lenWanted.endsWith("s") ? lenWanted : lenWanted + "s";
      let r = await clickPill(withS);
      if (r && r.found === false) {
        r = await clickPill(lenWanted.replace(/s$/, ""));
      }
      summary.len = r;
    }
    return summary;
  }, opts);
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

    // Snapshot the current set of attachment-preview image srcs BEFORE
    // we attach. When the race-condition bug caused two workers to
    // share a tab, the second worker would see the first worker's
    // thumbnail and falsely report "attached" — sending a merged 2-
    // image submission. Even without the race, a stale preview from a
    // prior job can linger during re-navigation. We only consider the
    // attach successful when a NEW preview src appears.
    const preExistingSrcs = new Set(
      Array.from(document.querySelectorAll("img"))
        .map((img) => img.src || "")
        .filter(
          (s) => s && (s.startsWith("blob:") || s.includes("/users/0"))
        )
    );

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
    // Two-stage detection:
    //   Stage 1: thumbnail preview appears (blob: img, /users/0 asset,
    //            or filename text) — confirms file was registered.
    //   Stage 2: upload spinner (.animate-pulse / .animate-spin)
    //            disappears — confirms server-side upload completed.
    //   Plus: detect upload failure icon (svg.lucide-triangle-alert)
    //         and bail early so retry can re-attach a fresh image.
    //
    // Stage 2 is critical for grokClickSend's "wait for enable"
    // poll to find an enabled submit button — if we return before
    // the spinner clears, the click happens against a still-disabled
    // button and triggers no_enabled_send_button → retry.
    const startTs = Date.now();
    const stage1Deadline = startTs + 12000;
    let attached = false;
    let attachSignal = "";
    while (Date.now() < stage1Deadline) {
      await new Promise((r) => setTimeout(r, 400));
      // Upload-failed icon? Bail early so caller can retry attach.
      const failIcon = document.querySelector("svg.lucide-triangle-alert");
      if (failIcon && failIcon.offsetParent !== null) {
        return {
          ok: false,
          strategy,
          attachSignal: "upload_failed_icon",
          waited_ms: Date.now() - startTs,
          fileInputsFound: fileInputs.length,
        };
      }
      // 1) Blob-URL preview image (most dropzones create these) — must
      //    be a NEW src not present before the attach call, otherwise
      //    we're looking at a stale thumbnail from a prior job.
      const blobImg = Array.from(document.querySelectorAll("img")).find(
        (img) =>
          img.src &&
          (img.src.startsWith("blob:") ||
            img.src.includes("/users/0")) &&
          !preExistingSrcs.has(img.src)
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

    if (!attached) {
      return {
        ok: false,
        strategy,
        attachSignal: "stage1_timeout",
        waited_ms: Date.now() - startTs,
        fileInputsFound: fileInputs.length,
      };
    }

    // Stage 2: wait for spinner to clear (upload server-side processing
    // finished). Up to 20s additional — large reference images may take
    // a while on slow connections. Failure icon check still applies.
    const stage2Deadline = Date.now() + 20000;
    let spinnerCleared = false;
    while (Date.now() < stage2Deadline) {
      // Bail if upload failed mid-processing
      const failIcon = document.querySelector("svg.lucide-triangle-alert");
      if (failIcon && failIcon.offsetParent !== null) {
        return {
          ok: false,
          strategy,
          attachSignal: "upload_failed_during_processing",
          waited_ms: Date.now() - startTs,
          fileInputsFound: fileInputs.length,
        };
      }
      const spinner = document.querySelector(".animate-pulse, .animate-spin");
      if (!spinner || spinner.offsetParent === null) {
        spinnerCleared = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 250));
    }

    return {
      ok: true,
      strategy,
      attachSignal,
      spinnerCleared,
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

    // ─── Helpers ───
    const visible = (el) => {
      if (!el) return false;
      if (el.offsetParent === null) return false;
      const r = el.getBoundingClientRect();
      return r.width > 4 && r.height > 4;
    };
    const isDisabled = (el) => {
      if (!el) return true;
      if (el.disabled === true) return true;
      if (el.getAttribute("aria-disabled") === "true") return true;
      if (el.getAttribute("disabled") !== null) return true;
      return false;
    };
    const clickTarget = (el) => {
      try { el.click(); } catch {}
      for (const t of ["mousedown", "mouseup", "click"]) {
        try {
          el.dispatchEvent(new MouseEvent(t, {
            bubbles: true, cancelable: true, view: window, button: 0,
          }));
        } catch {}
      }
    };

    // ─── Find the prompt input ───
    // Preferred selector mirrors competitor's remote config:
    //   form div[contenteditable='true']  → ProseMirror/Lexical composer
    //   div[data-testid='drop-ui'] textarea → drop-zone variant
    // Fall back to any visible contenteditable / textarea / text input.
    const inputCandidates = [];
    for (const el of document.querySelectorAll(
      'form div[contenteditable="true"]'
    )) inputCandidates.push({ el, type: "contenteditable" });
    for (const el of document.querySelectorAll(
      'div[data-testid="drop-ui"] textarea'
    )) inputCandidates.push({ el, type: "textarea" });
    for (const el of document.querySelectorAll(
      '[contenteditable="true"],[contenteditable=""]'
    )) inputCandidates.push({ el, type: "contenteditable" });
    for (const el of document.querySelectorAll("textarea")) {
      inputCandidates.push({ el, type: "textarea" });
    }
    for (const el of document.querySelectorAll(
      'input[type="text"],input:not([type])'
    )) inputCandidates.push({ el, type: "input" });

    const inputVisible = inputCandidates
      .filter(({ el }) => {
        try {
          if (el.disabled) return false;
          return visible(el);
        } catch { return false; }
      })
      // Dedupe — we may have hit the same element via multiple selectors
      .filter((c, i, arr) => arr.findIndex((x) => x.el === c.el) === i);

    const chosen = inputVisible[0];
    if (!chosen) {
      window.__grokAutomationActive = false;
      return {
        error: "no_input_found",
        debug: {
          totalCandidates: inputCandidates.length,
          visibleCount: inputVisible.length,
        },
      };
    }
    const inputEl = chosen.el;
    const inputType = chosen.type;

    // ─── Set the prompt value ───
    try {
      inputEl.focus();
      if (inputType === "textarea" || inputType === "input") {
        const proto =
          inputType === "textarea" ? HTMLTextAreaElement : HTMLInputElement;
        const setter = Object.getOwnPropertyDescriptor(
          proto.prototype, "value"
        ).set;
        setter.call(inputEl, String(args.prompt || ""));
        inputEl.dispatchEvent(new Event("input", { bubbles: true }));
      } else {
        // contenteditable — execCommand insertText fires real InputEvents
        // that React/ProseMirror listen to. Most robust path for modern
        // chat composers.
        const sel = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(inputEl);
        sel.removeAllRanges();
        sel.addRange(range);
        try { document.execCommand("delete", false); } catch {}
        try {
          document.execCommand(
            "insertText", false, String(args.prompt || "")
          );
        } catch {
          // Fallback if execCommand is disabled
          inputEl.textContent = String(args.prompt || "");
          inputEl.dispatchEvent(new InputEvent("beforeinput", {
            bubbles: true, cancelable: true,
            inputType: "insertText", data: String(args.prompt || ""),
          }));
          inputEl.dispatchEvent(new InputEvent("input", {
            bubbles: true,
            inputType: "insertText", data: String(args.prompt || ""),
          }));
        }
      }
    } catch (e) {
      window.__grokAutomationActive = false;
      return {
        error: "set_input_failed",
        detail: String(e?.message || e).slice(0, 200),
      };
    }

    // Initial settle — let React re-render with the new prompt value.
    await new Promise((r) => setTimeout(r, 800));

    // ─── Wait for upload spinners to clear ───
    // If a reference image was attached, Grok shows .animate-pulse /
    // .animate-spin while server-side processing runs. The send
    // button stays disabled during that window. Polling here
    // explicitly is much more direct than the old 8s heuristic poll.
    const uploadDeadline = Date.now() + 20000;
    while (Date.now() < uploadDeadline) {
      const spinner = document.querySelector(
        ".animate-pulse, .animate-spin"
      );
      if (!spinner || !visible(spinner)) break;
      await new Promise((r) => setTimeout(r, 250));
    }

    // ─── Detect upload failures ───
    // Grok renders a triangle-alert icon when an upload fails. If we
    // see one, the composer state is bad — bail so upstream retry
    // can re-attach (and our 1.9.8 force-reload wipes the failed
    // attachment first).
    const failedIcon = document.querySelector("svg.lucide-triangle-alert");
    if (failedIcon && visible(failedIcon)) {
      window.__grokAutomationActive = false;
      return {
        error: "upload_failed",
        debug: { icon_visible: true },
      };
    }

    // ─── Find the Submit button by SVG arrow path ───
    // The send icon is a Lucide-style up-arrow. Its <path> d-attribute
    // is the GEOMETRY of the arrow — it survives className refactors,
    // aria-label changes, theme switches, and React component
    // reorganization. Only an actual icon redesign would change this
    // string. Path: "M6 11L12 5M12 5L18 11M12 5V19".
    //
    // Compared to v1.9.x heuristic scoring (which broke ~weekly when
    // class names rebuilt), this is ~10x more stable.
    const SUBMIT_ICON_PATH = "M6 11L12 5M12 5L18 11M12 5V19";

    const findSubmitButton = () => {
      const paths = document.querySelectorAll(
        `path[d="${SUBMIT_ICON_PATH}"]`
      );
      for (const p of paths) {
        let el = p;
        // Walk up to enclosing button (max 6 levels)
        for (let i = 0; i < 6 && el; i++) {
          if (el.tagName === "BUTTON") break;
          el = el.parentElement;
        }
        if (el && el.tagName === "BUTTON" && visible(el)) return el;
      }
      return null;
    };

    let submitBtn = findSubmitButton();
    if (!submitBtn) {
      window.__grokAutomationActive = false;
      // no_send_button_found triggers upstream retry — same as
      // no_enabled_send_button. Distinct error code helps debug.
      return {
        error: "no_send_button_found",
        debug: {
          svg_paths_total: document.querySelectorAll("path").length,
          submit_path_count:
            document.querySelectorAll(`path[d="${SUBMIT_ICON_PATH}"]`).length,
          inputType,
          uploadDidComplete: !document.querySelector(
            ".animate-pulse, .animate-spin"
          ),
        },
      };
    }

    // ─── Wait for submit button to ENABLE ───
    // After upload completes, Grok briefly keeps the button disabled
    // while React processes state. Poll up to 8s for the disabled
    // attribute to clear.
    const enableDeadline = Date.now() + 8000;
    while (Date.now() < enableDeadline) {
      submitBtn = findSubmitButton() || submitBtn;
      if (!isDisabled(submitBtn)) break;
      await new Promise((r) => setTimeout(r, 300));
    }

    if (isDisabled(submitBtn)) {
      window.__grokAutomationActive = false;
      // no_enabled_send_button is the canonical "transient" error —
      // upstream retry will bounce the composer and try again.
      return {
        error: "no_enabled_send_button",
        debug: {
          ariaLabel: submitBtn.getAttribute("aria-label") || "",
          ariaDisabled: submitBtn.getAttribute("aria-disabled") || "",
          disabledAttr: submitBtn.hasAttribute("disabled"),
          uploadStillRunning: !!document.querySelector(
            ".animate-pulse, .animate-spin"
          ),
        },
      };
    }

    // ─── Click ───
    clickTarget(submitBtn);

    return {
      ok: true,
      inputType,
      buttonLabel:
        submitBtn.getAttribute("aria-label") ||
        submitBtn.title ||
        "svg-submit-arrow",
      buttonScore: 999,        // SVG-path match = high confidence
      explicitMatch: true,     // for upstream logging compatibility
      topCandidates: [],       // no scoring pass — empty for log compat
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
          // Anti-throttle: silent-audio + visibility override. Makes
          // background tabs behave more like foreground ones so
          // streaming/download doesn't crawl at 1Hz when user Alt-Tabs
          // away. Also idempotent.
          try {
            await grokInstallAntiThrottle(tab.id);
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
              // Send the per-account tab count so the Python side can
              // size its worker pool to match real capacity. With 3
              // tabs open, no point spinning up 12 workers — they'd
              // all sit in tab_wait and the tail of the queue would
              // hit Python's idle timeout before getting a slot.
              tab_count: Array.isArray(a.tab_ids) ? a.tab_ids.length : 1,
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
    // Per-job composer settings — Grok remembers per-device defaults
    // for resolution / duration / aspect ratio, so we have to push
    // these onto the UI toggles each dispatch. Otherwise a fresh
    // login generates whatever Grok picked last (commonly 480p / 6s)
    // regardless of what the user chose in the app.
    aspect_ratio,
    resolution,
    video_length,
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
    //
    // IMPORTANT: reserve the tab (set busy=true) *synchronously* before
    // the `await chrome.tabs.get()` call. Otherwise two workers can
    // both see busy=false, both yield on the await, and both claim the
    // same tab — resulting in two image attachments merged onto one
    // composer and a single combined submission. Release the flag if
    // the tab turns out to be dead.
    for (const candidate of tabPool) {
      if (grokTabBusy[candidate]) continue;
      grokTabBusy[candidate] = true;   // reserve before awaiting
      try {
        const t = await chrome.tabs.get(candidate);
        if (t) { tabId = candidate; break; }
        grokTabBusy[candidate] = false;  // tab missing — release
      } catch {
        grokTabBusy[candidate] = false;  // tab closed — release, try next
      }
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

  // grokTabBusy[tabId] is already true — reserved synchronously inside
  // the picker loop above to prevent two workers racing into the same
  // tab (both seeing busy=false during an await yield).

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

    // Ensure the composer is in Video mode. Grok remembers the last-
    // used mode per device, so a fresh login defaults to Image mode —
    // submitting a reference there generates an IMAGE (image-to-image
    // edit), not a video. Click the Video toggle if needed.
    //
    // We always emit a status event with the detection summary so
    // failures here are visible in the user-facing log (silent skips
    // are exactly what made the original bug go undiagnosed).
    try {
      const modeResult = await grokEnsureVideoMode(tabId);
      if (modeResult?.switched) {
        const verified = modeResult.verified_video_active ? "verified" : "click_only";
        await grokReportProgress(
          request_id, "mode_video",
          `switched Image → Video (${verified})`
        );
      } else if (modeResult?.already_video) {
        await grokReportProgress(
          request_id, "mode_video", "already on Video — no switch needed"
        );
      } else if (modeResult?.no_video_btn) {
        const dbg = modeResult.debug
          ? `scanned=${modeResult.debug.total_buttons_scanned}, video_cands=${modeResult.debug.video_candidates}, image_cands=${modeResult.debug.image_candidates}`
          : "no_debug";
        await grokReportProgress(
          request_id, "mode_video_warn",
          `Video toggle not found — ${dbg}. If output is an image, send a screenshot of the composer.`
        );
      }
    } catch (e) {
      await grokReportProgress(
        request_id, "mode_video_warn",
        `${String(e?.message || e).slice(0, 80)}`
      );
    }

    // Push resolution + duration onto the composer pills. Grok remembers
    // these per device so a fresh login defaults to 480p / 6s — without
    // this step, the user's 720p / 10s app setting would be silently
    // overridden by Grok's last-used UI state.
    try {
      const settingsResult = await grokEnsureMediaSettings(tabId, {
        aspect_ratio: aspect_ratio || "",
        resolution: resolution || "",
        video_length: video_length ? String(video_length) : "",
      });
      const fmt = (label, r) => {
        if (!r || r.skipped) return null;
        if (!r.found) return `${label}=NOT_FOUND(${r.token})`;
        if (r.option_not_found) return `${label}=${r.token}(opt_not_found)`;
        const path = r.dbg?.decision ? `via=${r.dbg.decision}` : "";
        if (r.already) return `${label}=${r.token}(already${path ? " " + path : ""})`;
        if (r.switched) {
          return `${label}=${r.token}(${r.verified_active ? "set" : "click_only"}${path ? " " + path : ""})`;
        }
        return null;
      };
      const parts = [
        fmt("aspect", settingsResult?.aspect),
        fmt("res", settingsResult?.res),
        fmt("len", settingsResult?.len),
      ].filter(Boolean);
      if (parts.length) {
        await grokReportProgress(
          request_id, "media_settings", parts.join(", ")
        );
      }
      // Also dump the snapshots when we made a decision based on a
      // non-luma path — those are the cases where detection might
      // be wrong and we want raw bytes to triage.
      const needsTriage = (r) => r && r.dbg?.decision && r.dbg.decision !== "luma";
      if (needsTriage(settingsResult?.res) || needsTriage(settingsResult?.len)) {
        const snaps = {
          res_wanted: settingsResult?.res?.dbg?.wanted_snap,
          res_alt: settingsResult?.res?.dbg?.alt_snap,
          len_wanted: settingsResult?.len?.dbg?.wanted_snap,
          len_alt: settingsResult?.len?.dbg?.alt_snap,
        };
        await grokReportProgress(
          request_id, "media_settings_dbg",
          JSON.stringify(snaps).slice(0, 500),
        );
      }
    } catch (e) {
      await grokReportProgress(
        request_id, "media_settings_warn",
        `${String(e?.message || e).slice(0, 80)}`
      );
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
  // Transient click states — these are page-level conditions that
  // typically resolve after a re-navigate (Grok's UI was mid-upload,
  // the script context died because the tab was busy, the send button
  // was briefly disabled, etc.). Forward them into the retry loop
  // (re-nav + re-attach + re-click) instead of failing the job.
  //
  // Hard failures (no_input_found, set_input_failed) still fail fast
  // because they're non-transient — the page is in an unrecognized
  // state and a retry won't help.
  let skipInitialPoll = false;
  const clickResult = await grokClickSend(tabId, messageForClick);
  const isTransientClickFail = (
    !clickResult                                              // executeScript threw / tab busy
    || clickResult.error === "no_enabled_send_button"         // send still disabled
    || clickResult.error === "no_send_button_found"           // legacy variant of same
  );
  if (isTransientClickFail) {
    const reason = !clickResult ? "exec_failed (script context died)"
      : clickResult.error === "no_enabled_send_button" ? "send button still disabled"
      : "send button not found";
    await grokReportProgress(
      request_id, "click_transient_fail",
      `${reason} — will retry`
    );
    skipInitialPoll = true;
  } else if (clickResult.error) {
    const dbgStr = clickResult.debug
      ? ` | debug: ${JSON.stringify(clickResult.debug).slice(0, 300)}`
      : "";
    await grokSubmitResult(request_id, {
      error: `click_failed_${clickResult.error || "unknown"}`,
      detail:
        "Could not locate input or send button on grok.com/imagine. " +
        "Make sure the tab is open to /imagine and Video mode is selected." +
        dbgStr,
    });
    return;
  }
  if (!skipInitialPoll) {
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
  }

  // Poll for videoUrl — either from the fetch-wrapper's stream capture
  // (__grokAutomationVideoUrl) or from the DOM (<video> element src).
  // Grok typically finishes a 10s/720p video in 60-180 seconds.
  let videoUrl = "";
  let videoId = "";
  let progressPct = 0;
  let domFallbackUrl = "";
  // Pre-seed errorSeen so the no-enabled-send-button case skips the
  // 4-min poll loop and goes straight to the retry block — no point
  // polling for a video that was never submitted.
  let errorSeen = skipInitialPoll ? "click_no_effect" : "";
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

  while (Date.now() < pollDeadline && !errorSeen) {
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

  // ─────────────────────────────────────────────────────────────
  // Retry on click_no_effect — the click landed on a stale/disabled
  // button (common when Grok's send button hasn't finished its
  // enable transition, or when the page has leftover buttons from
  // prior completions). Re-navigate to /imagine, re-attach the
  // image if one was used, re-type, re-click. Up to 2 retries.
  // Anything else (click_failed, video_timeout, etc.) still fails
  // fast — those aren't transient click-placement issues.
  // ─────────────────────────────────────────────────────────────
  let retryCount = 0;
  while (errorSeen === "click_no_effect" && retryCount < 2) {
    retryCount++;
    await grokReportProgress(
      request_id,
      "click_retry",
      `attempt ${retryCount + 1} of 3 — bouncing to /imagine`
    );
    errorSeen = "";
    videoUrl = "";
    videoId = "";
    progressPct = 0;
    domFallbackUrl = "";
    lastReportedProgress = -1;

    // force:true so the composer state is wiped — without this, the
    // previous attempt's attached image would still be in the composer
    // and re-attaching here would stack a 2nd/3rd image onto the same
    // submission. Forcing window.location.replace gives us a fresh
    // composer with zero attachments every retry.
    const renav = await grokEnsureOnImaginePage(tabId, { force: true });
    if (renav.error) { errorSeen = `renav_${renav.error}`; break; }

    // Re-ensure Video mode + media settings after the bounce-back
    // nav — same reason as the initial dispatch: if Grok defaulted
    // to Image mode / 480p / 6s we'd generate the wrong output.
    try { await grokEnsureVideoMode(tabId); } catch {}
    try {
      await grokEnsureMediaSettings(tabId, {
        aspect_ratio: aspect_ratio || "",
        resolution: resolution || "",
        video_length: video_length ? String(video_length) : "",
      });
    } catch {}

    if (hasReference) {
      const reAttach = await grokAttachImage(
        tabId, reference_image_base64, reference_image_filename,
        reference_image_mime || "image/jpeg",
      );
      if (!reAttach || !reAttach.ok) {
        errorSeen = "retry_attach_failed";
        break;
      }
    }

    const reClick = await grokClickSend(tabId, messageForClick);
    // Same transient handling as the initial click — null reClick or
    // no_enabled_send_button means the retry attempt itself was hit
    // by the same condition. Loop continues if we still have retries
    // left (errorSeen stays "click_no_effect"), else surfaces below.
    const reClickTransient = (
      !reClick
      || reClick.error === "no_enabled_send_button"
      || reClick.error === "no_send_button_found"
    );
    if (reClickTransient) {
      const reason = !reClick ? "exec_failed"
        : reClick.error || "no_send_button";
      await grokReportProgress(
        request_id, "retry_click_transient",
        `${reason} — will try again if retries remain`
      );
      // Keep errorSeen as "click_no_effect" so the outer while loop
      // continues to the next retry iteration. Don't break.
      errorSeen = "click_no_effect";
      continue;
    }
    if (reClick.error) {
      errorSeen = `retry_click_${reClick.error}`;
      break;
    }
    await grokReportProgress(
      request_id, "clicked_retry",
      `${reClick.buttonLabel || "send"} score=${(reClick.buttonScore || 0).toFixed(1)}`
    );

    // Re-run the poll loop for this retry attempt.
    const retryInitialUrl = (await (async () => {
      try { const t = await chrome.tabs.get(tabId); return t.url || ""; }
      catch { return ""; }
    })()) || "";
    const retryPollDeadline = Date.now() + 240000;
    const retryClickEffectDeadline = Date.now() + 25000;
    urlNavigated = false;

    while (Date.now() < retryPollDeadline) {
      const st = await grokReadAutomationState(tabId);
      if (st.error) { errorSeen = st.error; break; }
      if (st.videoUrl) { videoUrl = st.videoUrl; videoId = st.videoId; break; }
      if (st.domVideoUrl && !domFallbackUrl) domFallbackUrl = st.domVideoUrl;
      if (typeof st.progress === "number" && st.progress !== lastReportedProgress) {
        progressPct = st.progress;
        lastReportedProgress = st.progress;
        if (st.progress > 0 && st.progress % 25 === 0) {
          await grokReportProgress(request_id, "progress", `${st.progress}%`);
        }
      }
      if (!urlNavigated) {
        try {
          const tb = await chrome.tabs.get(tabId);
          if (tb.url && tb.url !== retryInitialUrl && tb.url.includes("/imagine/post/")) {
            urlNavigated = true;
            await grokReportProgress(request_id, "post_page_reached", tb.url.slice(0, 100));
          }
        } catch {}
      }
      const hadEffect = urlNavigated || progressPct > 0 || !!domFallbackUrl || !!videoUrl;
      if (!hadEffect && Date.now() > retryClickEffectDeadline) {
        errorSeen = "click_no_effect";
        break;
      }
      await new Promise((r) => setTimeout(r, 2000));
    }
    await grokClearAutomationFlag(tabId);
    if (videoUrl || domFallbackUrl) break;  // succeeded on retry
  }

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

  // ─────────────────────────────────────────────────────────────
  // Download strategy:
  //   1. PRIMARY: Service-worker fetch. The SW (this background
  //      script) isn't subject to tab throttling, doesn't need the
  //      tab to be foreground, and uses Chrome's shared cookie jar.
  //      Pair with FileReader.readAsDataURL — native C++ base64
  //      encoding is ~3x faster than the manual btoa(charCodes) loop.
  //
  //   2. FALLBACK: In-tab fetch via grokExecInTab. Used only if SW
  //      fetch returns non-200 (e.g. CORS / cookie scope mismatch).
  //      Same 180s timeout applies.
  //
  // Net win on a typical 10MB video: ~25s → ~6s end-to-end.
  // ─────────────────────────────────────────────────────────────

  // Heartbeat every 20s — Python idle-timeout is 300s. We fire more
  // often than strictly needed so the user sees the download is alive.
  await grokReportProgress(request_id, "downloading", "fetch started (sw)");
  let downloadFinished = false;
  const heartbeat = (async () => {
    while (!downloadFinished) {
      await new Promise((r) => setTimeout(r, 20000));
      if (!downloadFinished) {
        try {
          await grokReportProgress(
            request_id, "downloading", "still fetching..."
          );
        } catch {}
      }
    }
  })();

  // SW-side fetch + FileReader-based base64
  const fetchInSW = async (url, timeoutMs) => {
    const ctrl = new AbortController();
    const tid = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const r = await fetch(url, {
        method: "GET",
        credentials: "include",
        signal: ctrl.signal,
      });
      if (!r.ok) return { status: r.status, error: "download_http" };
      const blob = await r.blob();
      // FileReader.readAsDataURL is implemented in C++ and runs an
      // order of magnitude faster than the JS char-code-then-btoa
      // approach for 10MB+ payloads. Strip the "data:...;base64,"
      // prefix to get just the base64 body.
      const dataUrl = await new Promise((res, rej) => {
        const fr = new FileReader();
        fr.onerror = () => rej(fr.error || new Error("FileReader error"));
        fr.onload = () => res(fr.result);
        fr.readAsDataURL(blob);
      });
      const commaIdx = dataUrl.indexOf(",");
      const b64 = commaIdx >= 0 ? dataUrl.slice(commaIdx + 1) : dataUrl;
      return { status: 200, size: blob.size, content_base64: b64 };
    } catch (e) {
      const name = (e && e.name) || "";
      return {
        status: 0,
        error: name === "AbortError" ? "download_timeout" : `fetch_exc_${String(e?.message || e).slice(0, 100)}`,
      };
    } finally {
      clearTimeout(tid);
    }
  };

  let downloadResult;
  try {
    downloadResult = await fetchInSW(fullVideoUrl, 180000);
    if (!downloadResult || downloadResult.status !== 200) {
      // Fallback: in-tab fetch. Bring tab forward only here since SW
      // didn't get it — minimizes UX disruption when SW path works.
      await grokReportProgress(
        request_id, "downloading",
        `sw fetch failed (${downloadResult?.error || downloadResult?.status}) — falling back to tab fetch`
      );
      try { await chrome.tabs.update(tabId, { active: true }); } catch {}
      downloadResult = await grokExecInTab(tabId, async (args) => {
        try {
          const ctrl = new AbortController();
          const tid = setTimeout(() => ctrl.abort(), args.timeout_ms);
          try {
            const r = await fetch(args.url, {
              method: "GET", credentials: "include", signal: ctrl.signal,
            });
            if (!r.ok) return { status: r.status, error: "download_http" };
            const blob = await r.blob();
            const dataUrl = await new Promise((res, rej) => {
              const fr = new FileReader();
              fr.onerror = () => rej(fr.error);
              fr.onload = () => res(fr.result);
              fr.readAsDataURL(blob);
            });
            const i = dataUrl.indexOf(",");
            const b64 = i >= 0 ? dataUrl.slice(i + 1) : dataUrl;
            return { status: 200, size: blob.size, content_base64: b64 };
          } finally {
            clearTimeout(tid);
          }
        } catch (e) {
          const name = (e && e.name) || "";
          return {
            status: 0,
            error: name === "AbortError" ? "download_timeout" : `fetch_exc_${String(e?.message || e).slice(0, 100)}`,
          };
        }
      }, { url: fullVideoUrl, timeout_ms: 180000 });
    }
  } finally {
    downloadFinished = true;
    try { await heartbeat; } catch {}
  }

  if (!downloadResult || downloadResult.status !== 200 || !downloadResult.content_base64) {
    await grokSubmitResult(request_id, {
      error: `download_failed_${downloadResult?.status || downloadResult?.error || "?"}`,
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
