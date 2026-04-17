/**
 * G-Labs Studio Helper — Chrome Extension Background Service Worker
 *
 * Architecture:
 *   Extension polls local Bridge Server (Python app) every 1.5s
 *   When work available → inject reCAPTCHA script into Labs tab (world: "MAIN")
 *   Send token + auth back to Bridge → Python makes direct API call
 *
 * Zero CDP, zero automation markers, real Chrome context.
 */

// Load persona system (ecosystem/warmup query pools)
try { importScripts("personas.js"); } catch (e) { console.warn("personas.js not loaded:", e); }

const BRIDGE_URL = "http://127.0.0.1:18924";
const POLL_INTERVAL = 1500;
const LABS_ORIGIN = "https://labs.google";
const ACCOUNT_DETECT_INTERVAL = 10000;

// ─── State ───
let bridgeConnected = false;
let connectedAccounts = {};  // tabId → { email, name, access_token, project_id }
let tokenCount = 0;
let lastPollError = "";
let _workInProgress = false;  // serialization lock — only one handleWork at a time

// ─── reCAPTCHA readiness cache (30s validity) ───
const _recaptchaCache = {};  // tabId → { valid: bool, ts: timestamp }
const _RECAPTCHA_CACHE_TTL = 30000;  // 30 seconds

// ─── Ecosystem / Auto Warmup Mode state ───
// Directive comes from bridge via /poll:
//   "disabled" = toggle off, do nothing
//   "paused"   = generation is running, stop activity
//   "active"   = idle, run warmup activity
let ecosystemDirective = "disabled";
let ecosystemHeldAccounts = {};  // email -> seconds_remaining
const ecosystemState = {
  running: false,       // an activity is currently executing
  currentAccount: "",   // which account's tab is active
  currentSite: "",      // e.g. "youtube"
  currentTabId: null,
  nextActivityAt: 0,    // timestamp when next activity should fire
  todayCounts: {},      // email -> int (reset daily locally)
  lastReset: 0,         // date tracker
};
const ECOSYSTEM_MIN_GAP_MS = 15 * 60 * 1000;   // 15 min
const ECOSYSTEM_MAX_GAP_MS = 40 * 60 * 1000;   // 40 min
const ECOSYSTEM_MAX_PER_DAY = 35;              // safety cap per account per day

// ═══════════════════════════════════════════════════════════════════
// Bridge Communication
// ═══════════════════════════════════════════════════════════════════

async function pollBridge() {
  try {
    // Tell bridge which accounts THIS extension instance has
    // so it only gives us work we can handle (multi-profile support)
    const myAccounts = Object.values(connectedAccounts)
      .filter((a) => a.logged_in && a.email)
      .map((a) => a.email);
    const accountsParam = myAccounts.length
      ? `?accounts=${encodeURIComponent(myAccounts.join(","))}`
      : "";

    const resp = await fetch(`${BRIDGE_URL}/poll${accountsParam}`, {
      method: "GET",
      headers: { "Accept": "application/json" },
    });

    if (!resp.ok) {
      bridgeConnected = false;
      lastPollError = `HTTP ${resp.status}`;
      return;
    }

    bridgeConnected = true;
    lastPollError = "";
    const data = await resp.json();

    // Handle pending work (token request) — serialize to avoid
    // 6 concurrent executeScript calls on the same tab hanging Chrome
    if (data.work && !_workInProgress) {
      _workInProgress = true;
      try {
        await handleWork(data.work);
      } finally {
        _workInProgress = false;
      }
    }

    // Handle commands (cookie clear, tab reload, etc.)
    if (data.command) {
      await handleCommand(data.command);
    }

    // ─── Ecosystem directive from bridge ───
    if (data.ecosystem) {
      const prevDirective = ecosystemDirective;
      ecosystemDirective = data.ecosystem.directive || "disabled";
      ecosystemHeldAccounts = data.ecosystem.held_accounts || {};

      if (prevDirective !== ecosystemDirective) {
        console.log(`[Ecosystem] Directive changed: ${prevDirective} → ${ecosystemDirective}`);
      }

      // If we were running an activity and directive is now paused/disabled,
      // abort it IMMEDIATELY so generation never shares bandwidth/CPU.
      if (ecosystemState.running && ecosystemDirective !== "active") {
        await ecosystemAbortCurrent("directive_changed:" + ecosystemDirective);
      }
    }
  } catch (e) {
    bridgeConnected = false;
    lastPollError = e.message || "fetch failed";
  }
}

// ═══════════════════════════════════════════════════════════════════
// Token Generation — world: "MAIN"
// ═══════════════════════════════════════════════════════════════════

async function handleWork(work) {
  const { request_id, account, action } = work;

  // ─── GET_COOKIES: return all cookies for labs.google (including .google.com parent) ───
  if (action === "GET_COOKIES") {
    try {
      const cookies = await chrome.cookies.getAll({ url: "https://labs.google/" });
      const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join("; ");
      await submitResult(request_id, { cookies: cookieStr });
    } catch (e) {
      await submitResult(request_id, { error: e.message || "cookies_error" });
    }
    return;
  }

  // ─── DOWNLOAD_MEDIA: resolve redirect URL via webRequest + MAIN world fetch (fallback) ───
  if (action && action.startsWith("DOWNLOAD_MEDIA:")) {
    const mediaUrl = action.slice("DOWNLOAD_MEDIA:".length);
    const allLabsTabs = await chrome.tabs.query({ url: `${LABS_ORIGIN}/*` });
    const tabId = allLabsTabs.length > 0 ? allLabsTabs[0].id : null;
    if (!tabId) {
      await submitResult(request_id, { error: "no_labs_tab_for_download" });
      return;
    }
    try {
      // 1. Set up webRequest listener to capture the 307 redirect at network level
      //    (fires BEFORE CORS, so we get the Location even though JS fetch will fail)
      const redirectPromise = new Promise((resolve) => {
        let resolved = false;
        const listener = (details) => {
          if (!resolved && details.url.includes("getMediaUrlRedirect")) {
            resolved = true;
            chrome.webRequest.onBeforeRedirect.removeListener(listener);
            resolve(details.redirectUrl || null);
          }
        };
        chrome.webRequest.onBeforeRedirect.addListener(
          listener,
          { urls: ["https://labs.google/fx/api/trpc/media.getMediaUrlRedirect*"] }
        );
        // Timeout fallback
        setTimeout(() => {
          if (!resolved) {
            resolved = true;
            chrome.webRequest.onBeforeRedirect.removeListener(listener);
            resolve(null);
          }
        }, 15000);
      });

      // 2. Fire the request from MAIN world (has page cookies, same-origin)
      //    We don't care about the fetch result — webRequest captures the redirect
      await chrome.scripting.executeScript({
        target: { tabId },
        world: "MAIN",
        func: (url) => {
          fetch(url, { credentials: "include", redirect: "follow" }).catch(() => {});
        },
        args: [mediaUrl],
      });

      // 3. Wait for the redirect URL from webRequest listener
      const cdnUrl = await redirectPromise;
      if (cdnUrl) {
        await submitResult(request_id, { cdn_url: cdnUrl });
      } else {
        await submitResult(request_id, { error: "no_redirect_captured" });
      }
    } catch (e) {
      await submitResult(request_id, { error: e.message || "download_resolve_error" });
    }
    return;
  }

  // Check if any Labs tab exists at all (before reCAPTCHA validation)
  const allLabsTabs = await chrome.tabs.query({ url: `${LABS_ORIGIN}/*` });

  // Find a Labs tab with reCAPTCHA ready for this account
  const tabId = await findLabsTab(account);
  if (!tabId) {
    // Distinguish: no tab at all vs tab exists but reCAPTCHA not loaded
    if (allLabsTabs.length > 0) {
      await submitResult(request_id, {
        error: "no_recaptcha_enterprise",  // tab exists, reCAPTCHA not ready (auto-reloading)
      });
    } else {
      await submitResult(request_id, {
        error: `no_labs_tab_for_${account || "any"}`,
      });
    }
    return;
  }

  try {
    // Single injection: get auth + reCAPTCHA token + project ID in one shot
    const results = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: mainWorldExecute,
      args: [action],
    });

    const result = results?.[0]?.result;
    if (!result) {
      await submitResult(request_id, { error: "script_returned_null" });
      return;
    }

    if (result.token) {
      tokenCount++;
      // Update cached account info
      if (result.email) {
        connectedAccounts[tabId] = {
          email: result.email,
          name: result.name || "",
          access_token: result.access_token || "",
          project_id: result.project_id || "",
          logged_in: true,
        };
      }
    }

    await submitResult(request_id, result);
  } catch (e) {
    await submitResult(request_id, { error: e.message || "script_error" });
  }
}

/**
 * Resolve a media redirect URL using the page's cookies.
 * Fetches the URL with credentials, follows redirect, returns the final CDN URL.
 * Runs in world: "MAIN" so it has the page's cookies.
 */
async function resolveMediaUrl(mediaUrl) {
  try {
    // Use redirect: "manual" to capture the 307 Location header
    // (redirect: "follow" fails because GCS is cross-origin)
    const resp = await fetch(mediaUrl, {
      method: "GET",
      credentials: "include",
      redirect: "manual",
    });
    // 307 redirect — extract Location from the opaque-redirect response
    if (resp.type === "opaqueredirect" || resp.status === 0) {
      // Can't read Location from opaque redirect in fetch API.
      // Fall back to XMLHttpRequest which CAN read redirect headers.
      return await new Promise((resolve) => {
        const xhr = new XMLHttpRequest();
        xhr.open("GET", mediaUrl, true);
        xhr.withCredentials = true;
        // Prevent following redirect so we can read Location
        xhr.onreadystatechange = function () {
          if (xhr.readyState === 2) {  // HEADERS_RECEIVED
            const loc = xhr.getResponseHeader("Location");
            if (loc) {
              xhr.abort();
              resolve({ cdn_url: loc });
              return;
            }
          }
          if (xhr.readyState === 4) {
            if (xhr.status >= 200 && xhr.status < 400) {
              // Got actual data (no redirect)
              resolve({ cdn_url: xhr.responseURL || mediaUrl });
            } else {
              resolve({ error: `xhr_failed_${xhr.status}` });
            }
          }
        };
        xhr.onerror = () => resolve({ error: "xhr_network_error" });
        xhr.send();
      });
    }
    if (resp.status >= 300 && resp.status < 400) {
      const loc = resp.headers.get("Location");
      if (loc) return { cdn_url: loc };
    }
    if (resp.ok) {
      // resp.url contains the final URL after redirects
      if (resp.url && resp.url !== mediaUrl) {
        return { cdn_url: resp.url };
      }
    }
    return { error: `fetch_failed_${resp.status}` };
  } catch (e) {
    return { error: e.message || "resolve_fetch_error" };
  }
}

/**
 * This function runs in the page's TRUE main world (world: "MAIN").
 * No CDP traces. reCAPTCHA sees it as the page's own JavaScript.
 *
 * Returns: { token, access_token, email, name, project_id, error }
 */
async function mainWorldExecute(action) {
  const result = { token: null, access_token: null, email: null, name: null, project_id: null, error: null };

  try {
    // ─── 1. Auth Session ───
    try {
      const authResp = await fetch("https://labs.google/fx/api/auth/session", {
        method: "GET",
        credentials: "include",
      });
      if (authResp.ok) {
        const auth = await authResp.json().catch(() => null);
        if (auth) {
          result.access_token = auth.access_token || null;
          result.email = auth.email || (auth.user && auth.user.email) || null;
          result.name = auth.name || (auth.user && auth.user.name) || null;
        }
      }
    } catch {}

    if (!result.access_token) {
      result.error = "no_auth_session";
      return result;
    }

    // ─── 2. Project ID (from URL → DOM → API fallback) ───
    try {
      const urlMatch = window.location.href.match(/\/project\/([a-z0-9-]{16,})/i);
      if (urlMatch) {
        result.project_id = urlMatch[1];
      } else {
        // Try DOM scrape
        const links = document.querySelectorAll("a[href*='/project/']");
        for (const link of links) {
          const m = (link.href || "").match(/\/project\/([a-z0-9-]{16,})/i);
          if (m) { result.project_id = m[1]; break; }
        }
      }
      // API fallback: fetch projects list if still no project_id
      if (!result.project_id && result.access_token) {
        try {
          const projResp = await fetch(
            "https://aisandbox-pa.googleapis.com/v1/projects",
            { method: "GET", credentials: "include",
              headers: { "authorization": "Bearer " + result.access_token } }
          );
          if (projResp.ok) {
            const projData = await projResp.json().catch(() => null);
            // Response: { projects: [{ name: "projects/abc-123", ... }] }
            const projects = projData && (projData.projects || projData.project || []);
            if (Array.isArray(projects) && projects.length > 0) {
              const pName = projects[0].name || projects[0].projectId || "";
              const pMatch = pName.match(/([a-z0-9-]{16,})/i);
              if (pMatch) result.project_id = pMatch[1];
            }
          }
        } catch {}
      }
    } catch {}

    // ─── 3. reCAPTCHA Token ───
    try {
      const enterprise = window.grecaptcha && window.grecaptcha.enterprise;
      if (!enterprise || typeof enterprise.execute !== "function") {
        result.error = "no_recaptcha_enterprise";
        return result;
      }

      // Primary: ___grecaptcha_cfg.clients (internal config object)
      let siteKey = null;
      try {
        const clients = window.___grecaptcha_cfg && window.___grecaptcha_cfg.clients;
        if (clients) {
          for (const id of Object.keys(clients)) {
            const client = clients[id];
            if (!client || typeof client !== "object") continue;
            const walk = (obj, depth) => {
              if (depth > 5 || !obj || typeof obj !== "object") return null;
              for (const key of Object.keys(obj)) {
                const val = obj[key];
                if (typeof val === "string" && val.length >= 20 && val.length <= 50
                  && /^[A-Za-z0-9_-]+$/.test(val)) {
                  const check = document.querySelector('script[src*="render=' + val + '"]');
                  if (check) return val;
                }
                if (typeof val === "object" && val !== null) {
                  const found = walk(val, depth + 1);
                  if (found) return found;
                }
              }
              return null;
            };
            siteKey = walk(client, 0);
            if (siteKey) break;
          }
        }
      } catch {}

      // Fallback: script tag parsing
      if (!siteKey) {
        for (const s of document.querySelectorAll('script[src*="recaptcha"][src*="render="]')) {
          try {
            const render = new URL(s.src).searchParams.get("render");
            if (render && render !== "explicit") { siteKey = render; break; }
          } catch {}
        }
      }

      if (!siteKey) {
        result.error = "no_sitekey";
        return result;
      }

      // Ready + Execute — zero delays
      if (typeof enterprise.ready === "function") {
        await new Promise((r) => enterprise.ready(r));
      }

      const token = await enterprise.execute(siteKey, { action });
      if (token) {
        result.token = token;
      } else {
        result.error = "execute_returned_null";
      }
    } catch (e) {
      result.error = e.message || "recaptcha_error";
    }

    return result;
  } catch (e) {
    result.error = e.message || "main_world_error";
    return result;
  }
}

// ═══════════════════════════════════════════════════════════════════
// Submit Result to Bridge
// ═══════════════════════════════════════════════════════════════════

async function submitResult(requestId, data) {
  try {
    await fetch(`${BRIDGE_URL}/token`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        request_id: requestId,
        ...data,
      }),
    });
  } catch {}
}

// ═══════════════════════════════════════════════════════════════════
// Tab Management
// ═══════════════════════════════════════════════════════════════════

async function checkRecaptchaReady(tabId) {
  // Check cache first (valid for 30s)
  const cached = _recaptchaCache[tabId];
  if (cached && (Date.now() - cached.ts) < _RECAPTCHA_CACHE_TTL) {
    return cached.valid;
  }

  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: () => {
        return typeof grecaptcha !== "undefined" && !!grecaptcha.enterprise
          && typeof grecaptcha.enterprise.execute === "function";
      },
    });
    const valid = !!(results?.[0]?.result);
    _recaptchaCache[tabId] = { valid, ts: Date.now() };
    return valid;
  } catch {
    _recaptchaCache[tabId] = { valid: false, ts: Date.now() };
    return false;
  }
}

function invalidateRecaptchaCache(tabId) {
  if (tabId) delete _recaptchaCache[tabId];
  else Object.keys(_recaptchaCache).forEach(k => delete _recaptchaCache[k]);
}

async function findLabsTab(targetAccount) {
  const tabs = await chrome.tabs.query({ url: `${LABS_ORIGIN}/*` });

  if (!tabs.length) return null;

  // If specific account requested, find matching tab with reCAPTCHA ready
  if (targetAccount) {
    // Check cache first — but also verify reCAPTCHA is ready
    for (const tab of tabs) {
      const cached = connectedAccounts[tab.id];
      if (cached && cached.email === targetAccount) {
        if (await checkRecaptchaReady(tab.id)) {
          return tab.id;
        }
        // reCAPTCHA not ready on this tab — try others
      }
    }

    // Cache miss — check each tab via script
    for (const tab of tabs) {
      try {
        const results = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          world: "MAIN",
          func: async () => {
            try {
              const resp = await fetch("https://labs.google/fx/api/auth/session", {
                method: "GET", credentials: "include",
              });
              const data = await resp.json().catch(() => null);
              if (!data) return null;
              return {
                email: data.email || (data.user && data.user.email) || "",
                name: data.name || (data.user && data.user.name) || "",
                logged_in: !!data.access_token,
              };
            } catch { return null; }
          },
        });

        const auth = results?.[0]?.result;
        if (auth && auth.email === targetAccount) {
          connectedAccounts[tab.id] = { ...auth, logged_in: true };
          if (await checkRecaptchaReady(tab.id)) {
            return tab.id;
          }
        }
      } catch {}
    }

    // No tab has reCAPTCHA ready — auto-reload first matching account tab
    for (const tab of tabs) {
      const cached = connectedAccounts[tab.id];
      if (cached && cached.email === targetAccount) {
        try {
          await chrome.tabs.reload(tab.id);
          invalidateRecaptchaCache(tab.id);
        } catch {}
        // Return null — caller will get no_recaptcha, Python side will wait & retry
        return null;
      }
    }

    return null;
  }

  // No specific account — return first Labs tab with reCAPTCHA ready
  for (const tab of tabs) {
    if (await checkRecaptchaReady(tab.id)) {
      return tab.id;
    }
  }

  // None ready — reload first tab
  try {
    await chrome.tabs.reload(tabs[0].id);
    invalidateRecaptchaCache(tabs[0].id);
  } catch {}
  return null;
}

// ═══════════════════════════════════════════════════════════════════
// Account Detection
// ═══════════════════════════════════════════════════════════════════

async function detectAccounts() {
  const tabs = await chrome.tabs.query({ url: `${LABS_ORIGIN}/*` });
  const accounts = [];

  for (const tab of tabs) {
    try {
      const results = await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        world: "MAIN",
        func: async () => {
          try {
            const resp = await fetch("https://labs.google/fx/api/auth/session", {
              method: "GET", credentials: "include",
            });
            if (!resp.ok) return null;
            const data = await resp.json().catch(() => null);
            if (!data || !data.access_token) return null;

            // Also try to get project ID from URL
            let projectId = null;
            const urlMatch = window.location.href.match(/\/project\/([a-z0-9-]{16,})/i);
            if (urlMatch) projectId = urlMatch[1];

            return {
              email: data.email || (data.user && data.user.email) || "",
              name: data.name || (data.user && data.user.name) || "",
              access_token: data.access_token,
              project_id: projectId,
              logged_in: true,
            };
          } catch { return null; }
        },
      });

      const auth = results?.[0]?.result;
      if (auth && auth.logged_in) {
        connectedAccounts[tab.id] = auth;
        accounts.push({
          email: auth.email,
          name: auth.name,
          tab_id: tab.id,
          project_id: auth.project_id,
        });
      }
    } catch {}
  }

  // Report to bridge
  try {
    await fetch(`${BRIDGE_URL}/accounts`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ accounts }),
    });
  } catch {}

  return accounts;
}

// ═══════════════════════════════════════════════════════════════════
// Commands from Bridge
// ═══════════════════════════════════════════════════════════════════

async function handleCommand(cmd) {
  const { type, account } = cmd;
  const tabId = await findLabsTab(account);

  switch (type) {
    case "clear_cookies":
      try {
        const cookies = await chrome.cookies.getAll({ domain: ".google.com" });
        for (const c of cookies) {
          if (c.domain.includes("labs.google")) {
            await chrome.cookies.remove({
              url: `https://labs.google${c.path}`,
              name: c.name,
            });
          }
        }
      } catch {}
      break;

    case "reload_tab":
      if (tabId) {
        try { await chrome.tabs.reload(tabId); } catch {}
      }
      break;

    case "clean_tracking":
      // Remove Service Workers + IndexedDB for labs.google — preserves login cookies
      try {
        await chrome.browsingData.remove(
          { origins: ["https://labs.google"] },
          {
            serviceWorkers: true,
            indexedDB: true,
            cacheStorage: true,
          }
        );
        console.log(`[G-Labs Helper] Tracking data cleaned for ${account || "all accounts"}`);
      } catch (e) {
        console.warn("[G-Labs Helper] clean_tracking failed:", e.message);
      }
      // Reload tab after cleanup so reCAPTCHA re-initializes fresh
      if (tabId) {
        try { await chrome.tabs.reload(tabId); } catch {}
      }
      break;

    case "clean_recaptcha_cookie":
      // Delete _GRECAPTCHA cookie only — keeps login session intact
      try {
        const allCookies = await chrome.cookies.getAll({ domain: ".google.com" });
        let deleted = 0;
        for (const c of allCookies) {
          if (c.name === "_GRECAPTCHA" || c.name.startsWith("_GRECAPTCHA")) {
            const protocol = c.secure ? "https" : "http";
            await chrome.cookies.remove({
              url: `${protocol}://${c.domain.replace(/^\./, "")}${c.path}`,
              name: c.name,
            });
            deleted++;
          }
        }
        console.log(`[G-Labs Helper] Removed ${deleted} _GRECAPTCHA cookie(s)`);
      } catch (e) {
        console.warn("[G-Labs Helper] clean_recaptcha_cookie failed:", e.message);
      }
      break;

    case "new_project":
      if (tabId) {
        try {
          const results = await chrome.scripting.executeScript({
            target: { tabId },
            world: "MAIN",
            func: async () => {
              // Click "New project" button
              const btn = document.querySelector('[data-testid="new-project"], button');
              const allBtns = Array.from(document.querySelectorAll("button"));
              const newProj = allBtns.find((b) => b.textContent?.trim() === "New project");
              if (newProj) {
                newProj.click();
                await new Promise((r) => setTimeout(r, 3000));
                const m = window.location.href.match(/\/project\/([a-z0-9-]{16,})/i);
                return m ? m[1] : null;
              }
              return null;
            },
          });
          const pid = results?.[0]?.result;
          if (pid) {
            await fetch(`${BRIDGE_URL}/project`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ account, project_id: pid }),
            });
          }
        } catch {}
      }
      break;
  }
}

// ═══════════════════════════════════════════════════════════════════
// Auto Warmup Mode — Ecosystem Activity Engine (Phase 1)
// ═══════════════════════════════════════════════════════════════════

// ─── Basic search query pool (Phase 3 will replace with per-persona pools) ───
const ECOSYSTEM_SEARCH_QUERIES = [
  "best pizza near me", "weather today", "cricket score", "bollywood news",
  "tech news today", "best mobile under 20000", "laptop deals",
  "recipe for pasta", "stock market today", "ipl schedule",
  "car reviews", "youtube trending", "movie reviews", "best smartphone 2026",
  "food near me", "chai recipe", "how to cook biryani", "shopping deals",
  "health tips", "morning workout", "news live", "cricket news today",
  "upcoming movies 2026", "travel destinations", "best restaurants in delhi",
  "electric car price", "iphone 17 features", "samsung new phone",
  "football news", "world news", "share market tips", "gold rate today",
];

const ECOSYSTEM_YT_QUERIES = [
  "funny cricket moments", "cooking at home", "car review", "phone unboxing",
  "bollywood songs", "motivational videos", "how to make pizza",
  "travel vlog india", "tech tips", "fitness workout", "ipl highlights",
  "movie trailer", "cricket highlights", "street food", "gaming stream",
];

const ECOSYSTEM_MAPS_QUERIES = [
  "restaurants near me", "coffee shops", "shopping mall", "petrol pump",
  "ATM nearby", "pharmacy near me", "hospital", "park", "gym near me",
  "metro station", "hotel near me", "movie theater", "grocery store",
];

// Activity pool — each activity has an executor function.
const ECOSYSTEM_ACTIVITIES = [
  { name: "youtube",  weight: 30, url: "https://www.youtube.com/",        duration: [120, 300], run: activityYouTube },
  { name: "gmail",    weight: 20, url: "https://mail.google.com/",        duration: [60, 120],  run: activityGmail },
  { name: "search",   weight: 15, url: "https://www.google.com/",         duration: [60, 180],  run: activitySearch },
  { name: "drive",    weight: 10, url: "https://drive.google.com/",       duration: [60, 120],  run: activityDrive },
  { name: "maps",     weight: 10, url: "https://www.google.com/maps",     duration: [60, 120],  run: activityMaps },
  { name: "news",     weight: 10, url: "https://news.google.com/",        duration: [60, 180],  run: activityNews },
  { name: "photos",   weight: 5,  url: "https://photos.google.com/",      duration: [60, 120],  run: activityPhotos },
];

// Helper: pick a random item from array
function rand(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randInt(min, max) { return Math.floor(min + Math.random() * (max - min + 1)); }
function randFloat(min, max) { return min + Math.random() * (max - min); }

// Helper: sleep with early-abort checks
async function abortableSleep(ms, checkFn) {
  const step = 500;
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (checkFn && checkFn()) throw new Error("aborted");
    await new Promise((r) => setTimeout(r, Math.min(step, ms - (Date.now() - start))));
  }
}

// Inject a function into the target tab's MAIN world and get result.
async function injectInTab(tabId, fn, args) {
  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: fn,
      args: args || [],
    });
    return results?.[0]?.result;
  } catch (e) {
    return null;
  }
}

// Wait for tab to finish loading (polls webNavigation via tab status)
async function waitForTabLoad(tabId, maxMs) {
  const start = Date.now();
  while (Date.now() - start < (maxMs || 15000)) {
    try {
      const t = await chrome.tabs.get(tabId);
      if (t && t.status === "complete") return true;
    } catch { return false; }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}

// ═══════════════════════════════════════════════════════════════════
// Site-specific activity executors
// Each returns when activity is done. Throws on abort/error.
// All interactions run inside MAIN world so they look like real user events.
// ═══════════════════════════════════════════════════════════════════

// Common: natural scroll in the tab
async function naturalScroll(tabId, scrolls, checkAbort) {
  for (let i = 0; i < scrolls; i++) {
    if (checkAbort && checkAbort()) throw new Error("aborted");
    const direction = Math.random() < 0.85 ? 1 : -1;   // mostly down, sometimes up
    const amount = randInt(200, 700) * direction;
    await injectInTab(tabId, (amt) => {
      window.scrollBy({ top: amt, behavior: "smooth" });
    }, [amount]);
    await abortableSleep(randInt(1500, 4500), checkAbort);
  }
}

// YouTube: persona search, scroll, click a video, watch for most of duration
async function activityYouTube(tabId, duration, checkAbort, account) {
  await waitForTabLoad(tabId, 15000);
  await abortableSleep(randInt(2000, 4000), checkAbort);

  // 60% chance: search using persona-driven query; 40% chance: browse homepage
  const doSearch = Math.random() < 0.6 && typeof personaForAccount === "function" && account;
  if (doSearch) {
    const q = personaYoutubeQuery(personaForAccount(account));
    await injectInTab(tabId, async (query) => {
      const box = document.querySelector("input#search, input[name='search_query']");
      if (!box) return false;
      box.focus();
      box.value = "";
      for (const ch of query) {
        box.value += ch;
        box.dispatchEvent(new Event("input", { bubbles: true }));
        await new Promise((r) => setTimeout(r, 70 + Math.random() * 140));
      }
      const form = box.form || box.closest("form");
      if (form) form.submit();
      else box.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
      return true;
    }, [q]);
    await abortableSleep(randInt(3000, 5000), checkAbort);
    await waitForTabLoad(tabId, 10000);
  }

  // Scroll 2-4 times
  await naturalScroll(tabId, randInt(2, 4), checkAbort);

  // Click a video thumbnail (random from visible)
  await injectInTab(tabId, () => {
    const thumbs = Array.from(
      document.querySelectorAll("a#thumbnail, ytd-thumbnail a, a.ytd-thumbnail")
    ).filter((a) => a.href && a.href.includes("watch"));
    if (thumbs.length) {
      const pick = thumbs[Math.floor(Math.random() * Math.min(thumbs.length, 10))];
      pick.click();
      return true;
    }
    return false;
  });

  await abortableSleep(randInt(3000, 5000), checkAbort);

  // Mute for battery + bandwidth safety; lower quality if possible
  await injectInTab(tabId, () => {
    const vid = document.querySelector("video");
    if (vid) {
      vid.muted = true;
      vid.volume = 0;
      // Lower quality is controlled via player menu — skip for now
    }
  });

  // Watch — during watch, scroll comments occasionally
  const watchTime = Math.max(30000, duration - 20000);
  const start = Date.now();
  while (Date.now() - start < watchTime) {
    if (checkAbort && checkAbort()) throw new Error("aborted");
    await abortableSleep(randInt(10000, 25000), checkAbort);
    // 40% chance to scroll comments
    if (Math.random() < 0.4) {
      await injectInTab(tabId, () => {
        window.scrollBy({ top: 400 + Math.random() * 500, behavior: "smooth" });
      });
    }
  }
}

// Gmail: open inbox, scroll, hover/click emails briefly
async function activityGmail(tabId, duration, checkAbort) {
  await waitForTabLoad(tabId, 20000);
  await abortableSleep(randInt(3000, 6000), checkAbort);
  await naturalScroll(tabId, randInt(3, 6), checkAbort);

  // Try to click first email to read
  await injectInTab(tabId, () => {
    const row = document.querySelector("tr[role='row'], div[role='main'] tr");
    if (row) row.click();
  });
  await abortableSleep(randInt(5000, 12000), checkAbort);

  // Go back to inbox
  await injectInTab(tabId, () => {
    const back = document.querySelector("[aria-label='Back to Inbox'], [aria-label='Back to inbox']");
    if (back) back.click();
  });
  // Scroll rest
  await naturalScroll(tabId, randInt(2, 4), checkAbort);
}

// Google Search: type a query, submit, scroll results, click one
async function activitySearch(tabId, duration, checkAbort, account) {
  await waitForTabLoad(tabId, 15000);
  await abortableSleep(randInt(1500, 3500), checkAbort);

  // Prefer persona-driven query if available
  let query;
  if (typeof personaForAccount === "function" && account) {
    query = personaSearchQuery(personaForAccount(account));
  } else {
    query = rand(ECOSYSTEM_SEARCH_QUERIES);
  }
  // Type query into search box (simulate character-by-character)
  await injectInTab(tabId, async (q) => {
    const box = document.querySelector("textarea[name='q'], input[name='q']");
    if (!box) return false;
    box.focus();
    box.value = "";
    for (const ch of q) {
      box.value += ch;
      box.dispatchEvent(new Event("input", { bubbles: true }));
      await new Promise((r) => setTimeout(r, 70 + Math.random() * 150));
    }
    // Submit form
    const form = box.form || box.closest("form");
    if (form) form.submit();
    else box.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    return true;
  }, [query]);

  await abortableSleep(randInt(3000, 5000), checkAbort);
  await waitForTabLoad(tabId, 10000);
  await naturalScroll(tabId, randInt(2, 5), checkAbort);

  // Click a result (50% chance)
  if (Math.random() < 0.5) {
    await injectInTab(tabId, () => {
      const links = Array.from(document.querySelectorAll("#search a h3, #rso a h3"));
      if (links.length) {
        const pick = links[Math.floor(Math.random() * Math.min(5, links.length))];
        pick.click();
      }
    });
    await abortableSleep(randInt(5000, 15000), checkAbort);
    await naturalScroll(tabId, randInt(2, 4), checkAbort);
  }
}

// Google Maps: type a location, view results
async function activityMaps(tabId, duration, checkAbort, account) {
  await waitForTabLoad(tabId, 20000);
  await abortableSleep(randInt(3000, 6000), checkAbort);

  let q;
  if (typeof personaForAccount === "function" && account) {
    q = personaMapsQuery(personaForAccount(account));
  } else {
    q = rand(ECOSYSTEM_MAPS_QUERIES);
  }
  await injectInTab(tabId, async (query) => {
    const box = document.querySelector("input#searchboxinput, input[name='q'], input[placeholder]");
    if (!box) return false;
    box.focus();
    box.value = "";
    for (const ch of query) {
      box.value += ch;
      box.dispatchEvent(new Event("input", { bubbles: true }));
      await new Promise((r) => setTimeout(r, 80 + Math.random() * 130));
    }
    box.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
    return true;
  }, [q]);

  await abortableSleep(randInt(5000, 10000), checkAbort);
  await naturalScroll(tabId, randInt(2, 4), checkAbort);
}

// Google Drive: scroll
async function activityDrive(tabId, duration, checkAbort) {
  await waitForTabLoad(tabId, 15000);
  await abortableSleep(randInt(3000, 6000), checkAbort);
  await naturalScroll(tabId, randInt(3, 6), checkAbort);
}

// Google News: scroll, click article
async function activityNews(tabId, duration, checkAbort) {
  await waitForTabLoad(tabId, 15000);
  await abortableSleep(randInt(2000, 4000), checkAbort);
  await naturalScroll(tabId, randInt(3, 5), checkAbort);

  if (Math.random() < 0.5) {
    await injectInTab(tabId, () => {
      const links = Array.from(document.querySelectorAll("article a, a[href*='/articles/']"));
      if (links.length) {
        const pick = links[Math.floor(Math.random() * Math.min(10, links.length))];
        pick.click();
      }
    });
    await abortableSleep(randInt(5000, 15000), checkAbort);
    await naturalScroll(tabId, randInt(2, 4), checkAbort);
  }
}

// Google Photos: scroll
async function activityPhotos(tabId, duration, checkAbort) {
  await waitForTabLoad(tabId, 15000);
  await abortableSleep(randInt(3000, 6000), checkAbort);
  await naturalScroll(tabId, randInt(3, 5), checkAbort);
}

function ecosystemPickActivity() {
  const totalWeight = ECOSYSTEM_ACTIVITIES.reduce((s, a) => s + a.weight, 0);
  let r = Math.random() * totalWeight;
  for (const a of ECOSYSTEM_ACTIVITIES) {
    r -= a.weight;
    if (r <= 0) return a;
  }
  return ECOSYSTEM_ACTIVITIES[0];
}

function ecosystemPickAccount() {
  // Pick a random logged-in account that isn't held and hasn't hit daily cap.
  const today = new Date().toISOString().slice(0, 10);
  if (ecosystemState.lastReset !== today) {
    ecosystemState.todayCounts = {};
    ecosystemState.lastReset = today;
  }
  const candidates = Object.values(connectedAccounts)
    .filter((a) => a.logged_in && a.email)
    .filter((a) => !(a.email in ecosystemHeldAccounts))
    .filter((a) => (ecosystemState.todayCounts[a.email] || 0) < ECOSYSTEM_MAX_PER_DAY);
  if (!candidates.length) return null;
  return candidates[Math.floor(Math.random() * candidates.length)];
}

// Gaussian-ish random gap (Box-Muller), time-of-day aware.
// Hours 1 AM - 7 AM: huge gaps (simulate sleep).
// Hours 11 PM - 1 AM and 7 AM - 9 AM: reduced activity.
function ecosystemNextGap() {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  const z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  const mean = (ECOSYSTEM_MIN_GAP_MS + ECOSYSTEM_MAX_GAP_MS) / 2;
  const std = (ECOSYSTEM_MAX_GAP_MS - ECOSYSTEM_MIN_GAP_MS) / 4;
  let gap = mean + z * std;
  gap = Math.max(ECOSYSTEM_MIN_GAP_MS, Math.min(ECOSYSTEM_MAX_GAP_MS, gap));

  // Time-of-day multiplier
  const hr = new Date().getHours();
  let mult = 1.0;
  if (hr >= 1 && hr < 7) mult = 4.0;       // deep sleep: 4x longer gaps
  else if (hr >= 23 || hr < 1) mult = 2.0; // late night: 2x
  else if (hr >= 7 && hr < 9) mult = 1.5;  // early morning: 1.5x
  return Math.floor(gap * mult);
}

async function ecosystemReportActivity(account, site, action, durationSec) {
  try {
    await fetch(`${BRIDGE_URL}/ecosystem/activity`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ account, site, action, duration_sec: durationSec || 0 }),
    });
  } catch {}
}

async function ecosystemAbortCurrent(reason) {
  if (!ecosystemState.running) return;
  const { currentAccount, currentSite, currentTabId } = ecosystemState;
  console.log(`[Ecosystem] Aborting ${currentSite} on ${currentAccount}: ${reason}`);
  ecosystemState.running = false;
  if (currentTabId) {
    try { await chrome.tabs.remove(currentTabId); } catch {}
  }
  ecosystemState.currentTabId = null;
  ecosystemState.currentAccount = "";
  ecosystemState.currentSite = "";
  ecosystemReportActivity(currentAccount, currentSite, "abort", 0);
}

async function ecosystemRunActivity() {
  if (ecosystemState.running) return;
  if (ecosystemDirective !== "active") return;

  const account = ecosystemPickAccount();
  if (!account) {
    console.log("[Ecosystem] No eligible accounts — waiting");
    ecosystemState.nextActivityAt = Date.now() + 5 * 60 * 1000;
    return;
  }
  const activity = ecosystemPickActivity();
  const [minDur, maxDur] = activity.duration;
  const duration = Math.floor(minDur + Math.random() * (maxDur - minDur)) * 1000;

  ecosystemState.running = true;
  ecosystemState.currentAccount = account.email;
  ecosystemState.currentSite = activity.name;

  console.log(
    `[Ecosystem] ${account.email} → ${activity.name} for ${Math.round(duration / 1000)}s`
  );
  ecosystemReportActivity(account.email, activity.name, "start", 0);

  let tabId = null;
  const startedAtWall = Date.now();
  try {
    // Open background tab (invisible to user)
    const tab = await chrome.tabs.create({ url: activity.url, active: false });
    tabId = tab.id;
    ecosystemState.currentTabId = tabId;

    // Abort check function — fast path for directive changes or external aborts
    const checkAbort = () => {
      if (ecosystemDirective !== "active") return true;
      if (!ecosystemState.running) return true;
      return false;
    };

    // Site-specific executor handles the real interaction
    if (typeof activity.run === "function") {
      await activity.run(tabId, duration, checkAbort, account.email);
    } else {
      // Fallback: just wait (for activities without executor)
      await abortableSleep(duration, checkAbort);
    }

    // Activity completed — track count
    ecosystemState.todayCounts[account.email] =
      (ecosystemState.todayCounts[account.email] || 0) + 1;
    const elapsedSec = Math.round((Date.now() - startedAtWall) / 1000);
    ecosystemReportActivity(
      account.email, activity.name, "end", elapsedSec
    );
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (msg === "aborted" || msg.startsWith("directive_")) {
      console.log(`[Ecosystem] Activity stopped: ${msg}`);
    } else {
      console.warn(`[Ecosystem] Activity error: ${msg}`);
    }
    ecosystemReportActivity(account.email, activity.name, "error", 0);
  } finally {
    if (tabId) {
      try { await chrome.tabs.remove(tabId); } catch {}
    }
    ecosystemState.running = false;
    ecosystemState.currentTabId = null;
    ecosystemState.currentAccount = "";
    ecosystemState.currentSite = "";
    // Schedule next activity
    ecosystemState.nextActivityAt = Date.now() + ecosystemNextGap();
  }
}

// Ecosystem scheduler tick — fires every 30s
async function ecosystemTick() {
  if (ecosystemDirective !== "active") return;
  if (ecosystemState.running) return;
  if (Date.now() < ecosystemState.nextActivityAt) return;

  // Schedule first run 1-5 min after becoming active
  if (ecosystemState.nextActivityAt === 0) {
    ecosystemState.nextActivityAt = Date.now() + (60 + Math.random() * 240) * 1000;
    return;
  }

  await ecosystemRunActivity();
}

// ═══════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════

// Start polling bridge — 500ms interval, but _workInProgress lock
// prevents concurrent handleWork calls. This ensures fast pickup
// of queued work (important when 6+ jobs are pending).
setInterval(pollBridge, 500);

// Ecosystem scheduler — checks every 30s whether to fire an activity.
// Heavy work is gated by directive + nextActivityAt, so idle cost is minimal.
setInterval(ecosystemTick, 30000);

// Detect accounts periodically
setInterval(detectAccounts, ACCOUNT_DETECT_INTERVAL);

// Initial detection after 2s (give Chrome time to load tabs)
setTimeout(detectAccounts, 2000);

// Listen for Labs tab changes
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status === "complete" && tab.url && tab.url.startsWith(LABS_ORIGIN)) {
    invalidateRecaptchaCache(tabId);  // force fresh check after reload
    setTimeout(() => detectAccounts(), 3000);
  }
});

// Clean up when tab closes
chrome.tabs.onRemoved.addListener((tabId) => {
  delete connectedAccounts[tabId];
  invalidateRecaptchaCache(tabId);
});

// Messages from popup
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === "getStatus") {
    const accs = Object.values(connectedAccounts).filter((a) => a.logged_in);
    sendResponse({
      connected: bridgeConnected,
      accounts: accs.map((a) => ({ email: a.email, name: a.name })),
      tokenCount,
      lastError: lastPollError,
      ecosystem: {
        directive: ecosystemDirective,
        running: ecosystemState.running,
        currentAccount: ecosystemState.currentAccount,
        currentSite: ecosystemState.currentSite,
        heldAccounts: ecosystemHeldAccounts,
        todayCounts: ecosystemState.todayCounts,
      },
    });
    return false;
  }

  if (msg.type === "ecosystemToggle") {
    // User clicked toggle in popup. Tell bridge; bridge broadcasts back
    // via /poll so all extensions (in multi-profile setups) stay in sync.
    const enabled = !!msg.enabled;
    fetch(`${BRIDGE_URL}/ecosystem`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled }),
    })
      .then((r) => r.json())
      .then((data) => sendResponse({ ok: true, ...data }))
      .catch((e) => sendResponse({ ok: false, error: e.message }));
    return true; // async
  }

  if (msg.type === "ecosystemReleaseAccount") {
    fetch(`${BRIDGE_URL}/ecosystem`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ release_account: msg.account }),
    })
      .then((r) => r.json())
      .then((data) => sendResponse({ ok: true, ...data }))
      .catch((e) => sendResponse({ ok: false, error: e.message }));
    return true;
  }

  if (msg.type === "detectAccounts") {
    detectAccounts().then((accounts) => sendResponse({ accounts }));
    return true; // async response
  }

  if (msg.type === "openLabsTab") {
    chrome.tabs.create({ url: `${LABS_ORIGIN}/fx/tools/flow` });
    sendResponse({ ok: true });
    return false;
  }
});

console.log("[G-Labs Helper] Extension started. Bridge:", BRIDGE_URL);
