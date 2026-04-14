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

const BRIDGE_URL = "http://127.0.0.1:18924";
const POLL_INTERVAL = 1500;
const LABS_ORIGIN = "https://labs.google";
const ACCOUNT_DETECT_INTERVAL = 10000;

// ─── State ───
let bridgeConnected = false;
let connectedAccounts = {};  // tabId → { email, name, access_token, project_id }
let tokenCount = 0;
let lastPollError = "";

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

    // Handle pending work (token request)
    if (data.work) {
      await handleWork(data.work);
    }

    // Handle commands (cookie clear, tab reload, etc.)
    if (data.command) {
      await handleCommand(data.command);
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

  // Find the Labs tab for this account
  const tabId = await findLabsTab(account);
  if (!tabId) {
    await submitResult(request_id, {
      error: `no_labs_tab_for_${account || "any"}`,
    });
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

    // ─── 2. Project ID (from URL) ───
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

async function findLabsTab(targetAccount) {
  const tabs = await chrome.tabs.query({ url: `${LABS_ORIGIN}/*` });

  if (!tabs.length) return null;

  // If specific account requested, find matching tab
  if (targetAccount) {
    // Check cache first
    for (const tab of tabs) {
      const cached = connectedAccounts[tab.id];
      if (cached && cached.email === targetAccount) {
        return tab.id;
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
          return tab.id;
        }
      } catch {}
    }

    return null;
  }

  // No specific account — return first Labs tab
  return tabs[0].id;
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
// Lifecycle
// ═══════════════════════════════════════════════════════════════════

// Start polling bridge
setInterval(pollBridge, POLL_INTERVAL);

// Detect accounts periodically
setInterval(detectAccounts, ACCOUNT_DETECT_INTERVAL);

// Initial detection after 2s (give Chrome time to load tabs)
setTimeout(detectAccounts, 2000);

// Listen for Labs tab changes
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status === "complete" && tab.url && tab.url.startsWith(LABS_ORIGIN)) {
    setTimeout(() => detectAccounts(), 3000);
  }
});

// Clean up when tab closes
chrome.tabs.onRemoved.addListener((tabId) => {
  delete connectedAccounts[tabId];
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
    });
    return false;
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
