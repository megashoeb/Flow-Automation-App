/**
 * G-Labs Studio Helper — Genspark Module
 *
 * Standalone module loaded via importScripts() in background.js.
 * Talks to GensparkBridge at http://127.0.0.1:18925 — completely separate
 * from the Flow bridge (18924). Flow and Genspark can run side-by-side
 * without interfering with each other.
 *
 * Responsibilities:
 *   1. Detect logged-in Genspark accounts (via /api/user/me)
 *   2. Periodically report accounts to the Genspark bridge
 *   3. Poll the bridge for pending image-generation work
 *   4. Execute each work item:
 *        a. Fetch a fresh reCAPTCHA Enterprise token
 *        b. POST /api/agent/ask_proxy with prompt + token
 *        c. Parse SSE stream to extract task_id
 *        d. Poll /api/spark/image_generation_task_detail until COMPLETED
 *        e. Fetch the image bytes from the returned URL
 *        f. Send result (base64 image + metadata) back to the bridge
 *
 * Runs only when the bridge is reachable on port 18925. Silent otherwise.
 */

const GENSPARK_BRIDGE_URL = "http://127.0.0.1:18925";
const GENSPARK_POLL_INTERVAL = 1500;            // ms between poll calls
const GENSPARK_ACCOUNT_DETECT_INTERVAL = 15000; // ms between account detection
const GENSPARK_ORIGIN = "https://www.genspark.ai";

// ─── State ───
let gensparkBridgeConnected = false;
let gensparkWorkInProgress = false;
let gensparkAccounts = {};  // email -> { email, plan_type, tab_id, ... }
let gensparkLastPollError = "";

// ═══════════════════════════════════════════════════════════════════
// Account detection
// ═══════════════════════════════════════════════════════════════════

async function gensparkDetectAccounts() {
  try {
    const tabs = await chrome.tabs.query({ url: `${GENSPARK_ORIGIN}/*` });
    if (!tabs.length) {
      // No Genspark tabs → clear local registry
      gensparkAccounts = {};
      return;
    }

    const fresh = {};
    for (const tab of tabs) {
      try {
        const result = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          world: "MAIN",
          func: async () => {
            try {
              const r = await fetch("/api/user/me", {
                method: "GET",
                credentials: "include",
                headers: { "Accept": "application/json" },
              });
              if (!r.ok) return { logged_in: false, status: r.status };
              const data = await r.json();
              // Genspark wraps responses in {status, message, data}
              const user = data?.data || data;
              const email = user?.email || "";
              if (!email) return { logged_in: false, reason: "no_email" };
              // Extract plan type — field names can vary; check common ones
              const plan =
                user?.subscription?.plan ||
                user?.subscription_plan ||
                user?.plan_type ||
                user?.plan ||
                "free";
              return {
                logged_in: true,
                email,
                plan_type: String(plan).toLowerCase(),
                user_id: user?.id || user?.user_id || "",
                display_name: user?.display_name || user?.name || "",
              };
            } catch (e) {
              return { logged_in: false, error: e.message };
            }
          },
        });
        const info = result?.[0]?.result;
        if (info && info.logged_in && info.email) {
          fresh[info.email] = {
            email: info.email,
            plan_type: info.plan_type,
            tab_id: tab.id,
            user_id: info.user_id,
            display_name: info.display_name,
          };
        }
      } catch (e) {
        // Tab might be closed or protected — skip silently
      }
    }
    gensparkAccounts = fresh;

    // Report to bridge
    if (Object.keys(fresh).length) {
      try {
        await fetch(`${GENSPARK_BRIDGE_URL}/genspark/accounts`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ accounts: Object.values(fresh) }),
        });
      } catch {}
    }
  } catch (e) {
    // Bridge offline or no genspark tabs — silent
  }
}

// ═══════════════════════════════════════════════════════════════════
// Bridge polling
// ═══════════════════════════════════════════════════════════════════

async function gensparkPollBridge() {
  try {
    const emails = Object.keys(gensparkAccounts);
    if (!emails.length) {
      // Silently skip — avoids 400s on the bridge when no accounts yet
      return;
    }
    const accountsParam = `?accounts=${encodeURIComponent(emails.join(","))}`;

    const resp = await fetch(`${GENSPARK_BRIDGE_URL}/genspark/poll${accountsParam}`, {
      method: "GET",
      headers: { "Accept": "application/json" },
    });
    if (!resp.ok) {
      gensparkBridgeConnected = false;
      gensparkLastPollError = `HTTP ${resp.status}`;
      return;
    }
    gensparkBridgeConnected = true;
    gensparkLastPollError = "";
    const data = await resp.json();

    if (data.work && !gensparkWorkInProgress) {
      gensparkWorkInProgress = true;
      try {
        await gensparkHandleWork(data.work);
      } catch (e) {
        console.warn("[Genspark] handleWork threw:", e.message);
      } finally {
        gensparkWorkInProgress = false;
      }
    }
  } catch (e) {
    gensparkBridgeConnected = false;
    gensparkLastPollError = e.message || "fetch failed";
  }
}

// ═══════════════════════════════════════════════════════════════════
// Work execution — image generation end-to-end
// ═══════════════════════════════════════════════════════════════════

async function gensparkHandleWork(work) {
  const { request_id, account, prompt, model_params, recaptcha_site_key } = work;
  const info = gensparkAccounts[account];
  if (!info) {
    await gensparkSubmitResult(request_id, { error: "account_tab_not_found" });
    return;
  }
  const tabId = info.tab_id;

  // Verify tab is still open
  try {
    await chrome.tabs.get(tabId);
  } catch {
    await gensparkSubmitResult(request_id, { error: "tab_closed" });
    return;
  }

  // Step 1: Get a fresh reCAPTCHA Enterprise token (in MAIN world)
  let recaptchaToken = "";
  try {
    const tokenResult = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: async (siteKey) => {
        // grecaptcha.enterprise is injected by Genspark's page script
        let tries = 0;
        while (tries < 30) {
          if (window.grecaptcha && window.grecaptcha.enterprise &&
              typeof window.grecaptcha.enterprise.execute === "function") {
            break;
          }
          await new Promise((r) => setTimeout(r, 300));
          tries++;
        }
        if (!window.grecaptcha?.enterprise?.execute) {
          return { error: "no_recaptcha_enterprise" };
        }
        try {
          const tok = await window.grecaptcha.enterprise.execute(siteKey, {
            action: "agent_ask",  // observed action in HAR
          });
          return { token: tok };
        } catch (e) {
          return { error: "recaptcha_failed: " + (e?.message || e) };
        }
      },
      args: [recaptcha_site_key],
    });
    const r = tokenResult?.[0]?.result;
    if (r?.error) {
      await gensparkSubmitResult(request_id, { error: r.error });
      return;
    }
    recaptchaToken = r?.token || "";
  } catch (e) {
    await gensparkSubmitResult(request_id, { error: "recaptcha_injection_failed: " + e.message });
    return;
  }
  if (!recaptchaToken) {
    await gensparkSubmitResult(request_id, { error: "empty_recaptcha_token" });
    return;
  }

  // Step 2: POST /api/agent/ask_proxy with prompt + token — SSE response.
  // We do this INSIDE the tab so cookies + same-origin rules apply correctly.
  let taskInfo;
  try {
    const askResult = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: async (reqBody) => {
        try {
          const r = await fetch("/api/agent/ask_proxy", {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(reqBody),
          });
          if (!r.ok) {
            const text = await r.text().catch(() => "");
            return { error: `ask_proxy_http_${r.status}`, body: text.slice(0, 500) };
          }
          // Parse SSE stream to find task_id
          const reader = r.body.getReader();
          const decoder = new TextDecoder();
          let buffer = "";
          let taskId = null;
          let projectId = null;
          let msgId = null;
          const deadline = Date.now() + 120000;  // 2 min hard cap

          while (Date.now() < deadline) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop() || "";  // keep incomplete last line
            for (const line of lines) {
              if (!line.startsWith("data:")) continue;
              const payload = line.slice(5).trim();
              if (!payload) continue;
              try {
                const obj = JSON.parse(payload);
                // task_id appears on a message_field event for field_name "tasks"
                // or in a field_value that's an array of tasks. Different patterns
                // — scan liberally.
                if (!projectId && obj.project_id) projectId = obj.project_id;
                if (!msgId && obj.message_id) msgId = obj.message_id;
                // Pattern 1: tasks array in field_value
                const fv = obj.field_value;
                if (!taskId && fv && typeof fv === "object") {
                  if (Array.isArray(fv)) {
                    for (const item of fv) {
                      if (item && item.id && item.task_type) {
                        taskId = item.id;
                        break;
                      }
                    }
                  } else if (fv.task_ids && Array.isArray(fv.task_ids)) {
                    taskId = fv.task_ids[0];
                  } else if (fv.id && fv.task_type) {
                    taskId = fv.id;
                  }
                }
                // Pattern 2: delta contains JSON fragment — ignore, we'll find
                // the task on a later event
                // Pattern 3: stop when we get completion event and task_id
                if (taskId) break;
              } catch (_e) {
                // Not JSON — skip
              }
            }
            if (taskId) break;
          }
          try { await reader.cancel(); } catch {}
          if (!taskId) {
            return { error: "sse_no_task_id_found" };
          }
          return { task_id: taskId, project_id: projectId, msg_id: msgId };
        } catch (e) {
          return { error: "ask_proxy_exception: " + (e?.message || e) };
        }
      },
      args: [{
        model_params: model_params,
        writingContent: null,
        type: "image_generation_agent",
        project_id: null,
        messages: [{
          role: "user",
          id: _gensparkUuid(),
          content: prompt,
        }],
        user_s_input: prompt,
        g_recaptcha_token: recaptchaToken,
      }],
    });
    taskInfo = askResult?.[0]?.result;
    if (!taskInfo || taskInfo.error) {
      await gensparkSubmitResult(request_id, {
        error: taskInfo?.error || "ask_proxy_no_result",
      });
      return;
    }
  } catch (e) {
    await gensparkSubmitResult(request_id, {
      error: "ask_proxy_injection_failed: " + e.message,
    });
    return;
  }

  const { task_id, project_id } = taskInfo;

  // Step 3: Poll /api/spark/image_generation_task_detail until COMPLETED
  // (using JSON endpoint for simplicity over SSE)
  let finalTask = null;
  try {
    const pollResult = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: async (taskId) => {
        const deadline = Date.now() + 180000;  // 3 min cap for generation
        let lastStatus = "";
        while (Date.now() < deadline) {
          try {
            const r = await fetch(
              `/api/spark/image_generation_task_detail?task_id=${encodeURIComponent(taskId)}`,
              { method: "GET", credentials: "include" }
            );
            if (!r.ok) {
              await new Promise((res) => setTimeout(res, 2500));
              continue;
            }
            const data = await r.json();
            const t = data?.data || {};
            lastStatus = t.status || "";
            if (lastStatus === "COMPLETED" || lastStatus === "SUCCESS") {
              return { ok: true, task: t };
            }
            if (lastStatus === "FAILED" || lastStatus === "ERROR") {
              return { ok: false, reason: `task_status_${lastStatus}`, task: t };
            }
            await new Promise((res) => setTimeout(res, 2500));
          } catch (e) {
            await new Promise((res) => setTimeout(res, 2500));
          }
        }
        return { ok: false, reason: "poll_timeout", last_status: lastStatus };
      },
      args: [task_id],
    });
    const r = pollResult?.[0]?.result;
    if (!r || !r.ok) {
      await gensparkSubmitResult(request_id, {
        error: r?.reason || "poll_failed",
      });
      return;
    }
    finalTask = r.task;
  } catch (e) {
    await gensparkSubmitResult(request_id, {
      error: "poll_injection_failed: " + e.message,
    });
    return;
  }

  // Step 4: Fetch the actual image bytes (prefer watermark-free URL)
  const urls = finalTask?.image_urls_nowatermark || finalTask?.image_urls || [];
  const imageUrl = Array.isArray(urls) ? urls[0] : urls;
  if (!imageUrl) {
    await gensparkSubmitResult(request_id, {
      error: "no_image_url_in_completed_task",
      task_id, project_id,
    });
    return;
  }

  let imageBytesB64 = "";
  try {
    const dlResult = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: async (url) => {
        try {
          const r = await fetch(url, { method: "GET", credentials: "include" });
          if (!r.ok) return { error: `image_fetch_http_${r.status}` };
          const buf = await r.arrayBuffer();
          // Convert to base64 in chunks (large images blow the stack if done
          // naively via apply)
          const bytes = new Uint8Array(buf);
          let bin = "";
          const chunk = 0x8000;
          for (let i = 0; i < bytes.length; i += chunk) {
            bin += String.fromCharCode.apply(
              null, bytes.subarray(i, i + chunk)
            );
          }
          return { b64: btoa(bin) };
        } catch (e) {
          return { error: "image_fetch_exception: " + (e?.message || e) };
        }
      },
      args: [imageUrl],
    });
    const r = dlResult?.[0]?.result;
    if (r?.error) {
      await gensparkSubmitResult(request_id, { error: r.error, image_url: imageUrl });
      return;
    }
    imageBytesB64 = r?.b64 || "";
  } catch (e) {
    await gensparkSubmitResult(request_id, {
      error: "download_injection_failed: " + e.message,
      image_url: imageUrl,
    });
    return;
  }

  // Step 5: Submit result to bridge
  await gensparkSubmitResult(request_id, {
    image_url: imageUrl,
    image_bytes_b64: imageBytesB64,
    image_urls_nowatermark: finalTask?.image_urls_nowatermark || [],
    model_used: finalTask?.model || model_params?.model || "",
    task_id: task_id,
    project_id: project_id || finalTask?.project_id || "",
    error: null,
  });
}

async function gensparkSubmitResult(requestId, body) {
  try {
    await fetch(`${GENSPARK_BRIDGE_URL}/genspark/work-result`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request_id: requestId, ...body }),
    });
  } catch (e) {
    console.warn("[Genspark] submit result failed:", e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════

function _gensparkUuid() {
  // Simple v4-ish UUID — doesn't need to be crypto-strong, it's a message id
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// ═══════════════════════════════════════════════════════════════════
// Lifecycle — started from background.js
// ═══════════════════════════════════════════════════════════════════

// Intervals are kicked off in background.js so the service worker can
// manage them alongside the Flow bridge's intervals.
function gensparkStart() {
  setInterval(gensparkPollBridge, GENSPARK_POLL_INTERVAL);
  setInterval(gensparkDetectAccounts, GENSPARK_ACCOUNT_DETECT_INTERVAL);
  // Initial account detection after 2s (tabs time to load)
  setTimeout(gensparkDetectAccounts, 2000);
  console.log("[Genspark] Module started — bridge:", GENSPARK_BRIDGE_URL);
}

// Expose to the service worker global scope
self.gensparkStart = gensparkStart;
self.gensparkGetStatus = function () {
  return {
    connected: gensparkBridgeConnected,
    accounts: Object.values(gensparkAccounts).map((a) => ({
      email: a.email,
      plan: a.plan_type,
    })),
    lastError: gensparkLastPollError,
  };
};
