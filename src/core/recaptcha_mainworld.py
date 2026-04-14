"""
Main-World reCAPTCHA Token Extraction

Instead of page.evaluate() which runs via CDP's Runtime.evaluate (detectable),
this injects a <script> tag into the DOM. The script runs through the browser's
native script parser — exactly like the page's own JavaScript. Zero CDP traces.

This matches the competitor's Chrome Extension approach:
  chrome.scripting.executeScript({ world: "MAIN" })

Flow:
  1. Store params on window (via evaluate — fine, not reCAPTCHA code)
  2. Inject <script> tag with reCAPTCHA code → runs in TRUE main world
  3. Poll window for result
"""

import uuid


# JavaScript that runs INSIDE the <script> tag (true main world).
# __UID__ is replaced at runtime with a unique key.
_RECAPTCHA_MAINWORLD_JS = """
(async () => {
    const uid = '__UID__';
    const cfg = window[uid + '_cfg'];
    delete window[uid + '_cfg'];
    if (!cfg) { window[uid] = { error: 'no_cfg' }; return; }
    const action = cfg.action;
    try {
        const enterprise = window.grecaptcha && window.grecaptcha.enterprise;
        if (!enterprise || typeof enterprise.execute !== 'function') {
            window[uid] = { error: 'no_enterprise' };
            return;
        }

        let siteKey = null;

        // ─── Primary: ___grecaptcha_cfg.clients (competitor method) ───
        // More reliable than script-tag parsing. The internal reCAPTCHA
        // config object stores the render key (site key) in nested props.
        try {
            const clients = window.___grecaptcha_cfg && window.___grecaptcha_cfg.clients;
            if (clients) {
                for (const id of Object.keys(clients)) {
                    const client = clients[id];
                    if (!client || typeof client !== 'object') continue;
                    const walk = (obj, depth) => {
                        if (depth > 5 || !obj || typeof obj !== 'object') return null;
                        for (const key of Object.keys(obj)) {
                            const val = obj[key];
                            if (typeof val === 'string' && val.length >= 20 && val.length <= 50
                                && /^[A-Za-z0-9_-]+$/.test(val)) {
                                // Verify against render= in script tags
                                const check = document.querySelector('script[src*="render=' + val + '"]');
                                if (check) return val;
                            }
                            if (typeof val === 'object' && val !== null) {
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
        } catch (ex) {}

        // ─── Fallback: script tag parsing ───
        if (!siteKey) {
            var scripts = document.querySelectorAll('script[src*="recaptcha"][src*="render="]');
            for (var i = 0; i < scripts.length; i++) {
                try {
                    var render = new URL(scripts[i].src).searchParams.get('render');
                    if (render && render !== 'explicit') { siteKey = render; break; }
                } catch (ex) {}
            }
        }

        if (!siteKey) {
            window[uid] = { error: 'no_sitekey' };
            return;
        }

        if (typeof enterprise.ready === 'function') {
            await new Promise(function(r) { enterprise.ready(r); });
        }

        // Execute immediately — fresh token, zero delays (like competitor)
        var token = await enterprise.execute(siteKey, { action: action });
        window[uid] = token ? { token: token } : { error: 'no_token' };
    } catch (e) {
        window[uid] = { error: (e && e.message) || 'execute_failed' };
    }
})();
"""


# Polling JS — reads the result from window and cleans up.
_POLL_JS = """(uid) => {
    return new Promise((resolve) => {
        let n = 0;
        const poll = setInterval(() => {
            n++;
            const r = window[uid];
            if (r) {
                delete window[uid];
                clearInterval(poll);
                resolve(r);
            } else if (n > 500) {
                clearInterval(poll);
                resolve({ error: 'timeout' });
            }
        }, 20);
    });
}"""


async def get_recaptcha_token_mainworld(page, action, log_fn=None):
    """
    Get reCAPTCHA Enterprise token via <script> tag injection.

    The reCAPTCHA execute() call runs in the page's TRUE main world —
    no CDP Runtime.evaluate traces. This is undetectable by reCAPTCHA
    Enterprise's client-side checks.

    Args:
        page: Playwright page object
        action: reCAPTCHA action string (e.g. "IMAGE_GENERATION")
        log_fn: Optional logging function

    Returns:
        Token string or None on failure
    """
    uid = f"__glrc_{uuid.uuid4().hex[:12]}"

    try:
        # Step 1: Store params on window (via evaluate — this is fine,
        # it's just setting a variable, not calling reCAPTCHA)
        await page.evaluate(
            "(p) => { window[p.uid + '_cfg'] = { action: p.action }; }",
            {"uid": uid, "action": action},
        )

        # Step 2: Inject <script> tag — code runs in TRUE main world
        js_code = _RECAPTCHA_MAINWORLD_JS.replace("__UID__", uid)
        try:
            await page.add_script_tag(content=js_code)
        except Exception:
            # Fallback: manually create script element
            await page.evaluate(
                """(code) => {
                    const s = document.createElement('script');
                    s.textContent = code;
                    document.head.appendChild(s);
                    s.remove();
                }""",
                js_code,
            )

        # Step 3: Poll for result from main world
        result = await page.evaluate(_POLL_JS, uid)

        token = result.get("token") if isinstance(result, dict) else None
        if token:
            if log_fn:
                log_fn(f"[MainWorld] reCAPTCHA token acquired ({len(token)} chars)")
            return token
        else:
            err = result.get("error", "unknown") if isinstance(result, dict) else "null"
            if log_fn:
                log_fn(f"[MainWorld] reCAPTCHA failed: {err}")
            return None

    except Exception as e:
        if log_fn:
            log_fn(f"[MainWorld] reCAPTCHA exception: {str(e)[:100]}")
        return None
