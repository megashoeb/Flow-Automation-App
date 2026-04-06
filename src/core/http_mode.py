"""
HTTP Mode: Shared Browser Fetch — reCAPTCHA + API in ONE browser context

Architecture:
  1 CloakBrowser per account  -> page.evaluate(fetch()) for ALL API calls
  N BrowserFetchWorkers       -> queue fetch() calls to the shared browser
  reCAPTCHA + fetch in SAME evaluate() call = same browser context = valid

RAM usage:
  1 browser per account = ~300 MB (fixed)
  Each worker = ~0 MB (just a Python coroutine)
  20 workers on 1 account = ~300 MB total vs ~6 GB with browser-per-slot
"""

import asyncio
import json
import os
import re
import time
import uuid
import random
import platform
from typing import Optional

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    import aiofiles
except ImportError:
    aiofiles = None

from src.core.app_paths import get_sessions_dir
from src.core.process_tracker import cleanup_session_locks
from src.core.cloakbrowser_support import load_cloakbrowser_api
from src.db.db_manager import (
    get_accounts,
    get_all_jobs,
    get_setting,
    get_int_setting,
    get_bool_setting,
    update_job_status,
)

DATA_DIR = str(get_sessions_dir())


# ═══════════════════════════════════════════════════════════════════════════
# Model / aspect ratio resolvers — maps display names to API enum values
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_image_model(model):
    lower = str(model or "").lower()
    if "nano banana pro" in lower:
        return "GEM_PIX_2"
    if "nano banana" in lower:
        return "NARWHAL"
    if "imagen 4" in lower:
        return "IMAGEN_4"
    if "imagen" in lower:
        return "NARWHAL"
    if model and model == model.upper():
        return model
    return "NARWHAL"


def _resolve_image_ratio(ratio):
    raw = str(ratio or "").strip()
    if raw.startswith("IMAGE_ASPECT_RATIO_"):
        return raw
    lower = raw.lower()
    if "4:3" in lower:
        return "IMAGE_ASPECT_RATIO_LANDSCAPE_FOUR_THREE"
    if "3:4" in lower:
        return "IMAGE_ASPECT_RATIO_PORTRAIT_THREE_FOUR"
    if "portrait" in lower or "9:16" in lower:
        return "IMAGE_ASPECT_RATIO_PORTRAIT"
    if "square" in lower or "1:1" in lower:
        return "IMAGE_ASPECT_RATIO_SQUARE"
    return "IMAGE_ASPECT_RATIO_LANDSCAPE"


def _resolve_video_model(model, video_model=""):
    source = str(video_model or model or "").strip().lower()
    if not source:
        return "veo_3_1_t2v_fast_ultra"
    if "lite" in source:
        return "veo_3_1_t2v_lite"
    if "lower pri" in source or "relaxed" in source:
        return "veo_3_1_t2v_fast_ultra_relaxed"
    if "quality" in source:
        return "veo_3_1_t2v"
    if "_" in source:
        return source
    return "veo_3_1_t2v_fast_ultra"


def _resolve_video_ratio(ratio):
    raw = str(ratio or "").strip()
    if raw.startswith("VIDEO_ASPECT_RATIO_"):
        return raw
    if raw.startswith("IMAGE_ASPECT_RATIO_"):
        return raw.replace("IMAGE_", "VIDEO_", 1)
    lower = raw.lower()
    if "portrait" in lower or "9:16" in lower:
        return "VIDEO_ASPECT_RATIO_PORTRAIT"
    if "square" in lower or "1:1" in lower:
        return "VIDEO_ASPECT_RATIO_SQUARE"
    return "VIDEO_ASPECT_RATIO_LANDSCAPE"


# ═══════════════════════════════════════════════════════════════════════════
# SharedBrowser — 1 browser per account, shared by all workers
# ═══════════════════════════════════════════════════════════════════════════

class SharedBrowser:
    """
    Single shared browser per account. Workers call page.evaluate(fetch())
    to make API requests — reCAPTCHA + fetch in the SAME browser context.
    No separate HTTP clients needed.
    """

    def __init__(self, account_name, session_path, cookies_json_path, log_fn, project_id=None):
        self.account_name = account_name
        self._session_path = session_path
        self._cookies_json = cookies_json_path
        self._log = log_fn
        self._context = None
        self._page = None
        self._project_id = project_id
        self._jobs_since_reload = 0
        self._last_page_reload = 0
        self._bearer_token = None
        self._bearer_token_time = 0

    async def start(self, playwright_instance):
        """Start shared browser and navigate to Flow page."""
        self._log(f"[SharedBrowser:{self.account_name}] Starting...")

        try:
            success = await self._launch_browser(playwright_instance)
            if not success:
                return False

            pages = list(getattr(self._context, "pages", []) or [])
            self._page = pages[0] if pages else await self._context.new_page()

            # Intercept requests to capture Bearer token
            self._setup_request_interception()

            await self._page.goto(
                "https://labs.google/fx/tools/flow",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(3)

            # Extract project ID
            if self._project_id:
                self._log(f"[SharedBrowser:{self.account_name}] Using cached project ID: {self._project_id}")
            else:
                self._project_id = await self._extract_project_id()

            if not self._project_id:
                self._log(f"[SharedBrowser:{self.account_name}] No project ID found. Check login.")
                return False

            # Verify reCAPTCHA is loaded
            recap_ok = await self._check_recaptcha()
            if not recap_ok:
                self._log(f"[SharedBrowser:{self.account_name}] reCAPTCHA not loaded on page.")

            # Capture Bearer token by triggering an API call
            if not self._bearer_token:
                await self._trigger_bearer_capture()

            self._log(
                f"[SharedBrowser:{self.account_name}] Ready! "
                f"Project: {self._project_id}, reCAPTCHA: {'yes' if recap_ok else 'NO'}, "
                f"Bearer: {'yes' if self._bearer_token else 'NO'}"
            )
            return True

        except Exception as e:
            self._log(f"[SharedBrowser:{self.account_name}] Start failed: {str(e)[:80]}")
            return False

    async def _launch_browser(self, playwright_instance):
        """Launch CloakBrowser (preferred) or Playwright persistent context."""
        # Debug: log cookie file status
        self._log(f"[SharedBrowser:{self.account_name}] Session path: {self._session_path}")
        self._log(
            f"[SharedBrowser:{self.account_name}] Cookies JSON: {self._cookies_json} "
            f"(exists: {os.path.exists(self._cookies_json)})"
        )

        cookies = self._load_exported_cookies()
        self._log(f"[SharedBrowser:{self.account_name}] Loaded {len(cookies)} cookies from JSON file.")

        # Try CloakBrowser
        try:
            cloak_api = load_cloakbrowser_api()
            cloak_persistent = cloak_api.get("persistent_async")

            if cloak_api.get("available") and cloak_persistent:
                seed = random.randint(10000, 99999)
                if platform.system() == "Darwin":
                    # Mac: fresh profile + imported cookies (avoid lock conflicts)
                    profile = self._session_path + "_shared_browser"
                    os.makedirs(profile, exist_ok=True)
                    self._clean_locks(profile)
                else:
                    # Windows: use account's session directly (persistent cookies)
                    profile = self._session_path
                    self._clean_locks(profile)

                self._log(f"[SharedBrowser:{self.account_name}] Profile path: {profile}")

                self._context = await cloak_persistent(
                    profile, headless=True,
                    args=[f"--fingerprint={seed}"], humanize=True,
                )

                # Import cookies — CRITICAL for login session
                cookies_imported = await self._import_cookies(cookies)

                # Check actual cookie count in browser
                try:
                    actual = await self._context.cookies()
                    actual_count = len(actual)
                except Exception:
                    actual_count = cookies_imported

                self._log(
                    f"[SharedBrowser:{self.account_name}] CloakBrowser started. "
                    f"Imported: {cookies_imported}, Browser has: {actual_count} cookies."
                )

                if actual_count == 0:
                    self._log(
                        f"[SharedBrowser:{self.account_name}] "
                        "NO cookies in browser! Login session missing. "
                        f"Check: {self._cookies_json}"
                    )

                return True
        except Exception as e:
            self._log(f"[SharedBrowser:{self.account_name}] CloakBrowser not available: {str(e)[:40]}")

        # Fallback: Playwright
        try:
            self._clean_locks(self._session_path)
            self._log(f"[SharedBrowser:{self.account_name}] Profile path: {self._session_path}")

            self._context = await playwright_instance.chromium.launch_persistent_context(
                self._session_path, headless=True,
                args=["--disable-gpu", "--no-sandbox", "--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
            )

            cookies_imported = await self._import_cookies(cookies)

            try:
                actual = await self._context.cookies()
                actual_count = len(actual)
            except Exception:
                actual_count = cookies_imported

            self._log(
                f"[SharedBrowser:{self.account_name}] Playwright started. "
                f"Imported: {cookies_imported}, Browser has: {actual_count} cookies."
            )
            return True
        except Exception as e:
            self._log(f"[SharedBrowser:{self.account_name}] Browser launch failed: {str(e)[:60]}")
            return False

    def _setup_request_interception(self):
        """Intercept browser requests to capture Authorization Bearer token."""
        def _on_request(request):
            if "aisandbox-pa.googleapis.com" in request.url and request.method == "POST":
                auth = request.headers.get("authorization", "")
                if auth.startswith("Bearer "):
                    self._bearer_token = auth
                    self._bearer_token_time = time.time()
                    self._log(f"[SharedBrowser:{self.account_name}] Bearer token captured (len={len(auth)})")
        self._page.on("request", _on_request)

    async def _trigger_bearer_capture(self):
        """Trigger a fetch() from the browser to capture the Bearer token."""
        self._log(f"[SharedBrowser:{self.account_name}] Triggering API request to capture Bearer token...")
        try:
            status = await self._page.evaluate("""
                async () => {
                    try {
                        const r = await fetch(
                            'https://aisandbox-pa.googleapis.com/v1/credits',
                            { method: 'GET', credentials: 'include' }
                        );
                        return r.status;
                    } catch(e) { return e.message; }
                }
            """)
            self._log(f"[SharedBrowser:{self.account_name}] Credits API: {status}")
            await asyncio.sleep(2)
        except Exception as e:
            self._log(f"[SharedBrowser:{self.account_name}] Credits call error: {str(e)[:50]}")

        if self._bearer_token:
            self._log(f"[SharedBrowser:{self.account_name}] Bearer token ready!")
            return

        # Fallback: dummy generation POST
        try:
            recap = await self._page.evaluate("""
                async () => {
                    try {
                        const scripts = document.querySelectorAll("script[src*='recaptcha'][src*='render=']");
                        let sk = null;
                        for (const s of scripts) {
                            const m = s.src.match(/render=([^&]+)/);
                            if (m && m[1] !== 'explicit') { sk = m[1]; break; }
                        }
                        if (!sk || !grecaptcha || !grecaptcha.enterprise) return null;
                        return await grecaptcha.enterprise.execute(sk, {action: 'generate'});
                    } catch(e) { return null; }
                }
            """)
            if recap and self._project_id:
                await self._page.evaluate(
                    """async ([pid, tok]) => {
                        try {
                            await fetch(
                                `https://aisandbox-pa.googleapis.com/v1/projects/${pid}/flowMedia:batchGenerateImages`,
                                {method:'POST', credentials:'include',
                                 headers:{'Content-Type':'text/plain;charset=UTF-8'},
                                 body:JSON.stringify({clientContext:{recaptchaContext:{token:tok,applicationType:'RECAPTCHA_APPLICATION_TYPE_WEB'},projectId:pid,tool:'PINHOLE',sessionId:';'+Date.now()},mediaGenerationContext:{batchId:crypto.randomUUID()},useNewMedia:true,requests:[{imageModelName:'NARWHAL',imageAspectRatio:'IMAGE_ASPECT_RATIO_SQUARE',structuredPrompt:{parts:[{text:'bearer capture test'}]},seed:12345,imageInputs:[]}]})}
                            );
                        } catch(e) {}
                    }""",
                    [self._project_id, recap],
                )
                await asyncio.sleep(2)
        except Exception:
            pass

        if self._bearer_token:
            self._log(f"[SharedBrowser:{self.account_name}] Bearer token ready!")
        else:
            self._log(f"[SharedBrowser:{self.account_name}] Bearer token NOT captured.")

    async def get_bearer_token(self):
        """Get Bearer token. Refresh if expired (>30 min)."""
        if self._bearer_token and (time.time() - self._bearer_token_time) < 1800:
            return self._bearer_token
        self._log(f"[SharedBrowser:{self.account_name}] Bearer token expired. Refreshing...")
        self._bearer_token = None
        await self._trigger_bearer_capture()
        return self._bearer_token

    async def _extract_project_id(self):
        """Extract Flow project ID from page."""
        if not self._page:
            return None

        url = self._page.url
        self._log(f"[SharedBrowser:{self.account_name}] Current URL: {url}")

        if "accounts.google.com" in url or "signin" in url.lower():
            self._log(f"[SharedBrowser:{self.account_name}] Redirected to sign-in — cookies not working!")
            return None

        # URL patterns
        match = re.search(r"/project/([a-z0-9-]{16,})", url, re.IGNORECASE)
        if match:
            self._log(f"[SharedBrowser:{self.account_name}] Project ID from URL: {match.group(1)}")
            return match.group(1)

        match = re.search(r"/flow/([a-f0-9-]{36})", url)
        if match:
            return match.group(1)

        # DOM hints
        try:
            pid = await self._page.evaluate("""
                () => {
                    const regex = /\\/project\\/([a-z0-9-]{16,})/i;
                    const candidates = [String(window.location.href || "")];
                    const links = document.querySelectorAll("a[href*='/project/']");
                    for (const n of links) candidates.push(String(n.href || ""));
                    const scripts = document.querySelectorAll("script");
                    for (const s of scripts) {
                        const t = String(s.textContent || "");
                        if (t.includes("/project/")) candidates.push(t.slice(0, 6000));
                    }
                    for (const item of candidates) {
                        const m = item.match(regex);
                        if (m && m[1]) return m[1];
                    }
                    const body = document.body ? document.body.innerHTML.slice(0, 20000) : '';
                    const m2 = body.match(/"projectId"\\s*:\\s*"([a-f0-9-]{16,})"/);
                    if (m2) return m2[1];
                    return null;
                }
            """)
            if pid:
                self._log(f"[SharedBrowser:{self.account_name}] Project ID from DOM: {pid}")
                return pid
        except Exception as e:
            self._log(f"[SharedBrowser:{self.account_name}] DOM extraction error: {str(e)[:50]}")

        # bot_engine shared cache
        try:
            from src.core.bot_engine import GoogleLabsBot
            cached = GoogleLabsBot._shared_flow_project_id_by_account.get(self.account_name)
            if cached:
                self._log(f"[SharedBrowser:{self.account_name}] Project ID from cache: {cached}")
                return cached
        except Exception:
            pass

        # Click "New project"
        try:
            self._log(f"[SharedBrowser:{self.account_name}] Clicking 'New project'...")
            btn = self._page.locator(
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                " 'abcdefghijklmnopqrstuvwxyz'), 'new project')]"
            ).first
            if await btn.is_visible():
                await btn.click(force=True)
                await asyncio.sleep(5)
                url = self._page.url
                match = re.search(r"/project/([a-z0-9-]{16,})", url, re.IGNORECASE)
                if match:
                    return match.group(1)
        except Exception:
            pass

        self._log(f"[SharedBrowser:{self.account_name}] No project ID found. URL: {self._page.url}")
        return None

    async def _check_recaptcha(self):
        """Check if reCAPTCHA enterprise is loaded on the page."""
        try:
            return await self._page.evaluate(
                "() => typeof grecaptcha !== 'undefined' && !!grecaptcha.enterprise"
            )
        except Exception:
            return False

    def get_page(self):
        return self._page

    def get_project_id(self):
        return self._project_id

    async def maybe_reload(self):
        """Reload page every 100 jobs to keep reCAPTCHA fresh."""
        self._jobs_since_reload += 1
        if self._jobs_since_reload >= 100 and (time.time() - self._last_page_reload) > 60:
            self._log(f"[SharedBrowser:{self.account_name}] Refreshing page (every 100 jobs)...")
            try:
                await self._page.goto(
                    "https://labs.google/fx/tools/flow",
                    wait_until="domcontentloaded", timeout=15000,
                )
                await asyncio.sleep(3)
                pid = await self._extract_project_id()
                if pid:
                    self._project_id = pid
                self._last_page_reload = time.time()
                self._jobs_since_reload = 0
            except Exception:
                pass

    def _load_exported_cookies(self):
        if not os.path.exists(self._cookies_json):
            self._log(f"[SharedBrowser:{self.account_name}] exported_cookies.json NOT FOUND at {self._cookies_json}")
            return []
        try:
            with open(self._cookies_json, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            valid = [c for c in cookies if c.get("name") and c.get("value") and c.get("domain")]
            if len(valid) != len(cookies):
                self._log(
                    f"[SharedBrowser:{self.account_name}] "
                    f"Filtered {len(cookies) - len(valid)} invalid cookies."
                )
            return valid
        except Exception as e:
            self._log(f"[SharedBrowser:{self.account_name}] Cookie file read error: {str(e)[:50]}")
            return []

    async def _import_cookies(self, cookies):
        """Import cookies into browser context. Returns count imported."""
        if not cookies or not self._context:
            return 0
        try:
            await self._context.add_cookies(cookies)
            return len(cookies)
        except Exception as e:
            self._log(f"[SharedBrowser:{self.account_name}] add_cookies error: {str(e)[:80]}")
            # Try importing one by one to skip bad cookies
            imported = 0
            for c in cookies:
                try:
                    await self._context.add_cookies([c])
                    imported += 1
                except Exception:
                    pass
            if imported > 0:
                self._log(f"[SharedBrowser:{self.account_name}] Imported {imported}/{len(cookies)} cookies (some failed).")
            return imported

    def _clean_locks(self, path):
        for lock in ("SingletonLock", "SingletonCookie", "SingletonSocket", "lockfile"):
            lp = os.path.join(path, lock)
            try:
                if os.path.exists(lp) or os.path.islink(lp):
                    os.unlink(lp)
            except Exception:
                pass

    async def stop(self):
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        self._context = None
        self._page = None
        self._log(f"[SharedBrowser:{self.account_name}] Stopped.")


# ═══════════════════════════════════════════════════════════════════════════
# BrowserFetchWorker — executes API calls via shared browser's fetch()
# ═══════════════════════════════════════════════════════════════════════════

class BrowserFetchWorker:
    """
    Worker that executes reCAPTCHA + API calls in ONE page.evaluate().
    No separate browser, no HTTP client — just queues JS fetch() calls.
    RAM: ~0 MB (shares the SharedBrowser's page).
    """

    def __init__(self, slot_id, shared_browser, log_fn):
        self.slot_id = slot_id
        self.account_name = shared_browser.account_name
        self._browser = shared_browser
        self._log = log_fn
        self.is_busy = False
        self.jobs_completed = 0

    async def generate_image(self, prompt, model, ratio, references=None):
        """Generate image: reCAPTCHA + fetch with Bearer token in ONE evaluate."""
        self.is_busy = True
        try:
            page = self._browser.get_page()
            project_id = self._browser.get_project_id()
            bearer = await self._browser.get_bearer_token()
            if not page or not project_id:
                return None, "No browser page or project ID"
            if not bearer:
                return None, "No Bearer token — auth will fail"

            api_model = _resolve_image_model(model)
            api_ratio = _resolve_image_ratio(ratio)
            seed = random.randint(100000, 999999)
            session_id = f";{int(time.time() * 1000)}"
            batch_id = str(uuid.uuid4())

            self._log(f"[{self.slot_id}] Image: model={model} -> {api_model}, ratio={api_ratio}")

            result = await page.evaluate(
                """
                async ([projectId, prompt, model, ratio, seed, sessionId, batchId, refInputs, bearerToken]) => {
                    try {
                        // Step 1: Generate reCAPTCHA token (same browser context)
                        let recaptchaToken = null;
                        try {
                            const scripts = document.querySelectorAll("script[src*='recaptcha'][src*='render=']");
                            let siteKey = null;
                            for (const s of scripts) {
                                const m = s.src.match(/render=([^&]+)/);
                                if (m && m[1] !== 'explicit') { siteKey = m[1]; break; }
                            }
                            if (siteKey && typeof grecaptcha !== 'undefined' && grecaptcha.enterprise) {
                                recaptchaToken = await grecaptcha.enterprise.execute(siteKey, {action: 'generate'});
                            }
                        } catch(e) {
                            return { error: 'reCAPTCHA failed: ' + e.message };
                        }

                        // Step 2: Build payload
                        const clientContext = {
                            projectId: projectId,
                            tool: 'PINHOLE',
                            sessionId: sessionId
                        };
                        if (recaptchaToken) {
                            clientContext.recaptchaContext = {
                                token: recaptchaToken,
                                applicationType: 'RECAPTCHA_APPLICATION_TYPE_WEB'
                            };
                        }

                        const payload = {
                            clientContext: clientContext,
                            mediaGenerationContext: { batchId: batchId },
                            useNewMedia: true,
                            requests: [{
                                imageModelName: model,
                                imageAspectRatio: ratio,
                                structuredPrompt: { parts: [{ text: prompt }] },
                                seed: seed,
                                imageInputs: refInputs || []
                            }]
                        };

                        // Step 3: fetch with Bearer token + cookies
                        const url = `https://aisandbox-pa.googleapis.com/v1/projects/${projectId}/flowMedia:batchGenerateImages`;
                        const resp = await fetch(url, {
                            method: 'POST',
                            credentials: 'include',
                            headers: {
                                'Content-Type': 'text/plain;charset=UTF-8',
                                'Authorization': bearerToken
                            },
                            body: JSON.stringify(payload)
                        });

                        if (!resp.ok) {
                            const errText = await resp.text();
                            return { error: `HTTP ${resp.status}: ${errText.substring(0, 300)}` };
                        }

                        return { success: true, data: await resp.json() };
                    } catch(e) {
                        return { error: e.message || String(e) };
                    }
                }
                """,
                [project_id, prompt, api_model, api_ratio, seed, session_id, batch_id, references or [], bearer],
            )

            await self._browser.maybe_reload()

            if not result:
                return None, "No response from browser evaluate"
            if result.get("error"):
                return None, result["error"]
            if result.get("success"):
                self.jobs_completed += 1
                return result["data"], None
            return None, "Unknown response"

        except Exception as e:
            return None, str(e)[:300]
        finally:
            self.is_busy = False

    async def generate_video(self, prompt, model, ratio):
        """Generate video: reCAPTCHA + fetch with Bearer token in ONE evaluate."""
        self.is_busy = True
        try:
            page = self._browser.get_page()
            project_id = self._browser.get_project_id()
            bearer = await self._browser.get_bearer_token()
            if not page or not project_id:
                return None, "No browser page or project ID"
            if not bearer:
                return None, "No Bearer token — auth will fail"

            api_model = _resolve_video_model(model)
            api_ratio = _resolve_video_ratio(ratio)
            seed = random.randint(100000, 999999)
            session_id = f";{int(time.time() * 1000)}"
            batch_id = str(uuid.uuid4())

            self._log(f"[{self.slot_id}] Video: model={model} -> {api_model}, ratio={api_ratio}")

            result = await page.evaluate(
                """
                async ([projectId, prompt, model, ratio, seed, sessionId, batchId, bearerToken]) => {
                    try {
                        let recaptchaToken = null;
                        try {
                            const scripts = document.querySelectorAll("script[src*='recaptcha'][src*='render=']");
                            let siteKey = null;
                            for (const s of scripts) {
                                const m = s.src.match(/render=([^&]+)/);
                                if (m && m[1] !== 'explicit') { siteKey = m[1]; break; }
                            }
                            if (siteKey && typeof grecaptcha !== 'undefined' && grecaptcha.enterprise) {
                                recaptchaToken = await grecaptcha.enterprise.execute(siteKey, {action: 'generate'});
                            }
                        } catch(e) {
                            return { error: 'reCAPTCHA failed: ' + e.message };
                        }

                        const clientContext = {
                            projectId: projectId,
                            tool: 'PINHOLE',
                            userPaygateTier: 'PAYGATE_TIER_TWO',
                            sessionId: sessionId
                        };
                        if (recaptchaToken) {
                            clientContext.recaptchaContext = {
                                token: recaptchaToken,
                                applicationType: 'RECAPTCHA_APPLICATION_TYPE_WEB'
                            };
                        }

                        const payload = {
                            mediaGenerationContext: { batchId: batchId },
                            clientContext: clientContext,
                            requests: [{
                                aspectRatio: ratio,
                                seed: seed,
                                textInput: { structuredPrompt: { parts: [{ text: prompt }] } },
                                videoModelKey: model,
                                metadata: {}
                            }],
                            useV2ModelConfig: true
                        };

                        const resp = await fetch(
                            'https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText',
                            {
                                method: 'POST',
                                credentials: 'include',
                                headers: {
                                    'Content-Type': 'text/plain;charset=UTF-8',
                                    'Authorization': bearerToken
                                },
                                body: JSON.stringify(payload)
                            }
                        );

                        if (!resp.ok) {
                            const errText = await resp.text();
                            return { error: `HTTP ${resp.status}: ${errText.substring(0, 300)}` };
                        }

                        return { success: true, data: await resp.json() };
                    } catch(e) {
                        return { error: e.message || String(e) };
                    }
                }
                """,
                [project_id, prompt, api_model, api_ratio, seed, session_id, batch_id, bearer],
            )

            await self._browser.maybe_reload()

            if not result:
                return None, "No response from browser evaluate"
            if result.get("error"):
                return None, result["error"]
            if result.get("success"):
                self.jobs_completed += 1
                return result["data"], None
            return None, "Unknown response"

        except Exception as e:
            return None, str(e)[:300]
        finally:
            self.is_busy = False

    async def close(self):
        pass  # Nothing to close — no HTTP session


# ═══════════════════════════════════════════════════════════════════════════
# HttpModeManager — Orchestrates shared browsers + fetch workers
# ═══════════════════════════════════════════════════════════════════════════

class HttpModeManager:
    """
    Manages shared browsers and BrowserFetchWorkers for all accounts.
    Receives the AsyncQueueManager instance to access signals, settings, etc.
    """

    def __init__(self, queue_manager):
        self.qm = queue_manager
        self._log = lambda msg: queue_manager.signals.log_msg.emit(msg)
        self._shared_browsers = {}   # account_name -> SharedBrowser
        self._workers = {}           # account_name -> [BrowserFetchWorker, ...]
        self._active_tasks = []

    async def run(self):
        """Main entry — start shared browsers, dispatch jobs."""
        from playwright.async_api import async_playwright

        all_accs = get_accounts()
        if not all_accs:
            self._log("[HTTP] No accounts configured.")
            return

        slots_per_account = max(1, min(30, get_int_setting("slots_per_account", 3)))
        self._log(
            f"[HTTP] Starting: {len(all_accs)} account(s), "
            f"{slots_per_account} worker(s) each."
        )

        async with async_playwright() as p:
            try:
                # Start shared browsers (1 per account)
                for acc in all_accs:
                    name = acc.get("name", "unknown")
                    session_path = acc.get("session_path", os.path.join(DATA_DIR, name))
                    cookies_json = os.path.join(session_path, "exported_cookies.json")

                    # Try cached project ID
                    cached_pid = None
                    try:
                        from src.core.bot_engine import GoogleLabsBot
                        cached_pid = GoogleLabsBot._shared_flow_project_id_by_account.get(name)
                    except Exception:
                        pass

                    browser = SharedBrowser(name, session_path, cookies_json, self._log, project_id=cached_pid)
                    success = await browser.start(p)

                    if not success:
                        self._log(f"[HTTP] {name}: Browser failed! Account skipped.")
                        continue

                    self._shared_browsers[name] = browser

                    # Create fetch workers
                    account_workers = []
                    for idx in range(1, slots_per_account + 1):
                        slot_id = f"{name}#h{idx}"
                        worker = BrowserFetchWorker(slot_id, browser, self._log)
                        account_workers.append(worker)

                    self._workers[name] = account_workers
                    self._log(f"[HTTP] {name}: 1 shared browser + {len(account_workers)} fetch workers ready.")

                total_workers = sum(len(w) for w in self._workers.values())
                total_browsers = len(self._shared_browsers)

                if total_workers == 0:
                    self._log("[HTTP] No workers started. Cannot proceed.")
                    return

                self._log(
                    f"[HTTP] Total: {total_browsers} browser(s), "
                    f"{total_workers} fetch worker(s). "
                    f"Est. RAM: ~{total_browsers * 300} MB."
                )

                # Main dispatch loop
                while self.qm.is_running:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        break
                    if self.qm.pause_requested:
                        await asyncio.sleep(1)
                        continue

                    self._active_tasks = [t for t in self._active_tasks if not t.done()]

                    jobs = get_all_jobs()
                    pending = [j for j in jobs if j["status"] == "pending"]

                    if not pending:
                        if not self._active_tasks:
                            still_active = any(
                                j["status"] in ("pending", "running") for j in get_all_jobs()
                            )
                            if not still_active:
                                self._log("[HTTP] All jobs completed.")
                                break
                        await asyncio.sleep(self.qm.scheduler_poll_seconds)
                        continue

                    busy_slots = {t.get_name() for t in self._active_tasks if hasattr(t, "get_name")}

                    dispatched = 0
                    for job in pending:
                        worker = self._get_available_worker(busy_slots)
                        if not worker:
                            break

                        job_id = job["id"]
                        update_job_status(job_id, "running", account=worker.account_name)
                        self.qm.signals.job_updated.emit(job_id, "running", worker.account_name, "")

                        task = asyncio.create_task(
                            self._run_job(worker, job), name=worker.slot_id,
                        )
                        self._active_tasks.append(task)
                        busy_slots.add(worker.slot_id)
                        dispatched += 1

                        stagger = random.uniform(
                            self.qm.global_stagger_min_seconds,
                            self.qm.global_stagger_max_seconds,
                        )
                        if stagger > 0:
                            await asyncio.sleep(stagger)

                    if dispatched == 0:
                        await asyncio.sleep(self.qm.scheduler_poll_seconds)

                if self._active_tasks:
                    self._log(f"[HTTP] Waiting for {len(self._active_tasks)} active job(s)...")
                    await asyncio.gather(*self._active_tasks, return_exceptions=True)

            finally:
                for w_list in self._workers.values():
                    for w in w_list:
                        await w.close()
                for browser in self._shared_browsers.values():
                    await browser.stop()
                self._workers.clear()
                self._shared_browsers.clear()
                self._log("[HTTP] All browsers and workers stopped.")

    def _get_available_worker(self, busy_slots):
        for workers in self._workers.values():
            for worker in workers:
                if worker.slot_id not in busy_slots and not worker.is_busy:
                    return worker
        return None

    async def _run_job(self, worker, job):
        """Execute a single job with retries."""
        job_id = job["id"]
        job_type = job.get("job_type", "image")
        prompt = job.get("prompt", "")
        model = job.get("model", "")

        self._log(f"[{worker.slot_id}] Processing job {job_id[:6]}...: {prompt[:40]}...")

        max_retries = max(1, get_int_setting("max_auto_retries_per_job", 3))
        last_error = ""

        for attempt in range(max_retries + 1):
            try:
                if "video" in job_type:
                    video_model = job.get("video_model") or model
                    ratio = job.get("aspect_ratio", "ASPECT_RATIO_16_9")
                    result, error = await worker.generate_video(prompt, video_model, ratio)
                else:
                    ratio = job.get("aspect_ratio", "IMAGE_ASPECT_RATIO_LANDSCAPE")
                    result, error = await worker.generate_image(prompt, model, ratio)

                if result and not error:
                    update_job_status(job_id, "completed", account=worker.account_name)
                    self.qm.signals.job_updated.emit(job_id, "completed", worker.account_name, "")
                    self._log(f"[{worker.slot_id}] Job {job_id[:6]}... completed!")
                    return

                last_error = error or "Unknown error"
                self._log(
                    f"[{worker.slot_id}] Attempt {attempt + 1}/{max_retries + 1} "
                    f"failed: {last_error[:200]}"
                )

                # Force Bearer refresh on 401
                if "401" in last_error or "auth" in last_error.lower():
                    self._log(f"[{worker.slot_id}] Auth failure — refreshing Bearer token...")
                    worker._browser._bearer_token = None

                if attempt < max_retries:
                    await asyncio.sleep(10 * (attempt + 1))

            except Exception as e:
                last_error = str(e)[:300]
                self._log(f"[{worker.slot_id}] Attempt {attempt + 1} exception: {last_error}")
                if attempt < max_retries:
                    await asyncio.sleep(10)

        update_job_status(job_id, "failed", account=worker.account_name, error=last_error)
        self.qm.signals.job_updated.emit(job_id, "failed", worker.account_name, last_error)
        self._log(f"[{worker.slot_id}] Job {job_id[:6]}... FAILED: {last_error[:200]}")
