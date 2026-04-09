"""
CDP Shared Browser Mode — 1 CloakBrowser process per account, N contexts via CDP.

Each context = independent cookies, independent session, independent reCAPTCHA.
Uses SAME page.evaluate() API calls as proven bot_engine (not fetch/HTTP).

RAM: 300MB base + ~30MB per context
  20 slots = ~900MB total (vs ~6GB with browser-per-slot)
"""

import asyncio
import json
import os
import re
import time
import random
import platform
import subprocess

from src.core.app_paths import get_sessions_dir
from src.core.process_tracker import process_tracker, cleanup_session_locks
from src.core.cloakbrowser_support import load_cloakbrowser_api
from src.db.db_manager import (
    get_accounts,
    get_all_jobs,
    get_int_setting,
    get_setting,
    update_job_status,
)

DATA_DIR = str(get_sessions_dir())


# ═══════════════════════════════════════════════════════════════════════════
# Model / ratio resolvers (same as bot_engine)
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_image_model(model):
    lower = str(model or "").lower()
    if "nano banana pro" in lower:
        return "GEM_PIX_2"
    if "nano banana" in lower:
        return "NARWHAL"
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


def _fix_cookies_for_import(cookies):
    """Fix sameSite/secure mismatch before importing."""
    fixed = []
    for c in cookies:
        if not c.get("name") or not c.get("value") or not c.get("domain"):
            continue
        same_site = c.get("sameSite", "Lax")
        secure = c.get("secure", False)
        if same_site == "None" and not secure:
            c["sameSite"] = "Lax"
        if same_site not in ("Strict", "Lax", "None"):
            c["sameSite"] = "Lax"
        fixed.append(c)
    return fixed


# ═══════════════════════════════════════════════════════════════════════════
# CDPSlotWorker — one context in the shared browser
# ═══════════════════════════════════════════════════════════════════════════

# The SAME JavaScript that bot_engine uses for image generation.
# Handles auth session, reCAPTCHA, and API call all in one evaluate().
_IMAGE_GENERATE_JS = """
async ({ projectId, prompt, modelName, aspectRatio, batchId, seed, recaptchaAction, referenceMediaIds }) => {
    const getAuthSession = async () => {
        try {
            const resp = await fetch("https://labs.google/fx/api/auth/session", {
                method: "GET", credentials: "include",
            });
            if (!resp.ok) return null;
            const data = await resp.json().catch(() => null);
            if (!data || !data.access_token) return null;
            return data;
        } catch { return null; }
    };

    const getRecaptchaContext = async () => {
        try {
            const enterprise = window.grecaptcha?.enterprise;
            if (!enterprise || typeof enterprise.execute !== "function") return null;
            const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
            let siteKey = null;
            const scripts = Array.from(document.querySelectorAll("script[src*='recaptcha'][src*='render=']"));
            for (const script of scripts) {
                try {
                    const u = new URL(script.src);
                    const render = u.searchParams.get("render");
                    if (render && render !== "explicit") { siteKey = render; break; }
                } catch {}
            }
            if (!siteKey) return null;
            if (typeof enterprise.ready === "function") {
                await new Promise((r) => enterprise.ready(r));
            }
            await sleep(500 + Math.floor(Math.random() * 1000));
            const token = await enterprise.execute(siteKey, { action: recaptchaAction });
            if (token) await sleep(300 + Math.floor(Math.random() * 500));
            if (!token) return null;
            return { token, applicationType: "RECAPTCHA_APPLICATION_TYPE_WEB" };
        } catch { return null; }
    };

    const authSession = await getAuthSession();
    if (!authSession || !authSession.access_token) {
        return { ok: false, status: 0, error: "missing auth session access token" };
    }

    const recaptchaContext = await getRecaptchaContext();
    const clientContext = { projectId, tool: "PINHOLE", sessionId: ";" + Date.now() };
    if (recaptchaContext) clientContext.recaptchaContext = recaptchaContext;

    const refs = Array.isArray(referenceMediaIds) && referenceMediaIds.length > 0
        ? referenceMediaIds.map((id) => ({ imageInputType: "IMAGE_INPUT_TYPE_REFERENCE", name: id }))
        : [];

    const body = {
        clientContext,
        mediaGenerationContext: { batchId },
        useNewMedia: true,
        requests: [{
            clientContext,
            imageModelName: modelName,
            imageAspectRatio: aspectRatio,
            structuredPrompt: { parts: [{ text: String(prompt || "") }] },
            seed,
            imageInputs: refs,
        }],
    };

    try {
        const resp = await fetch(
            `https://aisandbox-pa.googleapis.com/v1/projects/${projectId}/flowMedia:batchGenerateImages`,
            {
                method: "POST", credentials: "include",
                headers: { "content-type": "text/plain;charset=UTF-8", "authorization": `Bearer ${authSession.access_token}` },
                body: JSON.stringify(body),
            }
        );
        const text = await resp.text();
        let data = null;
        try { data = JSON.parse(text); } catch {}
        if (!resp.ok) {
            return { ok: false, status: resp.status, error: (data?.error?.message || data?.error?.status || text.slice(0, 300) || `HTTP ${resp.status}`) };
        }
        return { ok: true, status: resp.status, data };
    } catch (e) {
        return { ok: false, status: 0, error: String(e) };
    }
}
"""

_VIDEO_GENERATE_JS = """
async ({ projectId, prompt, modelKey, aspectRatio, batchId, seed, recaptchaAction }) => {
    const getAuthSession = async () => {
        try {
            const resp = await fetch("https://labs.google/fx/api/auth/session", { method: "GET", credentials: "include" });
            if (!resp.ok) return null;
            const data = await resp.json().catch(() => null);
            return (data && data.access_token) ? data : null;
        } catch { return null; }
    };

    const getRecaptchaContext = async () => {
        try {
            const enterprise = window.grecaptcha?.enterprise;
            if (!enterprise) return null;
            const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
            let siteKey = null;
            for (const s of document.querySelectorAll("script[src*='recaptcha'][src*='render=']")) {
                try { const m = new URL(s.src).searchParams.get("render"); if (m && m !== "explicit") { siteKey = m; break; } } catch {}
            }
            if (!siteKey) return null;
            if (typeof enterprise.ready === "function") await new Promise((r) => enterprise.ready(r));
            await sleep(500 + Math.floor(Math.random() * 1000));
            const token = await enterprise.execute(siteKey, { action: recaptchaAction });
            if (!token) return null;
            await sleep(300 + Math.floor(Math.random() * 500));
            return { token, applicationType: "RECAPTCHA_APPLICATION_TYPE_WEB" };
        } catch { return null; }
    };

    const auth = await getAuthSession();
    if (!auth) return { ok: false, error: "missing auth session access token" };

    const recap = await getRecaptchaContext();
    const ctx = { projectId, tool: "PINHOLE", userPaygateTier: "PAYGATE_TIER_TWO", sessionId: ";" + Date.now() };
    if (recap) ctx.recaptchaContext = recap;

    const body = {
        mediaGenerationContext: { batchId },
        clientContext: ctx,
        requests: [{ clientContext: ctx, aspectRatio, seed, textInput: { structuredPrompt: { parts: [{ text: prompt }] } }, videoModelKey: modelKey, metadata: {} }],
        useV2ModelConfig: true,
    };

    try {
        const resp = await fetch("https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText", {
            method: "POST", credentials: "include",
            headers: { "content-type": "text/plain;charset=UTF-8", "authorization": `Bearer ${auth.access_token}` },
            body: JSON.stringify(body),
        });
        const text = await resp.text();
        let data = null; try { data = JSON.parse(text); } catch {}
        if (!resp.ok) return { ok: false, status: resp.status, error: (data?.error?.message || text.slice(0, 300)) };
        return { ok: true, status: resp.status, data };
    } catch (e) { return { ok: false, error: String(e) }; }
}
"""


class CDPSlotWorker:
    """One context in the shared CDP browser. Uses SAME API code as bot_engine."""

    def __init__(self, slot_id, context, page, project_id, log_fn):
        self.slot_id = slot_id
        self.account_name = slot_id.split("#")[0] if "#" in slot_id else slot_id
        self.context = context
        self._page = page
        self._project_id = project_id
        self._log = log_fn
        self.is_busy = False
        self.jobs_completed = 0

    async def generate_image(self, prompt, model, ratio, references=None):
        """Generate image using SAME page.evaluate JS as bot_engine."""
        self.is_busy = True
        try:
            api_model = _resolve_image_model(model)
            api_ratio = _resolve_image_ratio(ratio)
            seed = random.randint(100000, 999999)
            batch_id = f"{random.getrandbits(128):032x}"

            self._log(f"[{self.slot_id}] Image: {api_model}, {api_ratio}")

            result = await self._page.evaluate(
                _IMAGE_GENERATE_JS,
                {
                    "projectId": self._project_id,
                    "prompt": prompt,
                    "modelName": api_model,
                    "aspectRatio": api_ratio,
                    "batchId": batch_id,
                    "seed": seed,
                    "recaptchaAction": "IMAGE_GENERATION",
                    "referenceMediaIds": references or [],
                },
            )

            if not result:
                return None, "No response from page.evaluate"
            if not result.get("ok"):
                return None, result.get("error", "Unknown error")

            self.jobs_completed += 1
            return result.get("data"), None

        except Exception as e:
            await self._try_reload()
            return None, str(e)[:200]
        finally:
            self.is_busy = False
            await self._maybe_refresh()

    async def generate_video(self, prompt, model, ratio):
        """Generate video using SAME page.evaluate JS as bot_engine."""
        self.is_busy = True
        try:
            api_model = _resolve_video_model(model)
            api_ratio = _resolve_video_ratio(ratio)
            seed = random.randint(100000, 999999)
            batch_id = f"{random.getrandbits(128):032x}"

            self._log(f"[{self.slot_id}] Video: {api_model}, {api_ratio}")

            result = await self._page.evaluate(
                _VIDEO_GENERATE_JS,
                {
                    "projectId": self._project_id,
                    "prompt": prompt,
                    "modelKey": api_model,
                    "aspectRatio": api_ratio,
                    "batchId": batch_id,
                    "seed": seed,
                    "recaptchaAction": "VIDEO_GENERATION",
                },
            )

            if not result:
                return None, "No response"
            if not result.get("ok"):
                return None, result.get("error", "Unknown error")

            self.jobs_completed += 1
            return result.get("data"), None

        except Exception as e:
            await self._try_reload()
            return None, str(e)[:200]
        finally:
            self.is_busy = False
            await self._maybe_refresh()

    async def _try_reload(self):
        try:
            await self._page.goto(
                "https://labs.google/fx/tools/flow",
                wait_until="domcontentloaded", timeout=15000,
            )
            await asyncio.sleep(2)
        except Exception:
            pass

    async def _maybe_refresh(self):
        if self.jobs_completed > 0 and self.jobs_completed % 50 == 0:
            self._log(f"[{self.slot_id}] Auto-refresh (every 50 jobs)...")
            await self._try_reload()

    async def close(self):
        try:
            await self.context.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
# CDPBrowserServer — 1 CloakBrowser process per account
# ═══════════════════════════════════════════════════════════════════════════

class CDPBrowserServer:
    """Manages 1 CloakBrowser process. Creates N contexts via CDP."""

    def __init__(self, account_name, session_path, cookies_json_path, cdp_port, log_fn):
        self.account_name = account_name
        self._session_path = session_path
        self._cookies_json = cookies_json_path
        self._cdp_port = cdp_port
        self._log = log_fn
        self._process = None
        self._browser = None
        self._playwright = None
        self._project_id = None
        self._slots = []

    async def start(self, num_slots):
        """Start CloakBrowser process and create N context slots."""
        self._log(f"[CDPServer:{self.account_name}] Starting on port {self._cdp_port}...")

        cleanup_session_locks(self._session_path)

        # Find CloakBrowser binary
        binary = self._find_cloak_binary()
        if not binary:
            self._log(f"[CDPServer:{self.account_name}] CloakBrowser binary not found!")
            return False

        seed = random.randint(10000, 99999)
        cloak_display = str(get_setting("cloak_display", "headless") or "headless").strip().lower()
        is_headless = cloak_display != "visible"

        # Use _cloak profile to avoid lock conflicts with login Chrome
        profile = self._session_path + "_cloak"
        os.makedirs(profile, exist_ok=True)
        cleanup_session_locks(profile)

        self._log(f"[CDPServer:{self.account_name}] Profile: {profile}")
        self._log(f"[CDPServer:{self.account_name}] Display: {'headless' if is_headless else 'visible'}")

        # Build launch args
        chrome_args = [
            binary,
            f"--remote-debugging-port={self._cdp_port}",
            f"--user-data-dir={profile}",
            f"--fingerprint={seed}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ]
        if is_headless:
            chrome_args.append("--headless=new")

        # Launch
        popen_kwargs = {}
        if platform.system() == "Windows":
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        else:
            popen_kwargs["stdout"] = subprocess.DEVNULL
            popen_kwargs["stderr"] = subprocess.DEVNULL

        self._process = subprocess.Popen(chrome_args, **popen_kwargs)
        process_tracker.register(self._process.pid)
        self._log(f"[CDPServer:{self.account_name}] PID: {self._process.pid}")

        # Wait for CDP
        ready = await self._wait_for_cdp(timeout=15)
        if not ready:
            self._log(f"[CDPServer:{self.account_name}] CDP not ready after 15s!")
            return False

        self._log(f"[CDPServer:{self.account_name}] CDP ready!")

        # Connect Playwright
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.connect_over_cdp(
            f"http://127.0.0.1:{self._cdp_port}"
        )

        # Load cookies
        cookies = self._load_cookies()
        self._log(f"[CDPServer:{self.account_name}] Cookies loaded: {len(cookies)}")

        # Create first slot + extract project ID
        ctx1 = await self._browser.new_context()
        if cookies:
            try:
                await ctx1.add_cookies(cookies)
            except Exception:
                pass
        page1 = await ctx1.new_page()
        await page1.goto("https://labs.google/fx/tools/flow", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # Check login
        if "accounts.google.com" in page1.url:
            self._log(f"[CDPServer:{self.account_name}] Redirected to login — session invalid!")
            return False

        # Extract project ID
        self._project_id = await self._extract_project_id(page1)
        if not self._project_id:
            # Try bot_engine cache
            try:
                from src.core.bot_engine import GoogleLabsBot
                self._project_id = GoogleLabsBot._shared_flow_project_id_by_account.get(self.account_name)
            except Exception:
                pass
        if not self._project_id:
            self._log(f"[CDPServer:{self.account_name}] No project ID!")
            return False

        self._log(f"[CDPServer:{self.account_name}] Project: {self._project_id}")

        # First slot
        slot1 = CDPSlotWorker(f"{self.account_name}#c1", ctx1, page1, self._project_id, self._log)
        self._slots.append(slot1)

        # Remaining slots
        for i in range(2, num_slots + 1):
            try:
                ctx = await self._browser.new_context()
                if cookies:
                    try:
                        await ctx.add_cookies(cookies)
                    except Exception:
                        pass
                page = await ctx.new_page()
                await page.goto("https://labs.google/fx/tools/flow", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(2)
                slot = CDPSlotWorker(f"{self.account_name}#c{i}", ctx, page, self._project_id, self._log)
                self._slots.append(slot)
                await asyncio.sleep(0.5)
            except Exception as e:
                self._log(f"[CDPServer:{self.account_name}] Slot {i} failed: {str(e)[:60]}")

        est_ram = 300 + len(self._slots) * 30
        self._log(
            f"[CDPServer:{self.account_name}] {len(self._slots)} slots ready. "
            f"Est RAM: ~{est_ram}MB"
        )
        return len(self._slots) > 0

    def _find_cloak_binary(self):
        """Find CloakBrowser binary path."""
        try:
            from cloakbrowser import binary_info
            info = binary_info()
            path = info.get("binary_path", "")
            if path and os.path.exists(path):
                return path
        except Exception:
            pass
        try:
            from cloakbrowser.config import get_binary_path
            path = get_binary_path()
            if path and os.path.exists(path):
                return path
        except Exception:
            pass
        return None

    async def _wait_for_cdp(self, timeout=15):
        """Wait for CDP endpoint to accept connections."""
        import requests as http_req
        for _ in range(timeout * 2):
            try:
                resp = http_req.get(f"http://127.0.0.1:{self._cdp_port}/json/version", timeout=2)
                if resp.ok:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)
        return False

    def _load_cookies(self):
        """Load and fix cookies from exported_cookies.json."""
        if not os.path.exists(self._cookies_json):
            return []
        try:
            with open(self._cookies_json, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return _fix_cookies_for_import(raw)
        except Exception:
            return []

    async def _extract_project_id(self, page):
        """Extract project ID from Flow page."""
        url = page.url
        match = re.search(r"/project/([a-z0-9-]{16,})", url, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r"/flow/([a-f0-9-]{36})", url)
        if match:
            return match.group(1)
        try:
            pid = await page.evaluate("""
                () => {
                    const regex = /\\/project\\/([a-z0-9-]{16,})/i;
                    const candidates = [window.location.href];
                    for (const n of document.querySelectorAll("a[href*='/project/']"))
                        candidates.push(n.href || "");
                    for (const s of document.querySelectorAll("script")) {
                        const t = s.textContent || "";
                        if (t.includes("/project/")) candidates.push(t.slice(0, 6000));
                    }
                    for (const item of candidates) {
                        const m = item.match(regex);
                        if (m && m[1]) return m[1];
                    }
                    const body = document.body ? document.body.innerHTML.slice(0, 20000) : '';
                    const m2 = body.match(/"projectId"\\s*:\\s*"([a-f0-9-]{16,})"/);
                    return m2 ? m2[1] : null;
                }
            """)
            return pid
        except Exception:
            pass
        return None

    def get_available_slot(self):
        for slot in self._slots:
            if not slot.is_busy:
                return slot
        return None

    def get_all_slots(self):
        return list(self._slots)

    async def stop(self):
        """Stop all slots, browser, and process."""
        for slot in self._slots:
            await slot.close()

        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass

        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass

        if self._process:
            try:
                self._process.terminate()
                self._process.wait(timeout=5)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass
            process_tracker.unregister(self._process.pid)

        total = sum(s.jobs_completed for s in self._slots)
        self._log(f"[CDPServer:{self.account_name}] Stopped. {total} jobs across {len(self._slots)} slots.")


# ═══════════════════════════════════════════════════════════════════════════
# CDPSharedManager — orchestrates CDP servers for all accounts
# ═══════════════════════════════════════════════════════════════════════════

class CDPSharedManager:
    """Manages CDP servers and dispatches jobs to slot workers."""

    def __init__(self, queue_manager):
        self.qm = queue_manager
        self._log = lambda msg: queue_manager.signals.log_msg.emit(msg)
        self._servers = {}
        self._active_tasks = []

    async def run(self):
        """Main entry — start servers, dispatch jobs."""
        all_accs = get_accounts()
        if not all_accs:
            self._log("[CDPShared] No accounts configured.")
            return

        slots_per_account = max(1, min(40, get_int_setting("slots_per_account", 3)))
        base_port = random.randint(9222, 9280)

        self._log(
            f"[CDPShared] Starting: {len(all_accs)} account(s), "
            f"{slots_per_account} context(s) each."
        )

        try:
            for i, acc in enumerate(all_accs):
                name = acc.get("name", "unknown")
                session_path = acc.get("session_path", os.path.join(DATA_DIR, name))
                cookies_json = os.path.join(session_path, "exported_cookies.json")
                port = base_port + i

                server = CDPBrowserServer(name, session_path, cookies_json, port, self._log)
                success = await server.start(slots_per_account)

                if success:
                    self._servers[name] = server
                else:
                    self._log(f"[CDPShared] {name}: Failed! Account skipped.")

            total_slots = sum(len(s.get_all_slots()) for s in self._servers.values())
            total_servers = len(self._servers)

            if total_slots == 0:
                self._log("[CDPShared] No slots started.")
                return

            est_ram = total_servers * 300 + total_slots * 30
            self._log(
                f"[CDPShared] Total: {total_servers} browser(s), "
                f"{total_slots} context(s). Est RAM: ~{est_ram}MB."
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
                        still = any(j["status"] in ("pending", "running") for j in get_all_jobs())
                        if not still:
                            self._log("[CDPShared] All jobs completed.")
                            break
                    await asyncio.sleep(self.qm.scheduler_poll_seconds)
                    continue

                busy = {t.get_name() for t in self._active_tasks if hasattr(t, "get_name")}
                dispatched = 0

                for job in pending:
                    slot = self._get_available_slot(busy)
                    if not slot:
                        break

                    job_id = job["id"]
                    update_job_status(job_id, "running", account=slot.account_name)
                    self.qm.signals.job_updated.emit(job_id, "running", slot.account_name, "")

                    task = asyncio.create_task(self._run_job(slot, job), name=slot.slot_id)
                    self._active_tasks.append(task)
                    busy.add(slot.slot_id)
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
                self._log(f"[CDPShared] Waiting for {len(self._active_tasks)} active job(s)...")
                await asyncio.gather(*self._active_tasks, return_exceptions=True)

        finally:
            for server in self._servers.values():
                await server.stop()
            self._servers.clear()
            self._log("[CDPShared] All browsers and slots stopped.")

    def _get_available_slot(self, busy_slots):
        for server in self._servers.values():
            for slot in server.get_all_slots():
                if slot.slot_id not in busy_slots and not slot.is_busy:
                    return slot
        return None

    async def _run_job(self, slot, job):
        """Execute a single job with retries."""
        job_id = job["id"]
        job_type = job.get("job_type", "image")
        prompt = job.get("prompt", "")
        model = job.get("model", "")

        self._log(f"[{slot.slot_id}] Job {job_id[:6]}...: {prompt[:40]}...")

        max_retries = max(1, get_int_setting("max_auto_retries_per_job", 3))
        last_error = ""

        for attempt in range(max_retries + 1):
            try:
                if "video" in job_type:
                    video_model = job.get("video_model") or model
                    ratio = job.get("aspect_ratio", "ASPECT_RATIO_16_9")
                    result, error = await slot.generate_video(prompt, video_model, ratio)
                else:
                    ratio = job.get("aspect_ratio", "IMAGE_ASPECT_RATIO_LANDSCAPE")
                    result, error = await slot.generate_image(prompt, model, ratio)

                if result and not error:
                    update_job_status(job_id, "completed", account=slot.account_name)
                    self.qm.signals.job_updated.emit(job_id, "completed", slot.account_name, "")
                    self.qm.signals.account_auth_status.emit(slot.account_name, "logged_in", "Success")
                    self._log(f"[{slot.slot_id}] Job {job_id[:6]}... completed!")
                    return

                last_error = error or "Unknown error"
                self._log(f"[{slot.slot_id}] Attempt {attempt + 1}/{max_retries + 1} failed: {last_error[:200]}")

                if "recaptcha" in last_error.lower():
                    await slot._try_reload()
                    await asyncio.sleep(3)
                elif "auth" in last_error.lower() or "401" in last_error:
                    await slot._try_reload()
                    await asyncio.sleep(3)
                elif attempt < max_retries:
                    await asyncio.sleep(10 * (attempt + 1))

            except Exception as e:
                last_error = str(e)[:200]
                self._log(f"[{slot.slot_id}] Attempt {attempt + 1} exception: {last_error}")
                if attempt < max_retries:
                    await asyncio.sleep(10)

        update_job_status(job_id, "failed", account=slot.account_name, error=last_error)
        self.qm.signals.job_updated.emit(job_id, "failed", slot.account_name, last_error)
        self._log(f"[{slot.slot_id}] Job {job_id[:6]}... FAILED: {last_error[:200]}")
