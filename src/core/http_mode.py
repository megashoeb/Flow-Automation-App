"""
HTTP + Shared Token Server mode (EXPERIMENTAL).

Instead of 1 browser per slot (~300MB each), this uses:
- 1 browser per ACCOUNT for reCAPTCHA token generation
- Pure HTTP requests for all API calls (~5MB per slot)
- Shared cookie/token pool across all slots for the same account

This allows 20-30 parallel slots in ~500MB total RAM.
"""

import asyncio
import json
import os
import time
import uuid
import base64

import requests as http_requests

from src.core.app_paths import get_sessions_dir
from src.core.process_tracker import cleanup_session_locks
from src.db.db_manager import (
    get_accounts,
    get_setting,
    get_int_setting,
    get_bool_setting,
    get_pending_jobs,
    update_job_status,
    get_job_by_id,
)

DATA_DIR = str(get_sessions_dir())

# ── API Endpoints ──
AUTH_SESSION_URL = "https://labs.google/fx/api/auth/session"
FLOW_PAGE_URL = "https://labs.google/fx/tools/flow"

IMAGE_GENERATE_URL = (
    "https://aisandbox-pa.googleapis.com/v1/projects/{project_id}"
    "/flowMedia:batchGenerateImages"
)
VIDEO_TEXT_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText"
VIDEO_REF_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoReferenceImages"
VIDEO_START_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartImage"
VIDEO_START_END_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartAndEndImage"
VIDEO_STATUS_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus"
VIDEO_UPSCALE_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoUpsampleVideo"
IMAGE_UPLOAD_URL = "https://aisandbox-pa.googleapis.com/v1/flow/uploadImage"
MEDIA_DOWNLOAD_URL = "https://labs.google/fx/api/trpc/media.getMediaUrlRedirect"


class TokenServer:
    """
    Manages ONE browser per account solely for reCAPTCHA token generation.
    The browser stays open and generates tokens on demand.
    """

    def __init__(self, account_name, session_path, logger=None):
        self.account_name = account_name
        self.session_path = session_path
        self._log = logger or (lambda msg: None)
        self._browser = None
        self._context = None
        self._page = None
        self._pw = None
        self._site_key = None
        self._lock = asyncio.Lock()

    async def start(self, playwright_instance):
        """Launch browser and navigate to Flow page to load reCAPTCHA."""
        from src.core.bot_engine import GoogleLabsBot

        self._pw = playwright_instance
        chrome_path = GoogleLabsBot._find_chrome_path_static()

        cleanup_session_locks(self.session_path)

        # Import cookies
        cookies = self._load_exported_cookies()

        # Launch minimal browser just for reCAPTCHA
        try:
            from src.core.cloakbrowser_support import load_cloakbrowser_api
            cloak_api = load_cloakbrowser_api()
            cloak_persistent = cloak_api.get("persistent_async")

            if cloak_api.get("available") and cloak_persistent:
                self._context = await cloak_persistent(
                    self.session_path,
                    headless=True,
                    args=["--disable-gpu", "--no-sandbox"],
                    humanize=True,
                )
                self._log(f"[{self.account_name}] Token server: CloakBrowser started (headless).")
            else:
                raise RuntimeError("CloakBrowser not available")
        except Exception:
            # Fallback to Playwright persistent context
            launch_args = [
                "--disable-gpu",
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
            ]
            self._context = await playwright_instance.chromium.launch_persistent_context(
                self.session_path,
                headless=True,
                args=launch_args,
                ignore_default_args=["--enable-automation"],
            )
            self._log(f"[{self.account_name}] Token server: Playwright started (headless).")

        # Import cookies if available
        if cookies:
            try:
                await self._context.add_cookies(cookies)
                self._log(f"[{self.account_name}] Token server: Imported {len(cookies)} cookies.")
            except Exception as e:
                self._log(f"[{self.account_name}] Token server: Cookie import warning: {str(e)[:50]}")

        pages = self._context.pages
        self._page = pages[0] if pages else await self._context.new_page()

        # Navigate to Flow page to load reCAPTCHA scripts
        try:
            await self._page.goto(FLOW_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            self._log(f"[{self.account_name}] Token server: Flow page loaded.")
        except Exception as e:
            self._log(f"[{self.account_name}] Token server: Page load warning: {str(e)[:50]}")

        # Extract reCAPTCHA site key
        try:
            self._site_key = await self._page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll("script[src*='recaptcha'][src*='render=']");
                    for (const s of scripts) {
                        const m = s.src.match(/render=([^&]+)/);
                        if (m && m[1] !== 'explicit') return m[1];
                    }
                    return null;
                }
            """)
            if self._site_key:
                self._log(f"[{self.account_name}] Token server: reCAPTCHA siteKey found.")
            else:
                self._log(f"[{self.account_name}] Token server: No reCAPTCHA siteKey found.")
        except Exception as e:
            self._log(f"[{self.account_name}] Token server: siteKey extraction failed: {str(e)[:50]}")

    async def get_recaptcha_token(self, action="IMAGE_GENERATION"):
        """Generate a fresh reCAPTCHA token."""
        async with self._lock:
            if not self._page or not self._site_key:
                return None

            try:
                token = await self._page.evaluate(f"""
                    async () => {{
                        if (!window.grecaptcha || !window.grecaptcha.enterprise) return null;
                        try {{
                            const token = await window.grecaptcha.enterprise.execute(
                                '{self._site_key}', {{ action: '{action}' }}
                            );
                            return token;
                        }} catch(e) {{
                            return null;
                        }}
                    }}
                """)
                return token
            except Exception as e:
                self._log(f"[{self.account_name}] reCAPTCHA token error: {str(e)[:50]}")
                return None

    async def get_auth_session(self):
        """Get auth session (access_token) via the browser."""
        if not self._page:
            return None
        try:
            result = await self._page.evaluate("""
                async () => {
                    try {
                        const resp = await fetch('https://labs.google/fx/api/auth/session', {
                            credentials: 'include'
                        });
                        return await resp.json();
                    } catch(e) {
                        return null;
                    }
                }
            """)
            return result
        except Exception:
            return None

    async def get_cookies_for_http(self):
        """Export current browser cookies as a dict for requests.Session."""
        if not self._context:
            return {}
        try:
            cookies = await self._context.cookies()
            cookie_dict = {}
            for c in cookies:
                cookie_dict[c["name"]] = c["value"]
            return cookie_dict
        except Exception:
            return {}

    def _load_exported_cookies(self):
        """Load exported_cookies.json for this account."""
        cookies_path = os.path.join(self.session_path, "exported_cookies.json")
        if not os.path.exists(cookies_path):
            return []
        try:
            with open(cookies_path, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            # Filter invalid cookies
            valid = []
            for c in cookies:
                if c.get("name") and c.get("value") and c.get("domain"):
                    valid.append(c)
            return valid
        except Exception:
            return []

    async def stop(self):
        """Shut down the token server browser."""
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        self._context = None
        self._page = None
        self._browser = None


class HttpApiWorker:
    """
    Pure HTTP worker — uses requests library for API calls.
    Gets reCAPTCHA tokens from the shared TokenServer.
    No browser needed (~5MB RAM per worker).
    """

    def __init__(self, slot_label, account_name, token_server, logger=None):
        self.slot_label = slot_label
        self.account_name = account_name
        self.token_server = token_server
        self._log = logger or (lambda msg: None)
        self._session = http_requests.Session()
        self._access_token = None
        self._project_id = None
        self._cookies_loaded = False

    async def initialize(self):
        """Load cookies and get initial auth token."""
        # Get cookies from token server
        cookie_dict = await self.token_server.get_cookies_for_http()
        if cookie_dict:
            self._session.cookies.update(cookie_dict)
            self._cookies_loaded = True
            self._log(f"[{self.slot_label}] HTTP worker: Loaded {len(cookie_dict)} cookies.")

        # Get auth session
        await self._refresh_auth()

    async def _refresh_auth(self):
        """Refresh the access token."""
        auth = await self.token_server.get_auth_session()
        if auth and auth.get("access_token"):
            self._access_token = auth["access_token"]
            self._log(f"[{self.slot_label}] HTTP worker: Auth token obtained.")
            return True
        self._log(f"[{self.slot_label}] HTTP worker: Auth token failed!")
        return False

    def _api_headers(self):
        """Standard headers for API calls."""
        return {
            "content-type": "text/plain;charset=UTF-8",
            "authorization": f"Bearer {self._access_token}",
        }

    async def execute_image_job(self, job):
        """Execute an image generation job via pure HTTP."""
        prompt = job.get("prompt", "")
        model = job.get("model", "imagen-3.0-generate-002")
        aspect_ratio = job.get("aspect_ratio", "IMAGE_ASPECT_RATIO_LANDSCAPE")
        seed = job.get("seed") or int(time.time() * 1000) % 999999

        if not self._access_token:
            if not await self._refresh_auth():
                return {"success": False, "error": "No auth token"}

        # Get reCAPTCHA token
        recap_token = await self.token_server.get_recaptcha_token("IMAGE_GENERATION")
        recap_context = None
        if recap_token:
            recap_context = {
                "token": recap_token,
                "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
            }

        project_id = self._project_id or "default"
        batch_id = str(uuid.uuid4())
        session_id = f";{int(time.time() * 1000)}"

        client_context = {
            "projectId": project_id,
            "tool": "PINHOLE",
            "sessionId": session_id,
        }
        if recap_context:
            client_context["recaptchaContext"] = recap_context

        payload = {
            "clientContext": client_context,
            "mediaGenerationContext": {"batchId": batch_id},
            "useNewMedia": True,
            "requests": [
                {
                    "clientContext": client_context,
                    "imageModelName": model,
                    "imageAspectRatio": aspect_ratio,
                    "structuredPrompt": {"parts": [{"text": prompt}]},
                    "seed": seed,
                }
            ],
        }

        url = IMAGE_GENERATE_URL.format(project_id=project_id)

        try:
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: self._session.post(
                url,
                data=json.dumps(payload),
                headers=self._api_headers(),
                timeout=60,
            ))

            if resp.status_code == 401:
                self._log(f"[{self.slot_label}] Auth expired, refreshing...")
                await self._refresh_auth()
                resp = await loop.run_in_executor(None, lambda: self._session.post(
                    url,
                    data=json.dumps(payload),
                    headers=self._api_headers(),
                    timeout=60,
                ))

            if resp.status_code != 200:
                return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:100]}"}

            result = resp.json()
            media_list = result.get("media", []) or result.get("workflows", [])

            if not media_list:
                return {"success": False, "error": "No media in response"}

            self._log(f"[{self.slot_label}] Image generated: {len(media_list)} result(s).")
            return {"success": True, "media": media_list, "raw": result}

        except Exception as e:
            return {"success": False, "error": str(e)[:100]}

    async def execute_video_job(self, job):
        """Execute a video generation job via pure HTTP."""
        prompt = job.get("prompt", "")
        model = job.get("model", "veo-2.0-generate-001")
        aspect_ratio = job.get("aspect_ratio", "ASPECT_RATIO_16_9")
        seed = job.get("seed") or int(time.time() * 1000) % 999999

        if not self._access_token:
            if not await self._refresh_auth():
                return {"success": False, "error": "No auth token"}

        # Get reCAPTCHA token
        recap_token = await self.token_server.get_recaptcha_token("VIDEO_GENERATION")
        recap_context = None
        if recap_token:
            recap_context = {
                "token": recap_token,
                "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
            }

        project_id = self._project_id or "default"
        batch_id = str(uuid.uuid4())
        session_id = f";{int(time.time() * 1000)}"

        client_context = {
            "projectId": project_id,
            "tool": "PINHOLE",
            "userPaygateTier": "PAYGATE_TIER_TWO",
            "sessionId": session_id,
        }
        if recap_context:
            client_context["recaptchaContext"] = recap_context

        payload = {
            "mediaGenerationContext": {"batchId": batch_id},
            "clientContext": client_context,
            "requests": [
                {
                    "aspectRatio": aspect_ratio,
                    "seed": seed,
                    "textInput": {
                        "structuredPrompt": {"parts": [{"text": prompt}]},
                    },
                    "videoModelKey": model,
                    "metadata": {},
                }
            ],
            "useV2ModelConfig": True,
        }

        url = VIDEO_TEXT_URL

        try:
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: self._session.post(
                url,
                data=json.dumps(payload),
                headers=self._api_headers(),
                timeout=60,
            ))

            if resp.status_code == 401:
                await self._refresh_auth()
                resp = await loop.run_in_executor(None, lambda: self._session.post(
                    url,
                    data=json.dumps(payload),
                    headers=self._api_headers(),
                    timeout=60,
                ))

            if resp.status_code != 200:
                return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:100]}"}

            result = resp.json()
            media_list = result.get("media", [])

            if not media_list:
                return {"success": False, "error": "No media in response"}

            # Poll for completion
            media_ids = [m.get("name") for m in media_list if m.get("name")]
            if media_ids:
                final = await self._poll_video_status(media_ids, project_id)
                return final

            return {"success": True, "media": media_list, "raw": result}

        except Exception as e:
            return {"success": False, "error": str(e)[:100]}

    async def _poll_video_status(self, media_ids, project_id, max_polls=60):
        """Poll video generation status until complete."""
        payload = {
            "media": [{"name": mid, "projectId": project_id} for mid in media_ids],
        }

        for poll in range(max_polls):
            await asyncio.sleep(5)

            try:
                loop = asyncio.get_running_loop()
                resp = await loop.run_in_executor(None, lambda: self._session.post(
                    VIDEO_STATUS_URL,
                    data=json.dumps(payload),
                    headers=self._api_headers(),
                    timeout=30,
                ))

                if resp.status_code != 200:
                    continue

                result = resp.json()
                statuses = result.get("media", [])

                all_done = True
                for s in statuses:
                    status = s.get("mediaGenerationStatus", "")
                    if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                        continue
                    elif status == "MEDIA_GENERATION_STATUS_FAILED":
                        error = s.get("failureReason", "Unknown error")
                        return {"success": False, "error": f"Video failed: {error}"}
                    else:
                        all_done = False

                if all_done:
                    self._log(f"[{self.slot_label}] Video generation complete after {(poll+1)*5}s.")
                    return {"success": True, "media": statuses, "raw": result}

            except Exception:
                continue

        return {"success": False, "error": "Video generation timed out (300s)"}

    async def download_media(self, media_id, output_path):
        """Download generated media by ID."""
        url = f"{MEDIA_DOWNLOAD_URL}?name={media_id}"
        try:
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: self._session.get(
                url,
                headers={"authorization": f"Bearer {self._access_token}"},
                timeout=120,
                allow_redirects=True,
            ))
            if resp.status_code == 200:
                with open(output_path, "wb") as f:
                    f.write(resp.content)
                return True
        except Exception as e:
            self._log(f"[{self.slot_label}] Download failed: {str(e)[:50]}")
        return False


class HttpModeManager:
    """
    Orchestrates the HTTP + Token Server mode.
    Creates 1 TokenServer per account, N HttpApiWorkers per account.
    Dispatches jobs to workers via pure HTTP.
    """

    def __init__(self, queue_manager):
        self.qm = queue_manager
        self._log = lambda msg: queue_manager.signals.log_msg.emit(msg)
        self._token_servers = {}  # account_name -> TokenServer
        self._workers = {}  # slot_label -> HttpApiWorker
        self._active_tasks = []

    async def run(self):
        """Main entry point — start token servers, dispatch jobs."""
        from playwright.async_api import async_playwright

        all_accs = get_accounts()
        if not all_accs:
            self._log("[HTTP] No accounts configured.")
            return

        slots_per_account = max(1, min(30, get_int_setting("slots_per_account", 3)))
        self._log(f"[HTTP] Starting with {len(all_accs)} account(s), {slots_per_account} slot(s) each.")
        self._log(f"[HTTP] Total workers: {len(all_accs) * slots_per_account} "
                  f"(RAM: ~{len(all_accs) * 300 + len(all_accs) * slots_per_account * 5}MB)")

        async with async_playwright() as p:
            try:
                # Start token servers (1 per account)
                for acc in all_accs:
                    name = acc.get("name", acc.get("email", "unknown"))
                    session_path = acc.get("session_path", os.path.join(DATA_DIR, name))

                    ts = TokenServer(name, session_path, logger=self._log)
                    await ts.start(p)
                    self._token_servers[name] = ts
                    self._log(f"[HTTP] Token server started for {name}.")

                    # Create HTTP workers for this account
                    for slot_idx in range(slots_per_account):
                        slot_label = f"{name}:slot{slot_idx + 1}"
                        worker = HttpApiWorker(slot_label, name, ts, logger=self._log)
                        await worker.initialize()
                        self._workers[slot_label] = worker

                self._log(f"[HTTP] All {len(self._workers)} workers ready. Processing queue...")

                # Main job dispatch loop
                while self.qm.is_running:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        break
                    if self.qm.pause_requested:
                        await asyncio.sleep(1)
                        continue

                    # Prune finished tasks
                    self._active_tasks = [t for t in self._active_tasks if not t.done()]

                    # Find available workers
                    busy_slots = {t.get_name() for t in self._active_tasks if hasattr(t, "get_name")}
                    available = [label for label in self._workers if label not in busy_slots]

                    if available:
                        # Get pending jobs
                        pending = get_pending_jobs(limit=len(available))
                        for job, slot_label in zip(pending, available):
                            worker = self._workers[slot_label]
                            task = asyncio.create_task(
                                self._run_job(worker, job),
                                name=slot_label,
                            )
                            self._active_tasks.append(task)

                    await asyncio.sleep(self.qm.scheduler_poll_seconds)

                # Wait for active tasks
                if self._active_tasks:
                    self._log(f"[HTTP] Waiting for {len(self._active_tasks)} active job(s)...")
                    await asyncio.gather(*self._active_tasks, return_exceptions=True)

            finally:
                # Stop all token servers
                for name, ts in self._token_servers.items():
                    try:
                        await ts.stop()
                        self._log(f"[HTTP] Token server stopped for {name}.")
                    except Exception:
                        pass
                self._token_servers.clear()
                self._workers.clear()

    async def _run_job(self, worker, job):
        """Execute a single job on an HTTP worker."""
        job_id = job.get("id", "?")
        job_type = job.get("type", "image")
        prompt_preview = (job.get("prompt") or "")[:40]

        self._log(f"[{worker.slot_label}] Starting job #{job_id}: {prompt_preview}...")
        update_job_status(job_id, "running")

        try:
            if "video" in job_type.lower():
                result = await worker.execute_video_job(job)
            else:
                result = await worker.execute_image_job(job)

            if result.get("success"):
                self._log(f"[{worker.slot_label}] Job #{job_id} completed successfully.")
                update_job_status(job_id, "completed")
                self.qm.signals.job_completed.emit(job_id)
            else:
                error = result.get("error", "Unknown error")
                self._log(f"[{worker.slot_label}] Job #{job_id} failed: {error}")
                update_job_status(job_id, "failed")
                self.qm.signals.job_failed.emit(job_id, error)

        except Exception as e:
            self._log(f"[{worker.slot_label}] Job #{job_id} exception: {str(e)[:80]}")
            update_job_status(job_id, "failed")
            self.qm.signals.job_failed.emit(job_id, str(e)[:80])
