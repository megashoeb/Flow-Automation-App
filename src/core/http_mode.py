"""
HTTP Mode: Shared reCAPTCHA Token Server + Pure HTTP API Workers

Architecture:
  1 CloakBrowser per account  -> generates reCAPTCHA tokens
  N HTTP workers per account  -> pure aiohttp API calls (no browser)

RAM usage:
  1 browser  = ~300 MB (fixed per account)
  Each HTTP worker = ~5 MB
  30 workers = ~450 MB total  vs  ~9 GB with browser-per-slot
"""

import asyncio
import json
import os
import re
import time
import uuid
import random
import platform
from typing import Optional, List, Dict, Any

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    import yarl
except ImportError:
    yarl = None

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
# RecaptchaTokenServer — 1 shared browser per account for reCAPTCHA tokens
# ═══════════════════════════════════════════════════════════════════════════

class RecaptchaTokenServer:
    """
    Single shared browser per account.
    Generates reCAPTCHA tokens on demand.
    All HTTP workers share tokens from this server.
    """

    def __init__(self, account_name, session_path, cookies_json_path, log_fn):
        self.account_name = account_name
        self._session_path = session_path
        self._cookies_json = cookies_json_path
        self._log = log_fn
        self._context = None
        self._page = None
        self._token_queue = asyncio.Queue(maxsize=10)
        self._running = False
        self._token_count = 0
        self._failed_count = 0
        self._project_id = None
        self._cookies_cache = []
        self._last_page_reload = 0
        self._generator_task = None

    async def start(self, playwright_instance):
        """Start shared browser and begin pre-generating tokens."""
        self._log(f"[TokenServer:{self.account_name}] Starting shared browser...")

        try:
            success = await self._launch_browser(playwright_instance)
            if not success:
                return False

            # Navigate to Flow page
            pages = list(getattr(self._context, "pages", []) or [])
            self._page = pages[0] if pages else await self._context.new_page()

            await self._page.goto(
                "https://labs.google/fx/tools/flow",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(3)

            # Extract project ID
            self._project_id = await self._extract_project_id()
            if self._project_id:
                self._log(f"[TokenServer:{self.account_name}] Project ID: {self._project_id}")
            else:
                self._log(f"[TokenServer:{self.account_name}] No project ID found. Check login.")
                return False

            # Cache cookies for HTTP workers
            await self._refresh_cookies_cache()

            self._running = True
            self._generator_task = asyncio.create_task(self._token_generator_loop())

            self._log(
                f"[TokenServer:{self.account_name}] Ready! "
                f"Cookies: {len(self._cookies_cache)}"
            )
            return True

        except Exception as e:
            self._log(f"[TokenServer:{self.account_name}] Start failed: {str(e)[:80]}")
            return False

    async def _launch_browser(self, playwright_instance):
        """Launch CloakBrowser (preferred) or Playwright persistent context."""
        # Import cookies
        cookies = self._load_exported_cookies()

        # Try CloakBrowser first
        try:
            cloak_api = load_cloakbrowser_api()
            cloak_persistent = cloak_api.get("persistent_async")

            if cloak_api.get("available") and cloak_persistent:
                seed = random.randint(10000, 99999)

                if platform.system() == "Darwin":
                    # Mac: separate profile for token server to avoid lock conflicts
                    profile = self._session_path + "_token_server"
                    os.makedirs(profile, exist_ok=True)
                    self._clean_locks(profile)
                else:
                    profile = self._session_path
                    self._clean_locks(profile)

                self._context = await cloak_persistent(
                    profile,
                    headless=True,
                    args=[f"--fingerprint={seed}"],
                    humanize=True,
                )

                # Import cookies
                if cookies:
                    try:
                        await self._context.add_cookies(cookies)
                        self._log(
                            f"[TokenServer:{self.account_name}] "
                            f"CloakBrowser started. Imported {len(cookies)} cookies."
                        )
                    except Exception as e:
                        self._log(
                            f"[TokenServer:{self.account_name}] "
                            f"Cookie import warning: {str(e)[:50]}"
                        )
                return True
        except Exception as e:
            self._log(
                f"[TokenServer:{self.account_name}] "
                f"CloakBrowser not available ({str(e)[:40]}). Using Playwright."
            )

        # Fallback: Playwright persistent context
        try:
            self._clean_locks(self._session_path)
            self._context = await playwright_instance.chromium.launch_persistent_context(
                self._session_path,
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
                ignore_default_args=["--enable-automation"],
            )
            if cookies:
                try:
                    await self._context.add_cookies(cookies)
                except Exception:
                    pass
            self._log(f"[TokenServer:{self.account_name}] Playwright started (headless).")
            return True
        except Exception as e:
            self._log(f"[TokenServer:{self.account_name}] Browser launch failed: {str(e)[:60]}")
            return False

    async def _extract_project_id(self):
        """Extract Flow project ID from page URL or content."""
        if not self._page:
            return None

        url = self._page.url
        match = re.search(r"/flow/([a-f0-9-]{36})", url)
        if match:
            return match.group(1)

        # Try page content
        try:
            content = await self._page.content()
            match = re.search(r'"projectId"\s*:\s*"([a-f0-9-]{36})"', content)
            if match:
                return match.group(1)
        except Exception:
            pass

        # Try clicking New Project
        try:
            new_proj = self._page.locator("text='New project'")
            if await new_proj.count() > 0:
                await new_proj.first.click()
                await asyncio.sleep(3)
                url = self._page.url
                match = re.search(r"/flow/([a-f0-9-]{36})", url)
                if match:
                    return match.group(1)
        except Exception:
            pass

        return None

    async def _token_generator_loop(self):
        """Continuously pre-generate reCAPTCHA tokens in background."""
        while self._running:
            try:
                if self._token_queue.qsize() < 5:
                    token = await self._generate_recaptcha_token()
                    if token:
                        await self._token_queue.put(token)
                        self._token_count += 1
                        self._failed_count = 0
                    else:
                        self._failed_count += 1
                        if self._failed_count >= 5:
                            self._log(
                                f"[TokenServer:{self.account_name}] "
                                "5 consecutive token failures. Reloading page..."
                            )
                            await self._reload_page()
                            self._failed_count = 0
                        await asyncio.sleep(2)
                else:
                    await asyncio.sleep(1)

                # Reload page every 100 tokens
                if self._token_count > 0 and self._token_count % 100 == 0:
                    if time.time() - self._last_page_reload > 60:
                        self._log(
                            f"[TokenServer:{self.account_name}] "
                            "Refreshing page (every 100 tokens)..."
                        )
                        await self._reload_page()

                # Refresh cookies cache every 50 tokens
                if self._token_count > 0 and self._token_count % 50 == 0:
                    await self._refresh_cookies_cache()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log(f"[TokenServer:{self.account_name}] Loop error: {str(e)[:50]}")
                await asyncio.sleep(5)

    async def _generate_recaptcha_token(self):
        """Execute reCAPTCHA on shared page and return token."""
        if not self._page:
            return None
        try:
            token = await self._page.evaluate("""
                () => {
                    return new Promise((resolve, reject) => {
                        if (typeof grecaptcha === 'undefined' || !grecaptcha.enterprise) {
                            reject('grecaptcha not loaded');
                            return;
                        }
                        const scripts = document.querySelectorAll(
                            "script[src*='recaptcha'][src*='render=']"
                        );
                        let siteKey = null;
                        for (const s of scripts) {
                            const m = s.src.match(/render=([^&]+)/);
                            if (m && m[1] !== 'explicit') { siteKey = m[1]; break; }
                        }
                        if (!siteKey) { reject('no siteKey'); return; }
                        grecaptcha.enterprise.execute(siteKey, {action: 'generate'})
                            .then(resolve).catch(reject);
                    });
                }
            """)
            return token
        except Exception:
            return None

    async def _reload_page(self):
        """Reload Flow page to refresh reCAPTCHA context."""
        try:
            await self._page.goto(
                "https://labs.google/fx/tools/flow",
                wait_until="domcontentloaded",
                timeout=15000,
            )
            await asyncio.sleep(3)
            self._last_page_reload = time.time()

            # Re-extract project ID (might change after reload)
            pid = await self._extract_project_id()
            if pid:
                self._project_id = pid
        except Exception:
            pass

    async def _refresh_cookies_cache(self):
        """Refresh cached cookies from browser context."""
        try:
            if self._context:
                self._cookies_cache = await self._context.cookies()
        except Exception:
            pass

    async def get_token(self, timeout=30):
        """Get a reCAPTCHA token. Called by HTTP workers."""
        try:
            return await asyncio.wait_for(self._token_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            self._log(
                f"[TokenServer:{self.account_name}] "
                "Token queue empty. Generating on-demand..."
            )
            return await self._generate_recaptcha_token()

    def get_project_id(self):
        return self._project_id

    def get_cookies(self):
        """Get cached cookies (sync, no await)."""
        return list(self._cookies_cache)

    async def get_fresh_cookies(self):
        """Get fresh cookies from browser."""
        await self._refresh_cookies_cache()
        return list(self._cookies_cache)

    def get_stats(self):
        return {
            "tokens_generated": self._token_count,
            "tokens_queued": self._token_queue.qsize(),
            "cookies_cached": len(self._cookies_cache),
        }

    def _load_exported_cookies(self):
        """Load exported_cookies.json for this account."""
        if not os.path.exists(self._cookies_json):
            return []
        try:
            with open(self._cookies_json, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            return [c for c in cookies if c.get("name") and c.get("value") and c.get("domain")]
        except Exception:
            return []

    def _clean_locks(self, path):
        """Remove browser lock files."""
        for lock in ("SingletonLock", "SingletonCookie", "SingletonSocket", "lockfile"):
            lp = os.path.join(path, lock)
            try:
                if os.path.exists(lp) or os.path.islink(lp):
                    os.unlink(lp)
            except Exception:
                pass

    async def stop(self):
        """Stop token server and close browser."""
        self._running = False
        if self._generator_task and not self._generator_task.done():
            self._generator_task.cancel()
            try:
                await self._generator_task
            except (asyncio.CancelledError, Exception):
                pass

        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass

        self._context = None
        self._page = None
        self._log(
            f"[TokenServer:{self.account_name}] Stopped. "
            f"Generated {self._token_count} tokens total."
        )


# ═══════════════════════════════════════════════════════════════════════════
# HttpApiWorker — Pure HTTP worker, ~5 MB RAM, no browser
# ═══════════════════════════════════════════════════════════════════════════

class HttpApiWorker:
    """
    Pure HTTP worker — no browser, ~5 MB RAM.
    Gets reCAPTCHA tokens from shared TokenServer.
    Makes API calls via aiohttp.
    """

    def __init__(self, slot_id, account_name, token_server, log_fn):
        self.slot_id = slot_id
        self.account_name = account_name
        self._token_server = token_server
        self._log = log_fn
        self._session = None
        self.is_busy = False
        self.jobs_completed = 0

    async def start(self):
        """Initialize aiohttp session with cookies from token server."""
        if aiohttp is None:
            self._log(f"[{self.slot_id}] aiohttp not installed! Run: pip install aiohttp")
            return False

        cookies = self._token_server.get_cookies()

        jar = aiohttp.CookieJar(unsafe=True)
        self._session = aiohttp.ClientSession(cookie_jar=jar)

        # Load cookies into session
        for c in cookies:
            domain = (c.get("domain") or "").lstrip(".")
            if domain:
                try:
                    url_obj = yarl.URL(f"https://{domain}") if yarl else f"https://{domain}"
                    self._session.cookie_jar.update_cookies(
                        {c["name"]: c["value"]}, response_url=url_obj
                    )
                except Exception:
                    pass

        self._log(f"[{self.slot_id}] HTTP worker ready ({len(cookies)} cookies).")
        return True

    async def refresh_cookies(self):
        """Refresh cookies from token server."""
        cookies = await self._token_server.get_fresh_cookies()
        for c in cookies:
            domain = (c.get("domain") or "").lstrip(".")
            if domain:
                try:
                    url_obj = yarl.URL(f"https://{domain}") if yarl else f"https://{domain}"
                    self._session.cookie_jar.update_cookies(
                        {c["name"]: c["value"]}, response_url=url_obj
                    )
                except Exception:
                    pass

    async def generate_image(self, prompt, model, ratio, references=None):
        """Generate image via pure HTTP API call."""
        self.is_busy = True
        try:
            token = await self._token_server.get_token()
            if not token:
                return None, "Failed to get reCAPTCHA token"

            project_id = self._token_server.get_project_id()
            if not project_id:
                return None, "No project ID"

            url = (
                f"https://aisandbox-pa.googleapis.com/v1/projects/"
                f"{project_id}/flowMedia:batchGenerateImages"
            )

            seed = random.randint(100000, 999999)
            session_id = f";{int(time.time() * 1000)}"
            batch_id = str(uuid.uuid4())

            client_ctx = {
                "recaptchaContext": {
                    "token": token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                },
                "projectId": project_id,
                "tool": "PINHOLE",
                "sessionId": session_id,
            }

            request_item = {
                "clientContext": client_ctx,
                "imageModelName": model,
                "imageAspectRatio": ratio,
                "structuredPrompt": {"parts": [{"text": prompt}]},
                "seed": seed,
                "imageInputs": references or [],
            }

            payload = {
                "clientContext": client_ctx,
                "mediaGenerationContext": {"batchId": batch_id},
                "useNewMedia": True,
                "requests": [request_item],
            }

            headers = {
                "Content-Type": "text/plain;charset=UTF-8",
                "Origin": "https://labs.google",
                "Referer": "https://labs.google/",
            }

            async with self._session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.jobs_completed += 1
                    return data, None
                else:
                    text = await resp.text()
                    if "reCAPTCHA" in text:
                        return None, f"recaptcha_block: {text[:100]}"
                    if "auth" in text.lower():
                        return None, f"auth_failed: {text[:100]}"
                    return None, f"HTTP {resp.status}: {text[:100]}"

        except asyncio.TimeoutError:
            return None, "Request timeout (60s)"
        except Exception as e:
            return None, str(e)[:100]
        finally:
            self.is_busy = False

    async def generate_video(self, prompt, model, ratio):
        """Generate video via pure HTTP API call."""
        self.is_busy = True
        try:
            token = await self._token_server.get_token()
            if not token:
                return None, "Failed to get reCAPTCHA token"

            project_id = self._token_server.get_project_id()
            if not project_id:
                return None, "No project ID"

            url = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText"

            seed = random.randint(100000, 999999)
            session_id = f";{int(time.time() * 1000)}"
            batch_id = str(uuid.uuid4())

            payload = {
                "mediaGenerationContext": {"batchId": batch_id},
                "clientContext": {
                    "recaptchaContext": {
                        "token": token,
                        "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                    },
                    "projectId": project_id,
                    "tool": "PINHOLE",
                    "userPaygateTier": "PAYGATE_TIER_TWO",
                    "sessionId": session_id,
                },
                "requests": [
                    {
                        "aspectRatio": ratio,
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

            headers = {
                "Content-Type": "text/plain;charset=UTF-8",
                "Origin": "https://labs.google",
                "Referer": "https://labs.google/",
            }

            async with self._session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    media = data.get("media", [])
                    if media:
                        media_ids = [m.get("name") for m in media if m.get("name")]
                        if media_ids:
                            return await self._poll_video_status(media_ids, project_id), None
                    self.jobs_completed += 1
                    return data, None
                else:
                    text = await resp.text()
                    return None, f"HTTP {resp.status}: {text[:100]}"

        except asyncio.TimeoutError:
            return None, "Request timeout (60s)"
        except Exception as e:
            return None, str(e)[:100]
        finally:
            self.is_busy = False

    async def _poll_video_status(self, media_ids, project_id, max_polls=60):
        """Poll video status until complete (up to 300s)."""
        url = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus"
        payload = {
            "media": [{"name": mid, "projectId": project_id} for mid in media_ids],
        }
        headers = {
            "Content-Type": "text/plain;charset=UTF-8",
            "Origin": "https://labs.google",
            "Referer": "https://labs.google/",
        }

        for poll in range(max_polls):
            await asyncio.sleep(5)
            try:
                async with self._session.post(
                    url, json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        continue
                    result = await resp.json()
                    statuses = result.get("media", [])

                    all_done = True
                    for s in statuses:
                        status = s.get("mediaGenerationStatus", "")
                        if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                            continue
                        elif status == "MEDIA_GENERATION_STATUS_FAILED":
                            reason = s.get("failureReason", "Unknown")
                            return {"success": False, "error": f"Video failed: {reason}"}
                        else:
                            all_done = False

                    if all_done:
                        self._log(f"[{self.slot_id}] Video complete after {(poll + 1) * 5}s.")
                        self.jobs_completed += 1
                        return result
            except Exception:
                continue

        return {"success": False, "error": "Video timed out (300s)"}

    async def download_file(self, file_url, save_path):
        """Download generated image/video file."""
        try:
            async with self._session.get(
                file_url, timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status == 200:
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    content = await resp.read()
                    if aiofiles:
                        async with aiofiles.open(save_path, "wb") as f:
                            await f.write(content)
                    else:
                        with open(save_path, "wb") as f:
                            f.write(content)
                    return True
        except Exception:
            pass
        return False

    async def close(self):
        """Close HTTP session."""
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None


# ═══════════════════════════════════════════════════════════════════════════
# HttpModeManager — Orchestrates token servers + HTTP workers
# ═══════════════════════════════════════════════════════════════════════════

class HttpModeManager:
    """
    Manages token servers and HTTP workers for all accounts.
    Receives the AsyncQueueManager instance to access signals, settings, etc.
    """

    def __init__(self, queue_manager):
        self.qm = queue_manager
        self._log = lambda msg: queue_manager.signals.log_msg.emit(msg)
        self._token_servers = {}   # account_name -> RecaptchaTokenServer
        self._workers = {}         # account_name -> [HttpApiWorker, ...]
        self._active_tasks = []

    async def run(self):
        """Main entry — start servers, dispatch jobs, wait for completion."""
        from playwright.async_api import async_playwright

        all_accs = get_accounts()
        if not all_accs:
            self._log("[HTTP] No accounts configured.")
            return

        slots_per_account = max(1, min(30, get_int_setting("slots_per_account", 3)))
        self._log(
            f"[HTTP] Starting: {len(all_accs)} account(s), "
            f"{slots_per_account} HTTP slot(s) each."
        )

        async with async_playwright() as p:
            try:
                # ── Start token servers (1 per account) ──
                for acc in all_accs:
                    name = acc.get("name", "unknown")
                    session_path = acc.get("session_path", os.path.join(DATA_DIR, name))
                    cookies_json = os.path.join(session_path, "exported_cookies.json")

                    ts = RecaptchaTokenServer(name, session_path, cookies_json, self._log)
                    success = await ts.start(p)

                    if not success:
                        self._log(f"[HTTP] {name}: Token server failed! Account skipped.")
                        continue

                    self._token_servers[name] = ts

                    # ── Create HTTP workers for this account ──
                    account_workers = []
                    for idx in range(1, slots_per_account + 1):
                        slot_id = f"{name}#h{idx}"
                        worker = HttpApiWorker(slot_id, name, ts, self._log)
                        ok = await worker.start()
                        if ok:
                            account_workers.append(worker)

                    self._workers[name] = account_workers
                    self._log(
                        f"[HTTP] {name}: 1 token server + "
                        f"{len(account_workers)} HTTP workers ready."
                    )

                total_workers = sum(len(w) for w in self._workers.values())
                total_servers = len(self._token_servers)

                if total_workers == 0:
                    self._log("[HTTP] No workers started. Cannot proceed.")
                    return

                est_ram = total_servers * 300 + total_workers * 5
                self._log(
                    f"[HTTP] Total: {total_servers} token server(s), "
                    f"{total_workers} HTTP worker(s). Est. RAM: ~{est_ram} MB."
                )

                # ── Main dispatch loop ──
                while self.qm.is_running:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        break
                    if self.qm.pause_requested:
                        await asyncio.sleep(1)
                        continue

                    # Prune finished tasks
                    self._active_tasks = [t for t in self._active_tasks if not t.done()]

                    # Get pending jobs
                    jobs = get_all_jobs()
                    pending = [j for j in jobs if j["status"] == "pending"]

                    if not pending:
                        # Check if any tasks still running
                        if not self._active_tasks:
                            still_pending = any(
                                j["status"] in ("pending", "running")
                                for j in get_all_jobs()
                            )
                            if not still_pending:
                                self._log("[HTTP] All jobs completed.")
                                break
                        await asyncio.sleep(self.qm.scheduler_poll_seconds)
                        continue

                    # Find available workers
                    busy_slots = set()
                    for t in self._active_tasks:
                        name_attr = t.get_name() if hasattr(t, "get_name") else ""
                        if name_attr:
                            busy_slots.add(name_attr)

                    dispatched = 0
                    for job in pending:
                        worker = self._get_available_worker(busy_slots)
                        if not worker:
                            break

                        job_id = job["id"]
                        update_job_status(job_id, "running", account=worker.account_name)
                        self.qm.signals.job_updated.emit(
                            job_id, "running", worker.account_name, ""
                        )

                        task = asyncio.create_task(
                            self._run_job(worker, job),
                            name=worker.slot_id,
                        )
                        self._active_tasks.append(task)
                        busy_slots.add(worker.slot_id)
                        dispatched += 1

                        # Stagger between dispatches
                        stagger = random.uniform(
                            self.qm.global_stagger_min_seconds,
                            self.qm.global_stagger_max_seconds,
                        )
                        if stagger > 0:
                            await asyncio.sleep(stagger)

                    if dispatched == 0:
                        await asyncio.sleep(self.qm.scheduler_poll_seconds)

                # ── Wait for active tasks ──
                if self._active_tasks:
                    self._log(f"[HTTP] Waiting for {len(self._active_tasks)} active job(s)...")
                    await asyncio.gather(*self._active_tasks, return_exceptions=True)

            finally:
                # ── Cleanup ──
                for workers in self._workers.values():
                    for w in workers:
                        await w.close()
                for ts in self._token_servers.values():
                    await ts.stop()
                self._workers.clear()
                self._token_servers.clear()
                self._log("[HTTP] All token servers and workers stopped.")

    def _get_available_worker(self, busy_slots):
        """Get next available (not busy) HTTP worker."""
        for account_name, workers in self._workers.items():
            for worker in workers:
                if worker.slot_id not in busy_slots and not worker.is_busy:
                    return worker
        return None

    async def _run_job(self, worker, job):
        """Execute a single job on an HTTP worker with retries."""
        job_id = job["id"]
        job_type = job.get("job_type", "image")
        prompt = job.get("prompt", "")
        model = job.get("model", "")
        prompt_preview = prompt[:40]

        self._log(f"[{worker.slot_id}] Processing job {job_id[:6]}...: {prompt_preview}...")

        max_retries = max(1, get_int_setting("max_auto_retries_per_job", 3))
        last_error = ""

        for attempt in range(max_retries + 1):
            try:
                if "video" in job_type:
                    video_model = job.get("video_model") or model or "veo-2.0-generate-001"
                    ratio = job.get("aspect_ratio", "ASPECT_RATIO_16_9")
                    result, error = await worker.generate_video(prompt, video_model, ratio)
                else:
                    img_model = model or "imagen-3.0-generate-002"
                    ratio = job.get("aspect_ratio", "IMAGE_ASPECT_RATIO_LANDSCAPE")
                    result, error = await worker.generate_image(prompt, img_model, ratio)

                if result and not error:
                    update_job_status(job_id, "completed", account=worker.account_name)
                    self.qm.signals.job_updated.emit(
                        job_id, "completed", worker.account_name, ""
                    )
                    self._log(f"[{worker.slot_id}] Job {job_id[:6]}... completed!")
                    return

                last_error = error or "Unknown error"
                self._log(
                    f"[{worker.slot_id}] Attempt {attempt + 1}/{max_retries + 1} "
                    f"failed: {last_error[:60]}"
                )

                # Refresh cookies on auth failures
                if "auth" in last_error.lower():
                    await worker.refresh_cookies()

                if attempt < max_retries:
                    delay = 10 * (attempt + 1)
                    await asyncio.sleep(delay)

            except Exception as e:
                last_error = str(e)[:100]
                self._log(f"[{worker.slot_id}] Attempt {attempt + 1} exception: {last_error}")
                if attempt < max_retries:
                    await asyncio.sleep(10)

        # All retries exhausted
        update_job_status(job_id, "failed", account=worker.account_name, error=last_error)
        self.qm.signals.job_updated.emit(job_id, "failed", worker.account_name, last_error)
        self._log(f"[{worker.slot_id}] Job {job_id[:6]}... FAILED: {last_error[:60]}")
