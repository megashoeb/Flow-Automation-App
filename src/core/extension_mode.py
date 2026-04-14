"""
Extension Mode — Chrome Extension + Direct API calls.

Architecture:
  Chrome Extension → reCAPTCHA token + auth session
  Python (aiohttp) → Direct API calls to Google Labs
  NO browser launched by Python. Zero CDP. Real Chrome.

RAM: ~50MB (just Python HTTP calls, no browser process)
Speed: Same as HTTP Shared (~20-40 threads possible)
reCAPTCHA: Best possible (real Chrome, world: "MAIN", zero detection)
"""

import asyncio
import json
import os
import random
import time
import uuid
from typing import Optional, Dict, Any

try:
    import aiohttp
except ImportError:
    aiohttp = None

from src.core.extension_bridge import ExtensionBridge
from src.db.db_manager import (
    get_accounts,
    get_all_jobs,
    get_setting,
    get_int_setting,
    get_output_directory,
    update_job_status,
    update_job_runtime_state,
)

# Google Labs API endpoints
IMAGE_API_URL = "https://aisandbox-pa.googleapis.com/v1/projects/{project_id}/flowMedia:batchGenerateImages"
VIDEO_API_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText"
AUTH_SESSION_URL = "https://labs.google/fx/api/auth/session"
PROJECT_CREATE_URL = "https://aisandbox-pa.googleapis.com/v1/projects"


def _resolve_image_model(model_name):
    """Map UI model name to API model identifier."""
    lower = str(model_name or "").strip().lower()
    # Nano Banana models (must check "pro" first — it also contains "nano banana")
    if "nano banana pro" in lower:
        return "GEM_PIX_2"
    if "nano banana" in lower:
        return "NARWHAL"
    # ALL Imagen models (including Imagen 4) map to NARWHAL — same as http_mode
    if "imagen" in lower:
        return "NARWHAL"
    # Pass through already-resolved uppercase identifiers
    if model_name and model_name == model_name.upper():
        return model_name
    return "NARWHAL"


def _resolve_image_ratio(ratio_name):
    """Map UI ratio to API ratio identifier."""
    raw = str(ratio_name or "").strip()
    # Pass through already-resolved identifiers
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
    """Map UI video quality name to API video model key."""
    source = str(video_model or model or "").strip().lower()
    if not source:
        return "veo_3_1_t2v_fast_ultra"
    if "lite" in source:
        return "veo_3_1_t2v_lite"
    if "lower pri" in source or "relaxed" in source:
        return "veo_3_1_t2v_fast_ultra_relaxed"
    if "quality" in source:
        return "veo_3_1_t2v"
    # Pass through already-resolved keys (contain underscores)
    if "_" in source:
        return source
    return "veo_3_1_t2v_fast_ultra"


def _resolve_video_ratio(ratio_name):
    """Map UI ratio to API video ratio identifier."""
    raw = str(ratio_name or "").strip()
    # Pass through already-resolved identifiers
    if raw.startswith("VIDEO_ASPECT_RATIO_"):
        return raw
    # Convert IMAGE_ prefix to VIDEO_
    if raw.startswith("IMAGE_ASPECT_RATIO_"):
        return raw.replace("IMAGE_", "VIDEO_", 1)
    lower = raw.lower()
    if "portrait" in lower or "9:16" in lower:
        return "VIDEO_ASPECT_RATIO_PORTRAIT"
    if "square" in lower or "1:1" in lower:
        return "VIDEO_ASPECT_RATIO_SQUARE"
    return "VIDEO_ASPECT_RATIO_LANDSCAPE"


class ExtensionWorker:
    """A lightweight worker that uses the bridge for tokens and makes direct API calls."""

    def __init__(self, slot_id: str, account_email: str, bridge: ExtensionBridge, log_fn):
        self.slot_id = slot_id
        self.account_email = account_email
        self._bridge = bridge
        self._log = log_fn
        self.is_busy = False
        self.jobs_completed = 0

    async def generate_image(self, prompt, model, ratio, references=None):
        """Generate image via direct API call (token from extension)."""
        self.is_busy = True
        try:
            api_model = _resolve_image_model(model)
            api_ratio = _resolve_image_ratio(ratio)
            seed = random.randint(100000, 999999)
            batch_id = str(uuid.uuid4())
            prompt_text = prompt if prompt.endswith("\n") else f"{prompt}\n"

            self._log(f"[{self.slot_id}] Image: {api_model}, {api_ratio}")

            # Get token + auth from extension via bridge
            bridge_result = await self._bridge.request_token(
                self.account_email, "IMAGE_GENERATION", timeout=30
            )

            if bridge_result.get("error"):
                return None, f"Bridge error: {bridge_result['error']}"

            token = bridge_result.get("token")
            access_token = bridge_result.get("access_token")
            project_id = bridge_result.get("project_id") or self._bridge.get_project_id(self.account_email)

            if not access_token:
                return None, "No access token from extension"

            # If no project ID, try to create one
            if not project_id:
                project_id = await self._create_project(access_token)
                if project_id:
                    self._bridge.set_project_id(self.account_email, project_id)

            if not project_id:
                return None, "No project ID available"

            # Build request body
            client_context = {
                "projectId": project_id,
                "tool": "PINHOLE",
                "sessionId": f";{int(time.time() * 1000)}",
            }
            if token:
                client_context["recaptchaContext"] = {
                    "token": token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                }

            body = {
                "clientContext": client_context,
                "mediaGenerationContext": {"batchId": batch_id},
                "useNewMedia": True,
                "requests": [{
                    "clientContext": client_context,
                    "imageModelName": api_model,
                    "imageAspectRatio": api_ratio,
                    "structuredPrompt": {"parts": [{"text": prompt_text}]},
                    "seed": seed,
                    "imageInputs": (
                        [{"imageInputType": "IMAGE_INPUT_TYPE_REFERENCE", "name": ref_id}
                         for ref_id in references]
                        if references else []
                    ),
                }],
            }

            # Direct API call from Python
            url = IMAGE_API_URL.format(project_id=project_id)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers={
                        "content-type": "text/plain;charset=UTF-8",
                        "authorization": f"Bearer {access_token}",
                    },
                    data=json.dumps(body),
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    resp_text = await resp.text()

                    if not resp.ok:
                        return None, f"HTTP {resp.status}: {resp_text[:300]}"

                    try:
                        data = json.loads(resp_text)
                    except json.JSONDecodeError:
                        return None, f"Invalid JSON response: {resp_text[:200]}"

            self.jobs_completed += 1
            return data, None

        except Exception as e:
            return None, f"Exception: {str(e)[:300]}"
        finally:
            self.is_busy = False

    async def generate_video(self, prompt, model, ratio):
        """Generate video via direct API call."""
        self.is_busy = True
        try:
            api_model = _resolve_video_model(model)
            api_ratio = _resolve_video_ratio(ratio)
            seed = random.randint(100000, 999999)
            batch_id = str(uuid.uuid4())

            self._log(f"[{self.slot_id}] Video: {api_model}, {api_ratio}")

            # Get token + auth from extension
            bridge_result = await self._bridge.request_token(
                self.account_email, "VIDEO_GENERATION", timeout=30
            )

            if bridge_result.get("error"):
                return None, f"Bridge error: {bridge_result['error']}"

            token = bridge_result.get("token")
            access_token = bridge_result.get("access_token")
            project_id = bridge_result.get("project_id") or self._bridge.get_project_id(self.account_email)

            if not access_token:
                return None, "No access token from extension"

            # Build request body
            client_context = {
                "projectId": project_id or "",
                "tool": "PINHOLE",
                "userPaygateTier": "PAYGATE_TIER_TWO",
                "sessionId": f";{int(time.time() * 1000)}",
            }
            if token:
                client_context["recaptchaContext"] = {
                    "token": token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                }

            body = {
                "mediaGenerationContext": {"batchId": batch_id},
                "clientContext": client_context,
                "requests": [{
                    "clientContext": client_context,
                    "aspectRatio": api_ratio,
                    "seed": seed,
                    "textInput": {
                        "structuredPrompt": {"parts": [{"text": prompt}]},
                    },
                    "videoModelKey": api_model,
                    "metadata": {},
                }],
                "useV2ModelConfig": True,
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    VIDEO_API_URL,
                    headers={
                        "content-type": "text/plain;charset=UTF-8",
                        "authorization": f"Bearer {access_token}",
                    },
                    data=json.dumps(body),
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    resp_text = await resp.text()

                    if not resp.ok:
                        return None, f"HTTP {resp.status}: {resp_text[:300]}"

                    try:
                        data = json.loads(resp_text)
                    except json.JSONDecodeError:
                        return None, f"Invalid JSON: {resp_text[:200]}"

            self.jobs_completed += 1
            return data, None

        except Exception as e:
            return None, f"Exception: {str(e)[:300]}"
        finally:
            self.is_busy = False

    async def _create_project(self, access_token):
        """Create a new Labs project via API."""
        try:
            async with aiohttp.ClientSession() as session:
                # Navigate to Labs page to get a project
                async with session.get(
                    "https://labs.google/fx/api/trpc/backbone.listFlows",
                    headers={"authorization": f"Bearer {access_token}"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.ok:
                        data = await resp.json()
                        # Try to extract an existing project ID
                        flows = data.get("result", {}).get("data", {}).get("flows", [])
                        if flows:
                            pid = flows[0].get("name", "")
                            if pid:
                                self._log(f"[{self.slot_id}] Using existing project: {pid}")
                                return pid
        except Exception:
            pass

        # Ask extension to click "New project"
        self._bridge.send_command("new_project", self.account_email)
        await asyncio.sleep(5)

        # Check if extension reported project ID
        return self._bridge.get_project_id(self.account_email)


class ExtensionModeManager:
    """
    Manages Chrome Extension mode — same pattern as HttpModeManager.
    Receives AsyncQueueManager instance for signals, settings, etc.
    """

    def __init__(self, queue_manager):
        self.qm = queue_manager
        self._log = lambda msg: queue_manager.signals.log_msg.emit(msg)
        self._bridge = ExtensionBridge(self._log)
        self._workers: Dict[str, list] = {}  # account_email -> [ExtensionWorker, ...]
        self._active_tasks = []

    async def run(self):
        """Main entry — start bridge, wait for extension, dispatch jobs."""
        if aiohttp is None:
            self._log("[ExtMode] ERROR: aiohttp not installed. Run: pip install aiohttp")
            return

        # Start bridge server
        await self._bridge.start()

        try:
            slots_per_account = max(1, min(40, get_int_setting("slots_per_account", 5)))

            self._log(
                "[ExtMode] Chrome Extension mode — waiting for extension to connect...\n"
                "  Make sure Chrome is open with G-Labs Helper extension\n"
                "  and labs.google.com tabs are logged in."
            )

            # Wait for extension to connect (max 60 seconds)
            wait_start = time.time()
            while not self._bridge.is_extension_connected:
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    return
                if time.time() - wait_start > 60:
                    self._log("[ExtMode] Extension not connected after 60s. Aborting.")
                    return
                await asyncio.sleep(1)

            self._log("[ExtMode] Extension connected!")

            # Wait for all account reports to arrive.
            # Each Chrome profile reports separately — wait until count stabilizes.
            await asyncio.sleep(3)
            connected = self._bridge.get_connected_accounts()
            prev_count = len(connected)

            # Wait up to 15s for more accounts to trickle in
            stable_rounds = 0
            for _ in range(15):
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    return
                await asyncio.sleep(1)
                connected = self._bridge.get_connected_accounts()
                if len(connected) == prev_count and prev_count > 0:
                    stable_rounds += 1
                    if stable_rounds >= 3:
                        break  # count stable for 3s — all profiles reported
                else:
                    stable_rounds = 0
                    prev_count = len(connected)

            if not connected:
                self._log(
                    "[ExtMode] No accounts detected by extension.\n"
                    "  Open labs.google/fx/tools/flow in Chrome and login with Google account."
                )
                # Keep waiting for accounts (max 60 more seconds)
                wait_start = time.time()
                while not connected:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        return
                    if time.time() - wait_start > 60:
                        self._log("[ExtMode] No accounts found. Aborting.")
                        return
                    await asyncio.sleep(3)
                    connected = self._bridge.get_connected_accounts()

            self._log(
                f"[ExtMode] Found {len(connected)} account(s) via extension: "
                + ", ".join(connected.keys())
            )

            # Create workers for each extension-detected account
            for email, info in connected.items():
                account_name = email or info.get("name", "unknown")

                workers = []
                for idx in range(1, slots_per_account + 1):
                    slot_id = f"{account_name}#e{idx}"
                    worker = ExtensionWorker(slot_id, account_name, self._bridge, self._log)
                    workers.append(worker)

                self._workers[account_name] = workers
                self._log(f"[ExtMode] {account_name}: {len(workers)} worker(s) ready.")

            total_workers = sum(len(w) for w in self._workers.values())
            if total_workers == 0:
                self._log("[ExtMode] No workers started. Ensure accounts are logged in via Chrome Extension.")
                return

            self._log(
                f"[ExtMode] Total: {total_workers} worker(s). "
                f"RAM: ~50MB (no browser launched). "
            )

            # Main dispatch loop (same pattern as HttpModeManager)
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
                            self._log("[ExtMode] All jobs completed.")
                            break
                    await asyncio.sleep(self.qm.scheduler_poll_seconds)
                    continue

                busy_slots = {t.get_name() for t in self._active_tasks if hasattr(t, "get_name")}

                dispatched = 0
                for job in pending:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        break

                    worker = self._get_available_worker(busy_slots)
                    if not worker:
                        break

                    job_id = job["id"]
                    update_job_status(job_id, "running", account=worker.account_email)
                    self.qm.signals.job_updated.emit(job_id, "running", worker.account_email, "")

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

            # Handle remaining tasks
            if self._active_tasks:
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    self._log(f"[ExtMode] Cancelling {len(self._active_tasks)} active job(s)...")
                    for t in self._active_tasks:
                        if not t.done():
                            t.cancel()
                else:
                    self._log(f"[ExtMode] Waiting for {len(self._active_tasks)} active job(s)...")
                await asyncio.gather(*self._active_tasks, return_exceptions=True)

        finally:
            await self._bridge.stop()
            self._workers.clear()
            self._log("[ExtMode] Extension mode stopped.")

    def _get_available_worker(self, busy_slots):
        """Find an available worker across all accounts."""
        for account_email, workers in self._workers.items():
            for worker in workers:
                if worker.slot_id not in busy_slots and not worker.is_busy:
                    return worker
        return None

    async def _run_job(self, worker: ExtensionWorker, job: dict):
        """Execute a single job with retries."""
        job_id = job["id"]
        job_type = job.get("job_type", "image")
        prompt = job.get("prompt", "")
        model = job.get("model", "")
        queue_no = job.get("output_index") or job.get("queue_no")

        self._log(f"[{worker.slot_id}] Job {job_id[:6]}...: {prompt[:40]}...")

        max_retries = max(1, get_int_setting("max_auto_retries_per_job", 3))
        last_error = ""

        for attempt in range(max_retries + 1):
            if self.qm.stop_requested or self.qm.force_stop_requested:
                update_job_status(job_id, "pending", account="")
                self.qm.signals.job_updated.emit(job_id, "pending", "", "")
                return

            try:
                if "video" in job_type:
                    video_model = job.get("video_model") or model
                    ratio = job.get("aspect_ratio", "VIDEO_ASPECT_RATIO_LANDSCAPE")
                    result, error = await worker.generate_video(prompt, video_model, ratio)
                else:
                    ratio = job.get("aspect_ratio", "IMAGE_ASPECT_RATIO_LANDSCAPE")
                    result, error = await worker.generate_image(
                        prompt, model, ratio,
                        references=job.get("reference_media_ids"),
                    )

                if result and not error:
                    # Download and save
                    output_path, dl_error = await self._download_and_save(
                        worker, job_id, result, queue_no=queue_no
                    )
                    if dl_error:
                        last_error = dl_error
                        self._log(f"[{worker.slot_id}] Download failed: {dl_error[:200]}")
                        if attempt < max_retries:
                            await asyncio.sleep(5)
                            continue
                    else:
                        update_job_status(job_id, "completed", account=worker.account_email)
                        self.qm.signals.job_updated.emit(job_id, "completed", worker.account_email, "")
                        self._log(f"[{worker.slot_id}] Job {job_id[:6]}... completed! ({output_path})")
                        return

                last_error = error or "Unknown error"
                self._log(
                    f"[{worker.slot_id}] Attempt {attempt + 1}/{max_retries + 1} "
                    f"failed: {last_error[:200]}"
                )

                # reCAPTCHA failure → ask extension to refresh
                if "recaptcha" in last_error.lower() or "captcha" in last_error.lower():
                    self._bridge.send_command("clear_cookies", worker.account_email)
                    self._bridge.send_command("reload_tab", worker.account_email)
                    await asyncio.sleep(5)
                    continue

                if attempt < max_retries:
                    await asyncio.sleep(10 * (attempt + 1))

            except Exception as e:
                last_error = str(e)[:300]
                self._log(f"[{worker.slot_id}] Attempt {attempt + 1} exception: {last_error}")
                if attempt < max_retries:
                    await asyncio.sleep(10)

        update_job_status(job_id, "failed", account=worker.account_email, error=last_error)
        self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, last_error)
        self._log(f"[{worker.slot_id}] Job {job_id[:6]}... FAILED: {last_error[:200]}")

    async def _download_and_save(self, worker, job_id, api_data, queue_no=None):
        """Download generated media via direct HTTP and save to output directory."""
        # Extract media URL from API response
        media_list = api_data.get("media", []) if isinstance(api_data, dict) else []
        fife_url = None
        media_name = None

        for item in media_list:
            url = (item.get("image", {}).get("generatedImage", {}).get("fifeUrl", "")
                   if isinstance(item, dict) else "")
            name = item.get("name", "") if isinstance(item, dict) else ""
            if url:
                fife_url = url
                media_name = name
                break
            if name and not fife_url:
                media_name = name

        if not fife_url and media_name:
            fife_url = (
                f"https://labs.google/fx/api/trpc/backbone.redirect?"
                f"input=%7B%22name%22%3A%22{media_name}%22%7D"
            )

        if not fife_url:
            workflows = api_data.get("workflows", []) if isinstance(api_data, dict) else []
            if workflows:
                primary_id = workflows[0].get("metadata", {}).get("primaryMediaId", "")
                if primary_id:
                    fife_url = (
                        f"https://labs.google/fx/api/trpc/backbone.redirect?"
                        f"input=%7B%22name%22%3A%22{primary_id}%22%7D"
                    )

        if not fife_url:
            return None, "No downloadable media in API response"

        # Download via aiohttp (direct HTTP — no browser needed)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    fife_url,
                    timeout=aiohttp.ClientTimeout(total=90),
                ) as resp:
                    if not resp.ok:
                        return None, f"Download HTTP {resp.status}"

                    content_type = str(resp.headers.get("content-type", "")).lower()
                    ext_map = {
                        "image/png": ".png",
                        "image/jpeg": ".jpg",
                        "image/webp": ".webp",
                        "video/mp4": ".mp4",
                        "video/webm": ".webm",
                    }
                    ext = ".jpg"
                    for mime, candidate_ext in ext_map.items():
                        if mime in content_type:
                            ext = candidate_ext
                            break

                    data = await resp.read()
                    if not data:
                        return None, "Downloaded empty file"

            # Build output path
            output_dir = get_output_directory()
            os.makedirs(output_dir, exist_ok=True)

            normalized_qno = None
            try:
                val = int(queue_no)
                if val > 0:
                    normalized_qno = val
            except Exception:
                pass

            if normalized_qno is not None:
                filename = f"{normalized_qno}{ext}"
            else:
                safe_job = (job_id or "job").replace("-", "")[:8]
                ts = int(time.time() * 1000)
                nonce = random.randint(1000, 9999)
                filename = f"{safe_job}_{ts}_{nonce}_generation{ext}"

            output_path = os.path.join(output_dir, filename)

            with open(output_path, "wb") as f:
                f.write(data)

            try:
                update_job_runtime_state(job_id, output_path=output_path)
            except Exception:
                pass

            self._log(f"[{worker.slot_id}] Saved: {filename} ({len(data)} bytes)")
            return output_path, None

        except Exception as e:
            return None, f"Download error: {str(e)[:200]}"
