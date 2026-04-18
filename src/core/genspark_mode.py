"""
Genspark Mode — Chrome Extension + Genspark.ai direct API calls.

Parallel architecture to extension_mode.py (Flow), but for genspark.ai:
  - Separate HTTP bridge on port 18925
  - Cookies-only auth (no Bearer token dance)
  - Unlimited generation on Plus/Pro plans (so no per-day quota hustle)
  - LLM agent wraps prompts before calling Nano Banana

This file deliberately does NOT import from extension_mode.py — the two
modes are isolated so a change to Flow never breaks Genspark and vice versa.
The queue_manager picks which one to run based on the `generation_mode`
setting.
"""

import asyncio
import base64
import os
import random
import re
import time
import uuid
from typing import Any, Dict, List, Optional

from src.core.genspark_bridge import GensparkBridge
from src.db.db_manager import (
    get_all_jobs,
    get_int_setting,
    get_output_directory,
    update_job_status,
)


# ─── Aspect-ratio mapping (app format → Genspark format) ───
# Genspark accepts "16:9", "9:16", "1:1", "4:3", "3:4", "3:2", "2:3",
# "5:4", "4:5", "21:9", or "auto".
def _resolve_aspect_ratio(ratio_name: str) -> str:
    raw = str(ratio_name or "").strip()
    low = raw.lower()
    if not raw:
        return "auto"
    if raw in {"auto", "16:9", "9:16", "1:1", "4:3", "3:4", "3:2", "2:3",
              "5:4", "4:5", "21:9"}:
        return raw
    # Map IMAGE_ASPECT_RATIO_* identifiers used internally
    if "portrait" in low or "9:16" in low:
        return "9:16"
    if "square" in low or "1:1" in low:
        return "1:1"
    if "4:3" in low:
        return "4:3"
    if "3:4" in low:
        return "3:4"
    if "landscape" in low:
        return "16:9"
    return "auto"


def _resolve_model(model_name: str) -> str:
    """Map UI model name → Genspark model key.

    Genspark currently exposes:
      - nano-banana-2          (2K, Plus plan unlimited)
      - nano-banana-pro        (4K, Pro plan unlimited)
    """
    low = str(model_name or "").strip().lower()
    if "pro" in low and "nano" in low:
        return "nano-banana-pro"
    if "nano" in low:
        return "nano-banana-2"
    # Fallback to nano-banana-2 for anything else
    return "nano-banana-2"


class GensparkWorker:
    """A worker slot for a single Genspark account. Multiple workers per
    account allow parallel generation (respecting session rate limits).
    """

    def __init__(self, slot_id: str, account_email: str, bridge: GensparkBridge, log_fn):
        self.slot_id = slot_id
        self.account_email = account_email
        self._bridge = bridge
        self._log = log_fn
        self.is_busy = False


class GensparkModeManager:
    """
    Genspark automation mode — uses the Chrome Extension + a dedicated local
    bridge (GensparkBridge) to drive genspark.ai directly.

    Mirrors ExtensionModeManager's shape but is fully independent.
    """

    def __init__(self, queue_manager):
        self.qm = queue_manager
        self._log = lambda msg: queue_manager.signals.log_msg.emit(msg)
        self._bridge = GensparkBridge(self._log)
        self._workers: Dict[str, List[GensparkWorker]] = {}  # email -> [workers]
        self._active_tasks: List[asyncio.Task] = []

    # ═══════════════════════════════════════════════════════════════
    # Main loop
    # ═══════════════════════════════════════════════════════════════

    async def run(self) -> None:
        self._log("[GensparkMode] Starting Genspark automation mode...")
        await self._bridge.start()

        try:
            slots_per_account = max(
                1, min(20, get_int_setting("genspark_slots_per_account", 3))
            )

            self._log(
                "[GensparkMode] Waiting for Chrome Extension to connect...\n"
                "  Make sure Chrome is open with G-Labs Helper extension\n"
                "  and genspark.ai is logged in (Plus or Pro plan recommended)."
            )

            # Wait for first account detection (up to 60s)
            wait_start = time.time()
            while not self._bridge.is_extension_connected:
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    return
                if time.time() - wait_start > 60:
                    self._log("[GensparkMode] Extension did not connect. Aborting.")
                    return
                await asyncio.sleep(1)

            # Give extension 4s to report accounts
            await asyncio.sleep(4)
            connected = self._bridge.get_connected_accounts()
            prev_count = len(connected)
            stable_rounds = 0
            for _ in range(20):
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    return
                await asyncio.sleep(1)
                connected = self._bridge.get_connected_accounts()
                if len(connected) == prev_count and prev_count > 0:
                    stable_rounds += 1
                    if stable_rounds >= 4:
                        break
                else:
                    stable_rounds = 0
                    prev_count = len(connected)

            if not connected:
                self._log(
                    "[GensparkMode] No Genspark accounts detected.\n"
                    "  Open https://www.genspark.ai/ai_image and log in."
                )
                wait_start = time.time()
                while not connected:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        return
                    if time.time() - wait_start > 60:
                        self._log("[GensparkMode] No accounts found. Aborting.")
                        return
                    await asyncio.sleep(3)
                    connected = self._bridge.get_connected_accounts()

            self._log(
                f"[GensparkMode] Found {len(connected)} account(s): "
                + ", ".join(connected.keys())
            )

            # Spin up workers
            for email, info in connected.items():
                account_name = email or info.get("display_name", "unknown")
                workers = []
                for idx in range(1, slots_per_account + 1):
                    slot_id = f"{account_name}#gs{idx}"
                    workers.append(GensparkWorker(slot_id, account_name, self._bridge, self._log))
                self._workers[account_name] = workers
                plan = info.get("plan_type", "free")
                self._log(
                    f"[GensparkMode] {account_name} [{plan}]: {len(workers)} worker(s) ready."
                )

            total_workers = sum(len(w) for w in self._workers.values())
            if total_workers == 0:
                self._log("[GensparkMode] No workers started.")
                return

            self._log(f"[GensparkMode] Total: {total_workers} worker(s) across "
                     f"{len(self._workers)} account(s).")

            # Main dispatch loop
            while self.qm.is_running:
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    break
                if self.qm.pause_requested:
                    await asyncio.sleep(1)
                    continue

                # Dynamic account discovery — add new accounts that connected
                # after initial setup
                current_accounts = self._bridge.get_connected_accounts()
                for email, info in current_accounts.items():
                    if email and email not in self._workers:
                        workers = []
                        for idx in range(1, slots_per_account + 1):
                            slot_id = f"{email}#gs{idx}"
                            workers.append(
                                GensparkWorker(slot_id, email, self._bridge, self._log)
                            )
                        self._workers[email] = workers
                        self._log(
                            f"[GensparkMode] New account: {email} — "
                            f"{len(workers)} worker(s) added."
                        )

                # Prune finished tasks
                self._active_tasks = [t for t in self._active_tasks if not t.done()]

                jobs = get_all_jobs()
                pending = [j for j in jobs if j["status"] == "pending"]

                if not pending:
                    if not self._active_tasks:
                        still_active = any(
                            j["status"] in ("pending", "running") for j in get_all_jobs()
                        )
                        if not still_active:
                            self._log("[GensparkMode] All jobs completed.")
                            break
                    await asyncio.sleep(self.qm.scheduler_poll_seconds)
                    continue

                busy_slots = {
                    t.get_name() for t in self._active_tasks if hasattr(t, "get_name")
                }

                dispatched = 0
                for job in pending:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        break
                    worker = self._get_available_worker(busy_slots)
                    if not worker:
                        break

                    job_id = job["id"]
                    update_job_status(job_id, "running", account=worker.account_email)
                    self.qm.signals.job_updated.emit(
                        job_id, "running", worker.account_email, ""
                    )

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

            # Drain remaining tasks
            if self._active_tasks:
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    self._log(
                        f"[GensparkMode] Cancelling {len(self._active_tasks)} task(s)..."
                    )
                    for t in self._active_tasks:
                        if not t.done():
                            t.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*self._active_tasks, return_exceptions=True),
                            timeout=5.0,
                        )
                    except asyncio.TimeoutError:
                        self._log("[GensparkMode] Some tasks didn't cancel in 5s.")
                else:
                    self._log(
                        f"[GensparkMode] Waiting for {len(self._active_tasks)} task(s)..."
                    )
                    await asyncio.gather(*self._active_tasks, return_exceptions=True)

        finally:
            await self._bridge.stop()
            self._workers.clear()
            self._log("[GensparkMode] Genspark mode stopped.")

    # ═══════════════════════════════════════════════════════════════
    # Job execution
    # ═══════════════════════════════════════════════════════════════

    def _get_available_worker(self, busy_slots: set) -> Optional[GensparkWorker]:
        """Find a free worker, round-robin across accounts."""
        # Preserve insertion order but rotate starting account for fairness
        account_list = list(self._workers.keys())
        if not account_list:
            return None
        # Simple rotation
        for email, workers in self._workers.items():
            if self.qm.account_disabled.get(email):
                continue
            for w in workers:
                if w.slot_id not in busy_slots and not w.is_busy:
                    return w
        return None

    async def _run_job(self, worker: GensparkWorker, job: dict) -> None:
        """Execute a single image-generation job."""
        job_id = job["id"]
        prompt = str(job.get("prompt") or "").strip()
        if not prompt:
            update_job_status(job_id, "failed", account=worker.account_email,
                              error="empty_prompt")
            self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email,
                                            "empty_prompt")
            return

        model = _resolve_model(job.get("model") or "nano-banana-2")
        ratio = _resolve_aspect_ratio(job.get("aspect_ratio") or "auto")

        max_retries = int(self.qm.max_auto_retries_per_job or 2)
        last_error = ""
        worker.is_busy = True
        try:
            for attempt in range(max_retries + 1):
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    update_job_status(job_id, "pending", account="")
                    self.qm.signals.job_updated.emit(job_id, "pending", "", "")
                    return
                try:
                    result = await self._bridge.generate_image(
                        account=worker.account_email,
                        prompt=prompt,
                        model=model,
                        aspect_ratio=ratio,
                    )
                except Exception as e:
                    last_error = f"bridge_error: {e}"
                    self._log(f"[{worker.slot_id}] Bridge exception: {last_error}")
                    if attempt < max_retries:
                        await asyncio.sleep(10)
                        continue
                    break

                if result.get("error"):
                    last_error = result["error"]
                    self._log(
                        f"[{worker.slot_id}] Attempt {attempt + 1}/{max_retries + 1} "
                        f"failed: {last_error[:200]}"
                    )
                    # Common recoverable errors
                    err_lower = last_error.lower()
                    if "recaptcha" in err_lower or "captcha" in err_lower:
                        # Same as Flow: reload tab, retry
                        await asyncio.sleep(5)
                        continue
                    if "429" in err_lower or "rate" in err_lower:
                        self._log(
                            f"[{worker.slot_id}] 429 — backing off 60s (session limit)"
                        )
                        await asyncio.sleep(60)
                        continue
                    if "timeout" in err_lower:
                        await asyncio.sleep(5)
                        continue
                    # Unknown error — break after a couple of tries
                    if attempt < max_retries:
                        await asyncio.sleep(10)
                        continue
                    break

                # Success path — save the image to output dir
                out_path = await self._save_image(result, job_id, worker.account_email)
                if out_path:
                    update_job_status(job_id, "completed",
                                      account=worker.account_email)
                    self.qm.signals.job_updated.emit(
                        job_id, "completed", worker.account_email, ""
                    )
                    self._log(f"[{worker.slot_id}] Job {job_id[:6]}... completed! "
                             f"({out_path})")
                    return
                else:
                    last_error = "save_failed"

            # Out of retries
            update_job_status(job_id, "failed", account=worker.account_email,
                              error=last_error or "unknown")
            self.qm.signals.job_updated.emit(
                job_id, "failed", worker.account_email, last_error or "unknown"
            )
            self._log(
                f"[{worker.slot_id}] Job {job_id[:6]}... FAILED: "
                f"{(last_error or 'unknown')[:200]}"
            )
        finally:
            worker.is_busy = False

    async def _save_image(self, result: Dict[str, Any], job_id: str,
                          account: str) -> Optional[str]:
        """Save the returned image bytes (or fetch from URL) to the output dir."""
        try:
            out_dir = get_output_directory() or "outputs"
            os.makedirs(out_dir, exist_ok=True)

            # Prefer base64 payload (already downloaded by extension)
            b64 = result.get("image_bytes_b64") or ""
            if b64:
                data = base64.b64decode(b64)
                ext = "jpg"
                filename = f"genspark_{job_id[:8]}_{int(time.time())}.{ext}"
                path = os.path.join(out_dir, filename)
                with open(path, "wb") as f:
                    f.write(data)
                return path

            # Fallback: tell user the URL, mark as "completed" with a note
            url = result.get("image_url", "")
            if url:
                self._log(
                    f"[GensparkMode] No bytes returned, only URL: {url[:100]}"
                )
                # Write a small .txt pointer instead of a real image
                filename = f"genspark_{job_id[:8]}_{int(time.time())}.url.txt"
                path = os.path.join(out_dir, filename)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(url + "\n")
                return path
            return None
        except Exception as e:
            self._log(f"[GensparkMode] Save failed: {e}")
            return None
