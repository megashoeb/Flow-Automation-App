"""
Grok Mode — Chrome Extension + Grok Imagine direct API calls.

Parallel architecture to extension_mode.py (Flow) and genspark_mode.py:
  - Separate HTTP bridge on port 18926
  - Cookie-based auth (no Bearer token, no reCAPTCHA)
  - Single streaming endpoint handles progress + final video
  - 3-step pipeline: [optional upload] → create post → animate

This file deliberately does NOT import from extension_mode.py or
genspark_mode.py — the three modes are isolated so a change to one
never breaks the others. queue_manager decides which mode to run based
on the `generation_mode` setting ("flow" / "genspark" / "grok").
"""

import asyncio
import base64
import mimetypes
import os
import random
import time
from typing import Any, Dict, List, Optional

from src.core.grok_bridge import GrokBridge
from src.db.db_manager import (
    get_all_jobs,
    get_output_directory,
    get_setting,
    update_job_runtime_state,
    update_job_status,
)


# ─── Settings resolution helpers ───
_ALLOWED_ASPECT = {"16:9", "9:16", "1:1", "2:3", "3:2"}
_ALLOWED_RES = {"480p", "720p"}
_ALLOWED_LEN = {6, 10}


def _resolve_aspect_ratio(ratio_name: str) -> str:
    raw = str(ratio_name or "").strip()
    if raw in _ALLOWED_ASPECT:
        return raw
    low = raw.lower()
    if "portrait" in low or "9:16" in raw:
        return "9:16"
    if "square" in low or "1:1" in raw:
        return "1:1"
    if "2:3" in raw:
        return "2:3"
    if "3:2" in raw:
        return "3:2"
    if "landscape" in low or "16:9" in raw or not raw:
        return "16:9"
    return "16:9"


def _resolve_resolution(res_name: str) -> str:
    raw = str(res_name or "").strip().lower()
    if raw in {"480", "480p", "480 p"}:
        return "480p"
    return "720p"


def _resolve_video_length(length: Any) -> int:
    try:
        n = int(str(length).lower().replace("s", "").strip())
        if n in _ALLOWED_LEN:
            return n
    except Exception:
        pass
    return 10


def _safe_filename(job_id: str, idx: int, ext: str = "mp4") -> str:
    safe = "".join(c for c in str(job_id) if c.isalnum() or c in "._-")[:40]
    return f"{safe or 'video'}_{idx}.{ext}"


class GrokWorker:
    """A worker slot for a single Grok account. Multiple workers per account
    allow parallel generation (up to Grok's fair-use ceiling)."""

    def __init__(self, slot_id: str, account_email: str, bridge: GrokBridge, log_fn):
        self.slot_id = slot_id
        self.account_email = account_email
        self._bridge = bridge
        self._log = log_fn
        self.is_busy = False


class GrokModeManager:
    """
    Grok automation mode — uses the Chrome Extension + a dedicated local
    bridge (GrokBridge) to drive grok.com/imagine directly.

    Mirrors ExtensionModeManager / GensparkModeManager shape but is fully
    independent. queue_manager.py calls run() when generation_mode == "grok".
    """

    # Min gap between dispatches per account (respect fair-use)
    DISPATCH_STAGGER_MIN = 2.0
    DISPATCH_STAGGER_MAX = 3.5

    def __init__(self, queue_manager):
        self.qm = queue_manager
        self._log = lambda msg: queue_manager.signals.log_msg.emit(msg)
        self._bridge = GrokBridge(self._log)
        self._workers: Dict[str, List[GrokWorker]] = {}
        self._active_tasks: List[asyncio.Task] = []
        # Settings snapshot loaded at run() start
        self._aspect = "16:9"
        self._resolution = "720p"
        self._video_length = 10
        self._mode = "custom"
        self._use_reference = False
        self._reference_folder = ""

    # ═══════════════════════════════════════════════════════════════
    # Main loop
    # ═══════════════════════════════════════════════════════════════

    async def run(self) -> None:
        self._log("[GrokMode] Starting Grok Imagine automation mode...")
        try:
            await self._bridge.start()
        except Exception as e:
            self._log(f"[GrokMode] Bridge failed to start: {e}")
            return

        try:
            # Snapshot UI settings saved by main_window.py
            self._aspect = _resolve_aspect_ratio(str(get_setting("grok_aspect_ratio", "16:9")))
            self._resolution = _resolve_resolution(str(get_setting("grok_resolution", "720p")))
            self._video_length = _resolve_video_length(get_setting("grok_video_length", "10"))
            self._mode = str(get_setting("grok_mode", "custom") or "custom")
            self._use_reference = str(get_setting("grok_use_reference", "") or "").lower() in {"1", "true", "yes"}
            self._reference_folder = str(get_setting("grok_reference_folder", "") or "")
            self._log(
                f"[GrokMode] Settings → aspect={self._aspect}, "
                f"res={self._resolution}, length={self._video_length}s, "
                f"mode={self._mode}, use_ref={self._use_reference}"
            )

            slots_per_account = max(1, min(15, int(self.qm.account_parallel_slots or 3)))
            if slots_per_account > 5:
                self._log(
                    f"[GrokMode] ⚠ Parallel={slots_per_account}/account is high — "
                    "Grok fair-use throttle may kick in. 3–5 is safer."
                )

            self._log(
                "[GrokMode] Waiting for Chrome Extension to connect...\n"
                "  Make sure Chrome is open with G-Labs Helper extension\n"
                "  and grok.com/imagine is logged in (SuperGrok recommended)."
            )

            wait_start = time.time()
            while not self._bridge.is_extension_connected():
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    return
                if time.time() - wait_start > 60:
                    self._log("[GrokMode] Extension did not connect. Aborting.")
                    return
                await asyncio.sleep(1)

            # Give extension up to ~20s to report Grok accounts
            await asyncio.sleep(4)
            connected = self._bridge.get_accounts()
            stable = 0
            prev_count = len(connected)
            for _ in range(20):
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    return
                await asyncio.sleep(1)
                connected = self._bridge.get_accounts()
                if len(connected) == prev_count and prev_count > 0:
                    stable += 1
                    if stable >= 4:
                        break
                else:
                    stable = 0
                    prev_count = len(connected)

            if not connected:
                self._log(
                    "[GrokMode] No Grok accounts detected.\n"
                    "  Open https://grok.com/imagine and log in, then retry."
                )
                wait_start = time.time()
                while not connected:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        return
                    if time.time() - wait_start > 60:
                        self._log("[GrokMode] No accounts found. Aborting.")
                        return
                    await asyncio.sleep(3)
                    connected = self._bridge.get_accounts()

            account_names = [a["email"] for a in connected]
            self._log(
                f"[GrokMode] Found {len(connected)} account(s): "
                + ", ".join(account_names)
            )

            # Spin up workers
            for info in connected:
                email = info["email"]
                sub = info.get("subscription", "")
                workers = []
                for idx in range(1, slots_per_account + 1):
                    slot_id = f"{email}#gr{idx}"
                    workers.append(GrokWorker(slot_id, email, self._bridge, self._log))
                self._workers[email] = workers
                tag = f" [{sub}]" if sub else ""
                self._log(
                    f"[GrokMode] {email}{tag}: {len(workers)} worker(s) ready."
                )

            total_workers = sum(len(w) for w in self._workers.values())
            if total_workers == 0:
                self._log("[GrokMode] No workers started.")
                return

            self._log(
                f"[GrokMode] Total: {total_workers} worker(s) across "
                f"{len(self._workers)} account(s). RAM: ~50MB (no browser launched)."
            )

            # Idle heartbeat
            last_heartbeat = 0.0
            heartbeat_interval = 20.0

            # Main dispatch loop — mirrors genspark_mode pattern
            while self.qm.is_running:
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    break
                if getattr(self.qm, "pause_requested", False):
                    await asyncio.sleep(1)
                    continue

                # Dynamic account discovery — if new grok.com tabs opened
                # mid-run, spin up worker slots for them.
                current = self._bridge.get_accounts()
                for info in current:
                    email = info.get("email", "")
                    if email and email not in self._workers:
                        workers = []
                        for idx in range(1, slots_per_account + 1):
                            slot_id = f"{email}#gr{idx}"
                            workers.append(GrokWorker(slot_id, email, self._bridge, self._log))
                        self._workers[email] = workers
                        self._log(
                            f"[GrokMode] New account: {email} — {len(workers)} worker(s) added."
                        )

                # Prune finished tasks
                self._active_tasks = [t for t in self._active_tasks if not t.done()]

                jobs = get_all_jobs() or []
                # Grok handles VIDEO jobs only — skip image jobs which
                # belong to Flow/Genspark modes.
                pending = [
                    j for j in jobs
                    if j.get("status") == "pending"
                    and str(j.get("job_type") or "").lower() == "video"
                ]

                now_ts = time.time()
                if now_ts - last_heartbeat > heartbeat_interval:
                    running_count = sum(1 for j in jobs if j.get("status") == "running")
                    failed_count = sum(1 for j in jobs if j.get("status") == "failed")
                    done_count = sum(1 for j in jobs if j.get("status") == "completed")
                    self._log(
                        f"[GrokMode] ⏱ waiting — "
                        f"pending={len(pending)}, running={running_count}, "
                        f"done={done_count}, failed={failed_count}, "
                        f"active_tasks={len(self._active_tasks)}"
                    )
                    last_heartbeat = now_ts

                if not pending:
                    if not self._active_tasks:
                        still_active = any(
                            j.get("status") in ("pending", "running")
                            for j in (get_all_jobs() or [])
                        )
                        if not still_active:
                            self._log(
                                "[GrokMode] All jobs completed (or failed). "
                                "Stopping Grok mode. Add fresh prompts and "
                                "click Start Automation to run more."
                            )
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
                        break  # all busy — wait

                    job_id = job["id"]
                    prompt_preview = str(job.get("prompt") or "")[:60]
                    self._log(
                        f"[GrokMode] Dispatching job {str(job_id)[:6]}... "
                        f"to {worker.slot_id} | prompt: \"{prompt_preview}\""
                    )
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

                    stagger = random.uniform(self.DISPATCH_STAGGER_MIN, self.DISPATCH_STAGGER_MAX)
                    if stagger > 0:
                        await asyncio.sleep(stagger)

                if dispatched == 0:
                    await asyncio.sleep(self.qm.scheduler_poll_seconds)

            # Drain remaining tasks
            if self._active_tasks:
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    self._log(
                        f"[GrokMode] Cancelling {len(self._active_tasks)} task(s)..."
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
                        self._log("[GrokMode] Some tasks didn't cancel in 5s.")
                else:
                    self._log(
                        f"[GrokMode] Waiting for {len(self._active_tasks)} task(s)..."
                    )
                    await asyncio.gather(*self._active_tasks, return_exceptions=True)

        finally:
            try:
                self._bridge.cancel_all_pending()
            except Exception:
                pass
            try:
                await self._bridge.stop()
            except Exception:
                pass
            self._workers.clear()
            self._log("[GrokMode] Grok mode stopped.")

    # ═══════════════════════════════════════════════════════════════
    # Worker picker
    # ═══════════════════════════════════════════════════════════════

    def _get_available_worker(self, busy_slots: set) -> Optional[GrokWorker]:
        """Find a free worker, round-robin across accounts. Skips accounts
        that are disabled or currently on a 429-pause."""
        if not self._workers:
            return None
        now = time.time()
        for email, workers in self._workers.items():
            if getattr(self.qm, "account_disabled", {}).get(email):
                continue
            pause_until = getattr(self.qm, "account_pause_until", {}).get(email, 0)
            if pause_until and now < pause_until:
                continue
            for w in workers:
                if w.slot_id not in busy_slots and not w.is_busy:
                    return w
        return None

    # ═══════════════════════════════════════════════════════════════
    # Per-job execution
    # ═══════════════════════════════════════════════════════════════

    async def _run_job(self, worker: GrokWorker, job: Dict[str, Any]) -> None:
        """Execute a single Grok video-generation job end-to-end."""
        worker.is_busy = True
        job_id = job.get("id", "")
        # For video jobs the prompt is in video_prompt; fall back to prompt
        prompt = str(
            job.get("video_prompt") or job.get("prompt") or ""
        ).strip()
        short = (prompt[:40] + "…") if len(prompt) > 40 else prompt
        self._log(f"[{worker.slot_id}] Job {str(job_id)[:6]}…: {short}")

        # Per-job aspect ratio if the video job carries one, else global default
        per_job_ratio = str(job.get("video_ratio") or "").strip()
        job_aspect = _resolve_aspect_ratio(per_job_ratio) if per_job_ratio else self._aspect

        # Reference image: pipeline produces start_image_path; user may also
        # specify image_path directly. Fall back to configured folder.
        image_path = str(
            job.get("start_image_path") or job.get("image_path") or ""
        )
        ref_b64 = ""
        ref_name = ""
        ref_mime = "image/jpeg"

        use_ref = False
        if image_path and os.path.isfile(image_path):
            use_ref = True
        elif self._use_reference and self._reference_folder:
            cand = self._pick_reference_for_job(job)
            if cand:
                image_path = cand
                use_ref = True

        if use_ref and image_path:
            try:
                with open(image_path, "rb") as fh:
                    raw = fh.read()
                ref_b64 = base64.b64encode(raw).decode("ascii")
                ref_name = os.path.basename(image_path)
                guessed = mimetypes.guess_type(image_path)[0] or "image/jpeg"
                ref_mime = guessed
                self._log(
                    f"[{worker.slot_id}] Using reference: {ref_name} ({len(raw)} bytes)"
                )
            except Exception as e:
                self._log(
                    f"[{worker.slot_id}] Reference read failed: {e} — falling back to text-to-video"
                )
                ref_b64 = ""
                ref_name = ""

        # Submit to bridge
        try:
            future = self._bridge.submit_request(
                account=worker.account_email,
                prompt=prompt,
                aspect_ratio=job_aspect,
                video_length=self._video_length,
                resolution=self._resolution,
                mode=self._mode,
                reference_image_base64=ref_b64,
                reference_image_filename=ref_name,
                reference_image_mime=ref_mime,
            )
        except Exception as e:
            self._log(f"[{worker.slot_id}] Bridge submit failed: {e}")
            update_job_status(
                job_id, "failed", account=worker.account_email,
                error=f"bridge_submit: {e}"[:500],
            )
            self.qm.signals.job_updated.emit(
                job_id, "failed", worker.account_email, "bridge_submit"
            )
            worker.is_busy = False
            return

        self._log(
            f"[{worker.slot_id}] Dispatched to extension — waiting for result..."
        )

        try:
            # Upper cap 5 min: upload (~5s) + video gen (~2 min) + download (~10s)
            result = await asyncio.wait_for(future, timeout=300)
        except asyncio.TimeoutError:
            self._log(f"[{worker.slot_id}] Timeout waiting for video (>5min)")
            update_job_status(
                job_id, "failed", account=worker.account_email,
                error="grok_timeout_300s",
            )
            self.qm.signals.job_updated.emit(
                job_id, "failed", worker.account_email, "grok_timeout"
            )
            worker.is_busy = False
            return
        except asyncio.CancelledError:
            worker.is_busy = False
            raise
        except Exception as e:
            self._log(f"[{worker.slot_id}] Bridge wait failed: {e}")
            update_job_status(
                job_id, "failed", account=worker.account_email,
                error=str(e)[:500],
            )
            self.qm.signals.job_updated.emit(
                job_id, "failed", worker.account_email, str(e)[:120]
            )
            worker.is_busy = False
            return

        # Interpret result
        if not result or result.get("error"):
            err = (result or {}).get("error", "unknown_error")
            body_preview = (result or {}).get("response_body", "")
            msg = f"{err}"
            if body_preview:
                msg += f" — {body_preview[:120]}"
            self._log(f"[{worker.slot_id}] Grok returned error: {msg}")

            # Specific error handling
            if "moderat" in str(err).lower():
                update_job_status(
                    job_id, "failed", account=worker.account_email,
                    error=f"grok_moderated: {msg}"[:500],
                )
            elif "429" in str(err) or "rate" in str(err).lower() or "quota" in str(err).lower():
                try:
                    self.qm.pause_account_for_429(worker.account_email)
                except Exception:
                    pass
                update_job_status(
                    job_id, "failed", account=worker.account_email,
                    error=f"grok_rate_limited: {msg}"[:500],
                )
            else:
                update_job_status(
                    job_id, "failed", account=worker.account_email,
                    error=f"grok: {msg}"[:500],
                )
            self.qm.signals.job_updated.emit(
                job_id, "failed", worker.account_email, str(err)[:120]
            )
            worker.is_busy = False
            return

        b64 = result.get("content_base64", "")
        if not b64:
            self._log(f"[{worker.slot_id}] Grok success reported but no video bytes")
            update_job_status(
                job_id, "failed", account=worker.account_email,
                error="grok_no_bytes",
            )
            self.qm.signals.job_updated.emit(
                job_id, "failed", worker.account_email, "grok_no_bytes"
            )
            worker.is_busy = False
            return

        # Save to output folder
        out_dir = get_output_directory() or os.getcwd()
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception:
            pass

        fname = job.get("output_filename", "") or _safe_filename(
            job_id, int(job.get("idx", 1) or 1)
        )
        if not fname.lower().endswith(".mp4"):
            fname += ".mp4"
        out_path = os.path.join(out_dir, fname)

        try:
            video_bytes = base64.b64decode(b64)
            with open(out_path, "wb") as fh:
                fh.write(video_bytes)
            size = len(video_bytes)
            self._log(f"[{worker.slot_id}] Saved: {fname} ({size} bytes)")
            update_job_runtime_state(job_id, output_path=out_path)
            update_job_status(job_id, "completed", account=worker.account_email)
            self.qm.signals.job_updated.emit(
                job_id, "completed", worker.account_email, ""
            )
            self._log(
                f"[{worker.slot_id}] Job {str(job_id)[:6]}… completed! ({out_path})"
            )
        except Exception as e:
            self._log(f"[{worker.slot_id}] Save failed: {e}")
            update_job_status(
                job_id, "failed", account=worker.account_email,
                error=f"save_error: {e}"[:500],
            )
            self.qm.signals.job_updated.emit(
                job_id, "failed", worker.account_email, "save_error"
            )
        finally:
            worker.is_busy = False

    def _pick_reference_for_job(self, job: Dict[str, Any]) -> str:
        """If the user enabled a reference-folder, match this job to a file.
        Strategy:
          1. <idx>.jpg / <idx>.png in the folder (most common from pipeline)
          2. Else pick deterministically by idx modulo entry count
        Returns absolute path, or "" if nothing usable."""
        folder = self._reference_folder
        if not folder or not os.path.isdir(folder):
            return ""
        try:
            entries = [
                os.path.join(folder, f)
                for f in sorted(os.listdir(folder))
                if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))
            ]
        except Exception:
            return ""
        if not entries:
            return ""
        idx = int(job.get("idx", 0) or 0)
        for ext in (".jpg", ".jpeg", ".png", ".webp"):
            p = os.path.join(folder, f"{idx}{ext}")
            if os.path.isfile(p):
                return p
        return entries[idx % len(entries)]
