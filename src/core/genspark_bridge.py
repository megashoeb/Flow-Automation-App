"""
Genspark Bridge Server — local HTTP bridge for Chrome Extension <-> Python.

Architecture (mirrors extension_bridge.py but for genspark.ai):
  Python App <-> Bridge (localhost:18925) <-> Chrome Extension

Differences from Flow bridge (extension_bridge.py):
  - No Bearer token dance — Genspark uses cookies only
  - Unlimited generation on Plus/Pro plans (no per-day quota dance)
  - Session-based rate limits (5-hour windows) instead of daily
  - SSE parsing needed for /api/agent/ask_proxy response
  - JSON polling via /api/spark/image_generation_task_detail
  - Recaptcha Enterprise site key: 6LfYyWcsAAAAAK8DUr6Oo1wHl2CJ5kKbO0AK3LIM

Endpoints (served to extension):
  GET  /genspark/poll          — extension polls for pending work
  POST /genspark/work-result   — extension sends generation result back
  POST /genspark/accounts      — extension reports logged-in Genspark accounts
  GET  /genspark/status        — app checks bridge status
"""

import asyncio
import json
import logging
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional

from aiohttp import web

log = logging.getLogger(__name__)

GENSPARK_BRIDGE_PORT = 18925
GENSPARK_BRIDGE_HOST = "127.0.0.1"

# Genspark's reCAPTCHA Enterprise site key (observed from production traffic)
GENSPARK_RECAPTCHA_SITE_KEY = "6LfYyWcsAAAAAK8DUr6Oo1wHl2CJ5kKbO0AK3LIM"


class GensparkBridge:
    """Local HTTP bridge between Python app and Chrome Extension — Genspark mode.

    Runs on a SEPARATE port (18925) from the Flow bridge (18924) so both can
    coexist if a user switches between modes without restarting the app.
    """

    def __init__(self, log_fn: Optional[Callable[[str], None]] = None):
        self._log: Callable[[str], None] = log_fn or (lambda msg: None)
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

        # Pending generation requests: request_id -> { prompt, model_params,
        # future, created_at, account, ... }
        self._pending_requests: Dict[str, Dict[str, Any]] = {}
        self._request_counter = 0

        # Connected accounts reported by extension.
        # Shape: { email: {email, plan_type, tab_id, last_seen} }
        self._connected_accounts: Dict[str, Dict[str, Any]] = {}

        # Track which extension instance picked up which request
        # request_id -> frozenset(ext_accounts)
        self._dispatched_to: Dict[str, frozenset] = {}

        # Stats
        self._images_generated = 0
        self._extension_last_seen = 0.0

    # ═══════════════════════════════════════════════════════════════
    # Lifecycle
    # ═══════════════════════════════════════════════════════════════

    async def start(self) -> None:
        """Start the Genspark bridge HTTP server."""
        self._app = web.Application()
        self._app.router.add_get("/genspark/poll", self._handle_poll)
        self._app.router.add_post("/genspark/work-result", self._handle_work_result)
        self._app.router.add_post("/genspark/accounts", self._handle_accounts)
        self._app.router.add_get("/genspark/status", self._handle_status)

        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()

        try:
            self._site = web.TCPSite(self._runner, GENSPARK_BRIDGE_HOST, GENSPARK_BRIDGE_PORT)
            await self._site.start()
            self._log(
                f"[GensparkBridge] Started on "
                f"http://{GENSPARK_BRIDGE_HOST}:{GENSPARK_BRIDGE_PORT}"
            )
        except OSError as e:
            self._log(f"[GensparkBridge] Port {GENSPARK_BRIDGE_PORT} busy: {e}. Trying +1...")
            self._site = web.TCPSite(self._runner, GENSPARK_BRIDGE_HOST, GENSPARK_BRIDGE_PORT + 1)
            await self._site.start()
            self._log(
                f"[GensparkBridge] Started on "
                f"http://{GENSPARK_BRIDGE_HOST}:{GENSPARK_BRIDGE_PORT + 1}"
            )

    async def stop(self) -> None:
        """Stop the bridge server and fail any pending futures."""
        for req_id, req in list(self._pending_requests.items()):
            fut = req.get("future")
            if fut and not fut.done():
                fut.set_result({"error": "bridge_stopped"})
        self._pending_requests.clear()
        self._dispatched_to.clear()

        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        self._log("[GensparkBridge] Stopped.")

    # ═══════════════════════════════════════════════════════════════
    # Public API — called by GensparkModeManager
    # ═══════════════════════════════════════════════════════════════

    async def generate_image(
        self,
        account: str,
        prompt: str,
        model: str = "nano-banana-2",
        aspect_ratio: str = "auto",
        style: str = "auto",
        image_size: str = "auto",
        auto_prompt: bool = True,
        background_mode: bool = True,
        timeout: float = 300.0,
    ) -> Dict[str, Any]:
        """Submit a generation request to the extension.

        The extension will:
          1. POST /api/agent/ask_proxy with our params + its own reCAPTCHA token
          2. Parse the SSE stream to extract task_id
          3. Poll /api/spark/image_generation_task_detail until COMPLETED
          4. Download the image via /api/files/s/{id}
          5. Return {image_bytes_b64, image_url, prompt_used, model_used, ...}
            OR {error: "..."}

        Timeout default 300s (5 min) — generation typically 30-60s + polling.
        """
        self._request_counter += 1
        request_id = f"gen_{self._request_counter}_{int(time.time())}"

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()

        self._pending_requests[request_id] = {
            "account": account,
            "action": "IMAGE_GENERATION",
            "prompt": prompt,
            "model_params": {
                "type": "image",
                "model": model,
                "aspect_ratio": aspect_ratio,
                "auto_prompt": auto_prompt,
                "style": style,
                "image_size": image_size,
                "background_mode": background_mode,
                "camera_control": None,
            },
            "future": future,
            "created": time.time(),
            "_failed_ext_keys": set(),
        }

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            self._dispatched_to.pop(request_id, None)
            return {"error": "timeout"}

    def get_connected_accounts(self) -> Dict[str, Dict[str, Any]]:
        """Get currently-connected Genspark accounts (reported by extension)."""
        return dict(self._connected_accounts)

    @property
    def is_extension_connected(self) -> bool:
        """True if the extension polled us in the last 5 seconds."""
        return (time.time() - self._extension_last_seen) < 5

    # ═══════════════════════════════════════════════════════════════
    # HTTP Handlers — called by Chrome Extension
    # ═══════════════════════════════════════════════════════════════

    async def _handle_poll(self, request: web.Request) -> web.Response:
        """Extension polls for work. Same multi-profile pattern as Flow bridge —
        extension sends ?accounts=email1,email2 and we only give it work for
        those accounts.
        """
        self._extension_last_seen = time.time()

        ext_accounts_param = request.query.get("accounts", "")
        ext_accounts = set(
            e.strip() for e in ext_accounts_param.split(",") if e.strip()
        ) if ext_accounts_param else set()
        ext_key = frozenset(ext_accounts) if ext_accounts else None

        response_data: Dict[str, Any] = {"work": None}

        # Don't give work to an extension that hasn't detected its accounts yet
        if not ext_accounts:
            return web.json_response(response_data, headers=_cors())

        # Find a pending request targeted at one of this ext's accounts
        for req_id, req in list(self._pending_requests.items()):
            fut = req.get("future")
            if not fut or fut.done():
                continue
            target = req.get("account", "")
            failed_ext_keys = req.get("_failed_ext_keys", set())
            if ext_key and ext_key in failed_ext_keys:
                continue
            dispatched_key = self._dispatched_to.get(req_id)
            if dispatched_key is not None and dispatched_key != ext_key:
                # Another extension is already working this request
                continue

            if target in ext_accounts or not target:
                response_data["work"] = {
                    "request_id": req_id,
                    "account": target,
                    "action": req["action"],
                    "prompt": req["prompt"],
                    "model_params": req["model_params"],
                    "recaptcha_site_key": GENSPARK_RECAPTCHA_SITE_KEY,
                }
                self._dispatched_to[req_id] = ext_key
                break

        return web.json_response(response_data, headers=_cors())

    async def _handle_work_result(self, request: web.Request) -> web.Response:
        """Extension submits the final result for a request_id.

        Expected body:
          {
            "request_id": "gen_...",
            "image_url": "https://www.genspark.ai/api/files/s/abc?...",
            "image_bytes_b64": "<base64 jpeg>",     // optional but preferred
            "prompt_used": "enriched prompt ...",
            "model_used": "GEMINI_FLASH_IMAGE_EDIT:nano-banana-2",
            "task_id": "396305a9-...",
            "project_id": "9fbedbc1-...",
            "error": null | "error message"
          }
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "reason": "bad_json"}, status=400)

        request_id = str(data.get("request_id", ""))
        req = self._pending_requests.pop(request_id, None)
        self._dispatched_to.pop(request_id, None)
        if not req:
            # Possibly already timed out
            return web.json_response({"ok": True, "stale": True}, headers=_cors())

        fut = req.get("future")
        if not fut or fut.done():
            return web.json_response({"ok": True, "stale": True}, headers=_cors())

        if data.get("error"):
            dbg = data.get("debug")
            if dbg:
                try:
                    evt_summary = dbg.get("event_types") or {}
                    self._log(
                        f"[GensparkBridge] SSE debug — event types: "
                        f"{evt_summary} (project_id={dbg.get('project_id')})"
                    )
                    last_events = dbg.get("last_events") or []
                    for e in last_events[-5:]:
                        self._log(f"[GensparkBridge]   event: {e}")
                except Exception:
                    pass
            fut.set_result({"error": str(data["error"])})
        else:
            self._images_generated += 1
            fut.set_result({
                "image_url": data.get("image_url", ""),
                "image_bytes_b64": data.get("image_bytes_b64", ""),
                "image_urls_nowatermark": data.get("image_urls_nowatermark", []),
                "prompt_used": data.get("prompt_used", ""),
                "model_used": data.get("model_used", ""),
                "task_id": data.get("task_id", ""),
                "project_id": data.get("project_id", ""),
                "error": None,
            })

        return web.json_response({"ok": True}, headers=_cors())

    async def _handle_accounts(self, request: web.Request) -> web.Response:
        """Extension reports which Genspark accounts are logged in."""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False}, status=400)

        accounts = data.get("accounts") or []
        now = time.time()
        seen_emails = set()
        for a in accounts:
            email = str(a.get("email", "")).strip()
            if not email:
                continue
            seen_emails.add(email)
            self._connected_accounts[email] = {
                "email": email,
                "plan_type": str(a.get("plan_type", "free")),
                "tab_id": int(a.get("tab_id", 0) or 0),
                "user_id": str(a.get("user_id", "")),
                "display_name": str(a.get("display_name", "")),
                "last_seen": now,
            }
        # Expire accounts we haven't heard about in 30s
        for email in list(self._connected_accounts.keys()):
            if email in seen_emails:
                continue
            if now - self._connected_accounts[email].get("last_seen", 0) > 30:
                self._connected_accounts.pop(email, None)

        return web.json_response({"ok": True}, headers=_cors())

    async def _handle_status(self, request: web.Request) -> web.Response:
        """App checks bridge status."""
        return web.json_response({
            "running": True,
            "extension_connected": self.is_extension_connected,
            "connected_accounts": list(self._connected_accounts.keys()),
            "images_generated": self._images_generated,
            "pending_requests": len(self._pending_requests),
        }, headers=_cors())


def _cors() -> Dict[str, str]:
    return {"Access-Control-Allow-Origin": "*"}
