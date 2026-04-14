"""
Extension Bridge Server — local HTTP server for Chrome Extension communication.

Architecture:
  Python App ←→ Bridge (localhost:18924) ←→ Chrome Extension

Endpoints:
  GET  /poll      — Extension polls for pending work
  POST /token     — Extension sends reCAPTCHA token + auth back
  POST /accounts  — Extension reports connected accounts
  POST /project   — Extension reports project ID
  GET  /status    — App checks bridge status
  POST /command   — App sends commands to extension (cookie clear, reload)

The bridge runs as an asyncio background task inside the main app.
"""

import asyncio
import json
import time
import logging
from typing import Optional, Dict, Any, Callable
from aiohttp import web

log = logging.getLogger(__name__)

BRIDGE_PORT = 18924
BRIDGE_HOST = "127.0.0.1"


class ExtensionBridge:
    """Local HTTP bridge between Python app and Chrome Extension."""

    def __init__(self, log_fn: Optional[Callable] = None):
        self._log = log_fn or (lambda msg: None)
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

        # Token request queue: request_id -> { account, action, future }
        self._pending_requests: Dict[str, Dict[str, Any]] = {}
        self._request_counter = 0

        # Connected accounts from extension
        self._connected_accounts: Dict[str, Dict[str, Any]] = {}  # email -> info

        # Project IDs from extension
        self._project_ids: Dict[str, str] = {}  # account_email -> project_id

        # Pending commands for extension
        self._pending_commands = []

        # Stats
        self._tokens_received = 0
        self._extension_last_seen = 0

    # ═══════════════════════════════════════════════════════════════
    # Lifecycle
    # ═══════════════════════════════════════════════════════════════

    async def start(self):
        """Start the bridge HTTP server."""
        self._app = web.Application()
        self._app.router.add_get("/poll", self._handle_poll)
        self._app.router.add_post("/token", self._handle_token)
        self._app.router.add_post("/accounts", self._handle_accounts)
        self._app.router.add_post("/project", self._handle_project)
        self._app.router.add_get("/status", self._handle_status)
        self._app.router.add_post("/command", self._handle_command_post)

        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()

        try:
            self._site = web.TCPSite(self._runner, BRIDGE_HOST, BRIDGE_PORT)
            await self._site.start()
            self._log(f"[Bridge] Started on http://{BRIDGE_HOST}:{BRIDGE_PORT}")
        except OSError as e:
            self._log(f"[Bridge] Port {BRIDGE_PORT} busy: {e}. Trying alternative...")
            # Try alternative port
            self._site = web.TCPSite(self._runner, BRIDGE_HOST, BRIDGE_PORT + 1)
            await self._site.start()
            self._log(f"[Bridge] Started on http://{BRIDGE_HOST}:{BRIDGE_PORT + 1}")

    async def stop(self):
        """Stop the bridge server."""
        # Cancel all pending requests
        for req_id, req in self._pending_requests.items():
            fut = req.get("future")
            if fut and not fut.done():
                fut.set_result({"error": "bridge_stopped"})
        self._pending_requests.clear()

        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        self._log("[Bridge] Stopped.")

    # ═══════════════════════════════════════════════════════════════
    # Public API — called by ExtensionModeManager
    # ═══════════════════════════════════════════════════════════════

    async def request_token(self, account: str, action: str, timeout: float = 30.0) -> Dict[str, Any]:
        """
        Request a reCAPTCHA token from the Chrome Extension.

        Args:
            account: Email of the account to use
            action: reCAPTCHA action (e.g., "IMAGE_GENERATION")
            timeout: Max seconds to wait for token

        Returns:
            dict with keys: token, access_token, project_id, error
        """
        self._request_counter += 1
        request_id = f"req_{self._request_counter}_{int(time.time())}"

        loop = asyncio.get_event_loop()
        future = loop.create_future()

        self._pending_requests[request_id] = {
            "account": account,
            "action": action,
            "future": future,
            "created": time.time(),
        }

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            return {"error": "timeout"}

    def send_command(self, command_type: str, account: str = "", data: Any = None):
        """Queue a command for the extension (cookie clear, reload, etc.)."""
        self._pending_commands.append({
            "type": command_type,
            "account": account,
            "data": data,
        })

    def get_connected_accounts(self):
        """Get list of accounts currently connected via extension."""
        return dict(self._connected_accounts)

    def get_project_id(self, account: str) -> Optional[str]:
        """Get cached project ID for an account."""
        return self._project_ids.get(account)

    def set_project_id(self, account: str, project_id: str):
        """Cache a project ID for an account."""
        self._project_ids[account] = project_id

    @property
    def is_extension_connected(self) -> bool:
        """Check if extension has polled recently (within 5 seconds)."""
        return (time.time() - self._extension_last_seen) < 5

    # ═══════════════════════════════════════════════════════════════
    # HTTP Handlers — called by Chrome Extension
    # ═══════════════════════════════════════════════════════════════

    async def _handle_poll(self, request: web.Request) -> web.Response:
        """Extension polls for work. Return pending token request or command."""
        self._extension_last_seen = time.time()

        response_data = {"work": None, "command": None}

        # Check for pending token requests (oldest first)
        for req_id, req in list(self._pending_requests.items()):
            fut = req.get("future")
            if fut and not fut.done():
                response_data["work"] = {
                    "request_id": req_id,
                    "account": req["account"],
                    "action": req["action"],
                }
                break

        # Check for pending commands
        if self._pending_commands:
            response_data["command"] = self._pending_commands.pop(0)

        return web.json_response(response_data, headers={"Access-Control-Allow-Origin": "*"})

    async def _handle_token(self, request: web.Request) -> web.Response:
        """Extension sends back token + auth data."""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False}, status=400)

        request_id = data.get("request_id", "")
        req = self._pending_requests.pop(request_id, None)

        if req and req.get("future") and not req["future"].done():
            result = {
                "token": data.get("token"),
                "access_token": data.get("access_token"),
                "email": data.get("email"),
                "project_id": data.get("project_id"),
                "error": data.get("error"),
            }

            # Cache project ID if received
            email = data.get("email", "")
            pid = data.get("project_id")
            if email and pid:
                self._project_ids[email] = pid

            if data.get("token"):
                self._tokens_received += 1

            req["future"].set_result(result)

        return web.json_response({"ok": True}, headers={"Access-Control-Allow-Origin": "*"})

    async def _handle_accounts(self, request: web.Request) -> web.Response:
        """Extension reports connected accounts."""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False}, status=400)

        accounts = data.get("accounts", [])
        self._connected_accounts.clear()

        for acc in accounts:
            email = acc.get("email", "")
            if email:
                self._connected_accounts[email] = {
                    "email": email,
                    "name": acc.get("name", ""),
                    "tab_id": acc.get("tab_id"),
                    "project_id": acc.get("project_id"),
                }
                if acc.get("project_id"):
                    self._project_ids[email] = acc["project_id"]

        if accounts:
            self._log(
                f"[Bridge] Extension reports {len(accounts)} account(s): "
                + ", ".join(a.get("email", "?") for a in accounts)
            )

        return web.json_response({"ok": True}, headers={"Access-Control-Allow-Origin": "*"})

    async def _handle_project(self, request: web.Request) -> web.Response:
        """Extension reports project ID for an account."""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False}, status=400)

        account = data.get("account", "")
        project_id = data.get("project_id", "")
        if account and project_id:
            self._project_ids[account] = project_id
            self._log(f"[Bridge] Project for {account}: {project_id}")

        return web.json_response({"ok": True}, headers={"Access-Control-Allow-Origin": "*"})

    async def _handle_status(self, request: web.Request) -> web.Response:
        """App checks bridge status."""
        return web.json_response({
            "running": True,
            "extension_connected": self.is_extension_connected,
            "connected_accounts": list(self._connected_accounts.keys()),
            "tokens_received": self._tokens_received,
            "pending_requests": len(self._pending_requests),
        }, headers={"Access-Control-Allow-Origin": "*"})

    async def _handle_command_post(self, request: web.Request) -> web.Response:
        """App queues a command for the extension."""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False}, status=400)

        self.send_command(
            data.get("type", ""),
            data.get("account", ""),
            data.get("data"),
        )
        return web.json_response({"ok": True}, headers={"Access-Control-Allow-Origin": "*"})
