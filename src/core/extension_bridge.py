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
from collections import deque
from typing import Optional, Dict, Any, Callable, List
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

        # Token request queue: request_id -> { account, action, future, ... }
        self._pending_requests: Dict[str, Dict[str, Any]] = {}
        self._request_counter = 0

        # Connected accounts from extension
        self._connected_accounts: Dict[str, Dict[str, Any]] = {}  # email -> info

        # Project IDs from extension
        self._project_ids: Dict[str, str] = {}  # account_email -> project_id

        # Pending commands for extension
        self._pending_commands = []

        # Track which request was last dispatched to which extension
        # request_id -> frozenset(ext_accounts) that currently has the work
        self._dispatched_to: Dict[str, frozenset] = {}

        # ─── Token Pool (pre-fetched reCAPTCHA tokens) ───
        # Keeps N tokens per account ready so 15 parallel slots don't
        # all wait in line for serial extension token generation.
        self._token_pool: Dict[str, deque] = {}       # account -> deque of {token, access_token, project_id, ts}
        self._prefetch_accounts: Dict[str, str] = {}  # prefetch_request_id -> account
        self.TOKEN_POOL_TARGET = 10  # target pre-fetched tokens per account
        self.TOKEN_MAX_AGE = 90      # seconds before a cached token is too old

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
        self._dispatched_to.clear()
        self._token_pool.clear()
        self._prefetch_accounts.clear()

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
        Checks the pre-fetched token pool first for instant response.

        Args:
            account: Email of the account to use
            action: reCAPTCHA action (e.g., "IMAGE_GENERATION")
            timeout: Max seconds to wait for token

        Returns:
            dict with keys: token, access_token, project_id, error
        """
        # ─── Check token pool first ───
        pool = self._token_pool.get(account)
        if pool:
            now = time.time()
            while pool:
                cached = pool.popleft()
                age = now - cached["ts"]
                if age < self.TOKEN_MAX_AGE:
                    self._log(
                        f"[Bridge] Pool hit for {account} "
                        f"(age {age:.0f}s, {len(pool)} left in pool)"
                    )
                    return {
                        "token": cached["token"],
                        "access_token": cached["access_token"],
                        "project_id": cached["project_id"],
                        "error": None,
                    }
                # Token too old, discard and check next

        # ─── No cached token — fall through to live request ───
        self._request_counter += 1
        request_id = f"req_{self._request_counter}_{int(time.time())}"

        loop = asyncio.get_event_loop()
        future = loop.create_future()

        self._pending_requests[request_id] = {
            "account": account,
            "action": action,
            "future": future,
            "created": time.time(),
            "_failed_ext_keys": set(),   # frozensets of ext accounts that failed
            "_reroute_count": 0,
        }

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            self._dispatched_to.pop(request_id, None)
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
        """Extension polls for work. Only return work this extension can handle.

        The extension sends ?accounts=email1,email2 so the bridge knows which
        accounts this particular Chrome profile can serve. Multi-profile setups
        have multiple extensions, each with its own set of accounts.
        """
        self._extension_last_seen = time.time()

        # Parse which accounts this extension instance has
        ext_accounts_param = request.query.get("accounts", "")
        ext_accounts = set(
            e.strip() for e in ext_accounts_param.split(",") if e.strip()
        ) if ext_accounts_param else set()

        ext_key = frozenset(ext_accounts) if ext_accounts else None

        response_data = {"work": None, "command": None}

        # If extension hasn't detected its accounts yet (empty param),
        # DON'T give it any work — wait until it knows what it has.
        # This prevents the race condition where a fresh extension grabs
        # work it can't handle before detectAccounts() runs.
        if not ext_accounts:
            if self._pending_commands:
                response_data["command"] = self._pending_commands.pop(0)
            return web.json_response(response_data, headers={"Access-Control-Allow-Origin": "*"})

        # Check for pending token requests — match to this extension's accounts
        for req_id, req in list(self._pending_requests.items()):
            fut = req.get("future")
            if fut and not fut.done():
                target_account = req.get("account", "")

                # Skip if this extension already failed for this request
                failed_ext_keys = req.get("_failed_ext_keys", set())
                if ext_key and ext_key in failed_ext_keys:
                    continue

                # Skip if this request is currently dispatched to another extension
                # (waiting for response — don't give same work to two extensions)
                dispatched_key = self._dispatched_to.get(req_id)
                if dispatched_key is not None and dispatched_key != ext_key:
                    continue

                # Only give work if this extension has the target account
                if target_account in ext_accounts or not target_account:
                    response_data["work"] = {
                        "request_id": req_id,
                        "account": target_account,
                        "action": req["action"],
                    }
                    # Track that this request is now dispatched to this extension
                    self._dispatched_to[req_id] = ext_key
                    break

        # ─── Token Pool Pre-fetch ───
        # If no real work pending, pre-fetch tokens to keep the pool topped up.
        # This ensures 15 parallel slots can grab cached tokens instantly.
        if response_data["work"] is None:
            now = time.time()
            for account in ext_accounts:
                pool = self._token_pool.get(account, deque())
                # Count valid (non-expired) tokens in pool
                valid_count = sum(1 for t in pool if (now - t["ts"]) < self.TOKEN_MAX_AGE)
                # Count prefetch requests already in flight for this account
                in_flight = sum(1 for a in self._prefetch_accounts.values() if a == account)
                if valid_count + in_flight < self.TOKEN_POOL_TARGET:
                    self._request_counter += 1
                    prefetch_id = f"prefetch_{self._request_counter}_{int(time.time())}"
                    self._prefetch_accounts[prefetch_id] = account
                    response_data["work"] = {
                        "request_id": prefetch_id,
                        "account": account,
                        "action": "IMAGE_GENERATION",
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
        error = data.get("error", "")

        # ─── Handle pre-fetch results (token pool) ───
        if request_id.startswith("prefetch_"):
            account = self._prefetch_accounts.pop(request_id, "")
            if data.get("token") and account and not error:
                pool = self._token_pool.setdefault(account, deque())
                pool.append({
                    "token": data["token"],
                    "access_token": data.get("access_token"),
                    "project_id": data.get("project_id"),
                    "ts": time.time(),
                })
                # Also cache project ID
                pid = data.get("project_id")
                if account and pid:
                    self._project_ids[account] = pid
                self._tokens_received += 1
                self._log(f"[Bridge] Pool: cached token for {account} (pool size: {len(pool)})")
            return web.json_response({"ok": True}, headers={"Access-Control-Allow-Origin": "*"})

        # ─── Re-route on no_labs_tab errors ───
        # If extension couldn't find the tab for this account, don't fail the
        # request — put it back so a DIFFERENT extension instance can try.
        if error and "no_labs_tab" in str(error):
            req = self._pending_requests.get(request_id)
            if req and req.get("future") and not req["future"].done():
                # Mark which extension failed — use the dispatched_to tracking
                failed_ext_key = self._dispatched_to.pop(request_id, None)
                if failed_ext_key:
                    req.setdefault("_failed_ext_keys", set()).add(failed_ext_key)

                req["_reroute_count"] = req.get("_reroute_count", 0) + 1

                # If too many reroutes (all extensions tried), give up
                if req["_reroute_count"] >= 6:
                    self._pending_requests.pop(request_id, None)
                    req["future"].set_result({
                        "token": None, "access_token": None,
                        "email": None, "project_id": None,
                        "error": f"all_extensions_failed: {error}",
                    })
                    self._log(f"[Bridge] Request {request_id} FAILED after {req['_reroute_count']} reroutes: {error}")
                else:
                    # Leave request in _pending_requests — DON'T pop, DON'T resolve
                    # Next poll from a DIFFERENT extension will pick it up
                    self._log(f"[Bridge] Re-queuing {request_id} (attempt #{req['_reroute_count']}): {error}")

                return web.json_response({"ok": True}, headers={"Access-Control-Allow-Origin": "*"})

        # ─── Normal result (success or non-routing error) ───
        req = self._pending_requests.pop(request_id, None)
        self._dispatched_to.pop(request_id, None)

        if req and req.get("future") and not req["future"].done():
            # For special actions, pass through relevant fields
            action = req.get("action", "")
            if action.startswith("DOWNLOAD_MEDIA:"):
                result = {
                    "cdn_url": data.get("cdn_url"),
                    "base64_data": data.get("base64_data"),
                    "content_type": data.get("content_type"),
                    "size": data.get("size"),
                    "error": data.get("error"),
                }
            elif action == "GET_COOKIES":
                result = {
                    "cookies": data.get("cookies"),
                    "error": data.get("error"),
                }
            else:
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
        """Extension reports connected accounts. MERGE (don't replace) into global dict."""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False}, status=400)

        accounts = data.get("accounts", [])
        # MERGE — each Chrome profile reports its own accounts separately.
        # Don't clear() — that erases accounts from other profiles.
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
