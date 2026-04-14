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
import base64
import json
import mimetypes
import os
import random
import time
import uuid
from typing import Optional, Dict, Any, List

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
UPLOAD_IMAGE_URL = "https://aisandbox-pa.googleapis.com/v1/flow/uploadImage"
VIDEO_REFERENCE_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoReferenceImages"
VIDEO_START_IMAGE_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartImage"
VIDEO_POLL_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus"


def _parse_api_error(status_code: int, resp_text: str) -> str:
    """Parse Google Labs API error response into a clear, human-readable message."""
    text_lower = (resp_text or "").lower()

    # Try to extract structured error message from JSON
    detail = ""
    try:
        err_json = json.loads(resp_text)
        if isinstance(err_json, dict):
            err_obj = err_json.get("error", err_json)
            detail = (
                err_obj.get("message", "")
                or err_obj.get("details", "")
                or err_obj.get("status", "")
            )
            if isinstance(detail, list) and detail:
                detail = str(detail[0])
            detail = str(detail).strip()
    except (json.JSONDecodeError, TypeError, KeyError):
        detail = resp_text[:200].strip()

    detail_lower = detail.lower() if detail else text_lower

    # ── 403 Errors ──
    if status_code == 403:
        if "recaptcha" in text_lower:
            return "⛔ reCAPTCHA Score Too Low — Google rejected the token (score below threshold). Tab may need reload."
        if "quota" in text_lower or "rate" in text_lower:
            return "⛔ Rate Limited (403) — Account quota exceeded or too many requests."
        if "permission" in text_lower or "forbidden" in text_lower:
            return f"⛔ Access Denied (403) — Account doesn't have permission. {detail[:100]}"
        return f"⛔ Forbidden (403) — {detail[:150] or 'Google rejected the request.'}"

    # ── 401 Errors ──
    if status_code == 401:
        return "🔑 Access Token Expired (401) — Session expired, need fresh auth from extension."

    # ── 400 Errors (most common, multiple causes) ──
    if status_code == 400:
        if "recaptcha" in text_lower:
            return "⚠️ reCAPTCHA Token Expired/Invalid (400) — Token was stale or malformed by the time API received it."
        if any(k in text_lower for k in ("expired", "token_expired", "invalid_token")):
            return "🔑 Access Token Expired (400) — Auth session needs refresh."
        if any(k in text_lower for k in ("project", "project_id", "project not found")):
            return "📁 Invalid Project ID (400) — Project not found or was deleted. Will auto-create on retry."
        if any(k in text_lower for k in (
            "safety", "blocked", "policy", "filter", "harmful",
            "inappropriate", "violat", "content_filter", "responsible_ai",
        )):
            return f"🚫 Prompt Blocked by Content Filter (400) — Google's safety filter rejected this prompt."
        if any(k in text_lower for k in ("invalid", "malformed", "parse", "field")):
            return f"❌ Malformed Request (400) — {detail[:150]}"
        # Generic 400 with detail
        return f"⚠️ Bad Request (400) — {detail[:200] or 'Unknown cause.'}"

    # ── 429 Rate Limit ──
    if status_code == 429:
        return "🕐 Rate Limited (429) — Too many requests. Account needs cooldown."

    # ── 500+ Server Errors ──
    if status_code >= 500:
        return f"🔧 Google Server Error ({status_code}) — Temporary issue, will retry."

    # ── Fallback ──
    return f"HTTP {status_code}: {detail[:200] or resp_text[:200]}"


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


def _normalize_video_sub_mode(video_sub_mode="", ref_path=None, start_image_path=None, end_image_path=None):
    """Determine video sub-mode from explicit value or infer from provided paths."""
    raw = str(video_sub_mode or "").strip().lower()
    valid_modes = {"text_to_video", "ingredients", "frames_start", "frames_start_end"}
    if raw in valid_modes:
        return raw
    if end_image_path and start_image_path:
        return "frames_start_end"
    if start_image_path:
        return "frames_start"
    if ref_path:
        return "ingredients"
    return "text_to_video"


def _resolve_video_model_for_sub_mode(video_sub_mode, model="", video_model="", ratio=""):
    """Pick the correct video model key based on sub-mode and quality tier."""
    source = str(video_model or model or "").strip().lower()
    # Determine quality tier
    if "lite" in source:
        tier = "lite"
    elif "lower pri" in source or "relaxed" in source:
        tier = "lower_pri"
    elif "quality" in source:
        tier = "quality"
    else:
        tier = "fast"

    # Model key lookup table (matches bot_engine.py VIDEO_MODEL_KEYS)
    model_keys = {
        ("text_to_video", "fast"): "veo_3_1_t2v_fast_ultra",
        ("text_to_video", "lite"): "veo_3_1_t2v_lite",
        ("text_to_video", "lower_pri"): "veo_3_1_t2v_fast_ultra_relaxed",
        ("text_to_video", "quality"): "veo_3_1_t2v",
        ("ingredients", "fast"): "veo_3_1_r2v_fast_landscape_ultra",
        ("ingredients", "lite"): "veo_3_1_r2v_fast_landscape_ultra",
        ("ingredients", "lower_pri"): "veo_3_1_r2v_fast_landscape_ultra_relaxed",
        ("frames_start", "fast"): "veo_3_1_i2v_s_fast_ultra",
        ("frames_start", "lite"): "veo_3_1_i2v_s_fast_ultra",
        ("frames_start", "lower_pri"): "veo_3_1_i2v_s_fast_ultra_relaxed",
        ("frames_start_end", "fast"): "veo_3_1_i2v_s_fast_ultra_fl",
        ("frames_start_end", "lite"): "veo_3_1_i2v_s_fast_ultra_fl",
        ("frames_start_end", "lower_pri"): "veo_3_1_i2v_s_fast_fl_ultra_relaxed",
    }

    key = model_keys.get((video_sub_mode, tier))
    if key:
        return key

    # For reference with aspect-ratio-specific models
    if video_sub_mode == "ingredients":
        api_ratio = _resolve_video_ratio(ratio)
        ratio_map = {
            "VIDEO_ASPECT_RATIO_LANDSCAPE": "veo_3_1_r2v_fast_landscape_ultra",
            "VIDEO_ASPECT_RATIO_PORTRAIT": "veo_3_1_r2v_fast_portrait_ultra",
            "VIDEO_ASPECT_RATIO_SQUARE": "veo_3_1_r2v_fast_square_ultra",
        }
        return ratio_map.get(api_ratio, "veo_3_1_r2v_fast_landscape_ultra")

    return "veo_3_1_t2v_fast_ultra"


# Video endpoint lookup
VIDEO_ENDPOINTS = {
    "text_to_video": VIDEO_API_URL,
    "ingredients": VIDEO_REFERENCE_URL,
    "frames_start": VIDEO_START_IMAGE_URL,
    "frames_start_end": "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartAndEndImage",
}


class ExtensionWorker:
    """A lightweight worker that uses the bridge for tokens and makes direct API calls."""

    # Class-level lock per account: prevents 7 workers all trying to create project at once
    _project_locks: Dict[str, asyncio.Lock] = {}
    # Class-level reference upload cache: (project_id, file_path) -> media_name
    _reference_cache: Dict[tuple, str] = {}
    _reference_cache_locks: Dict[tuple, asyncio.Lock] = {}

    def __init__(self, slot_id: str, account_email: str, bridge: ExtensionBridge, log_fn):
        self.slot_id = slot_id
        self.account_email = account_email
        self._bridge = bridge
        self._log = log_fn
        self.is_busy = False
        self.jobs_completed = 0

    async def _upload_reference_image(self, access_token, project_id, file_path):
        """Upload a single reference image via aiohttp, return media name."""
        if not file_path or not os.path.exists(file_path):
            raise RuntimeError(f"Reference file not found: {file_path}")

        with open(file_path, "rb") as f:
            image_bytes_b64 = base64.b64encode(f.read()).decode("utf-8")

        file_name = os.path.basename(file_path)
        mime_type, _ = mimetypes.guess_type(file_path)
        mime_type = mime_type or "image/jpeg"

        body = {
            "clientContext": {
                "projectId": project_id,
                "tool": "PINHOLE",
                "sessionId": f";{int(time.time() * 1000)}",
            },
            "fileName": file_name,
            "mimeType": mime_type,
            "imageBytes": image_bytes_b64,
            "isHidden": False,
            "isUserUploaded": True,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                UPLOAD_IMAGE_URL,
                headers={
                    "content-type": "text/plain;charset=UTF-8",
                    "authorization": f"Bearer {access_token}",
                },
                data=json.dumps(body),
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                resp_text = await resp.text()
                if not resp.ok:
                    raise RuntimeError(f"Upload failed: {_parse_api_error(resp.status, resp_text)}")

                try:
                    data = json.loads(resp_text)
                except json.JSONDecodeError:
                    raise RuntimeError(f"Upload response not JSON: {resp_text[:200]}")

                media_name = (
                    data.get("name")
                    or data.get("mediaName")
                    or (data.get("media", {}).get("name") if isinstance(data.get("media"), dict) else "")
                    or (data["media"][0].get("name", "") if isinstance(data.get("media"), list) and data["media"] else "")
                )
                if not media_name:
                    raise RuntimeError(f"Upload response missing media name: {resp_text[:200]}")

                self._log(f"[{self.slot_id}] Uploaded reference: {file_name} -> {media_name}")
                return media_name

    async def _upload_and_cache_reference(self, access_token, project_id, file_path):
        """Upload reference image with cache + lock to avoid duplicate uploads."""
        cache_key = (project_id, os.path.abspath(file_path))

        # Fast path: already cached
        cached = ExtensionWorker._reference_cache.get(cache_key)
        if cached:
            self._log(f"[{self.slot_id}] Reference cached: {os.path.basename(file_path)}")
            return cached

        # Get or create lock for this specific file+project
        if cache_key not in ExtensionWorker._reference_cache_locks:
            ExtensionWorker._reference_cache_locks[cache_key] = asyncio.Lock()
        lock = ExtensionWorker._reference_cache_locks[cache_key]

        async with lock:
            # Re-check after acquiring lock
            cached = ExtensionWorker._reference_cache.get(cache_key)
            if cached:
                return cached

            media_name = await self._upload_reference_image(access_token, project_id, file_path)
            ExtensionWorker._reference_cache[cache_key] = media_name
            return media_name

    async def _upload_references(self, access_token, project_id, ref_paths):
        """Upload multiple reference images, return list of media IDs."""
        media_ids = []
        for path in ref_paths:
            path = str(path).strip()
            if not path:
                continue
            media_name = await self._upload_and_cache_reference(access_token, project_id, path)
            media_ids.append(media_name)
        return media_ids

    async def _poll_video_status(self, access_token, media_id, project_id, poll_interval=5, max_polls=60):
        """Poll video generation status until complete or failed."""
        for poll_num in range(1, max_polls + 1):
            await asyncio.sleep(poll_interval)

            poll_body = {
                "media": [{"name": media_id, "projectId": project_id}],
            }

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        VIDEO_POLL_URL,
                        headers={
                            "content-type": "text/plain;charset=UTF-8",
                            "authorization": f"Bearer {access_token}",
                        },
                        data=json.dumps(poll_body),
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        resp_text = await resp.text()
                        if not resp.ok:
                            if poll_num % 3 == 0:
                                self._log(f"[{self.slot_id}] Poll {poll_num} HTTP {resp.status}")
                            continue

                        data = json.loads(resp_text)
                        media_list = data.get("media", [])
                        if not media_list:
                            continue

                        media_item = media_list[0] if isinstance(media_list, list) else {}
                        media_status = (
                            media_item.get("mediaMetadata", {}).get("mediaStatus", {})
                        )
                        status = media_status.get("mediaGenerationStatus", "UNKNOWN")

                        if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                            self._log(f"[{self.slot_id}] Video complete (poll {poll_num})")
                            return "completed", None

                        if status == "MEDIA_GENERATION_STATUS_FAILED":
                            reason = (
                                media_status.get("failureReason")
                                or media_status.get("moderationResult")
                                or media_status.get("errorMessage")
                                or "server returned FAILED"
                            )
                            return "failed", str(reason)

                        if poll_num % 3 == 0:
                            self._log(f"[{self.slot_id}] Video generating... (poll {poll_num})")

            except Exception as e:
                if poll_num % 3 == 0:
                    self._log(f"[{self.slot_id}] Poll {poll_num} error: {str(e)[:100]}")

        return "timeout", f"Video timed out after {max_polls * poll_interval}s"

    async def generate_image(self, prompt, model, ratio, references=None, ref_paths=None):
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

            # If no project ID, resolve with lock (so only 1 worker creates per account)
            if not project_id:
                project_id = await self._resolve_project_id(access_token)

            if not project_id:
                return None, "📁 No project ID available — open a project in labs.google/fx/tools/flow"

            # Upload reference images if file paths provided
            media_ids = list(references or [])
            if ref_paths:
                try:
                    uploaded = await self._upload_references(access_token, project_id, ref_paths)
                    media_ids.extend(uploaded)
                except Exception as e:
                    return None, f"Reference upload failed: {str(e)[:200]}"

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
                         for ref_id in media_ids]
                        if media_ids else []
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
                        return None, _parse_api_error(resp.status, resp_text)

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

    async def generate_video(
        self, prompt, model, ratio,
        video_sub_mode="text_to_video",
        ref_path=None, start_image_path=None, end_image_path=None,
    ):
        """Generate video via direct API call. Supports text-to-video, reference, and image-to-video."""
        self.is_busy = True
        try:
            # Determine sub-mode from explicit param or inferred from paths
            sub_mode = _normalize_video_sub_mode(
                video_sub_mode=video_sub_mode,
                ref_path=ref_path,
                start_image_path=start_image_path,
                end_image_path=end_image_path,
            )

            api_ratio = _resolve_video_ratio(ratio)
            api_model = _resolve_video_model_for_sub_mode(sub_mode, model, model, ratio)
            endpoint = VIDEO_ENDPOINTS.get(sub_mode, VIDEO_API_URL)
            seed = random.randint(100000, 999999)
            batch_id = str(uuid.uuid4())

            self._log(f"[{self.slot_id}] Video: {sub_mode}, {api_model}, {api_ratio}")

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

            # If no project ID, resolve with lock
            if not project_id:
                project_id = await self._resolve_project_id(access_token)

            if not project_id:
                return None, "📁 No project ID available — open a project in labs.google/fx/tools/flow"

            # Upload reference/start/end images if file paths provided
            ref_media_id = None
            start_media_id = None
            end_media_id = None

            try:
                if ref_path and sub_mode == "ingredients":
                    ref_media_id = await self._upload_and_cache_reference(
                        access_token, project_id, ref_path
                    )
                if start_image_path and sub_mode in ("frames_start", "frames_start_end"):
                    start_media_id = await self._upload_and_cache_reference(
                        access_token, project_id, start_image_path
                    )
                if end_image_path and sub_mode == "frames_start_end":
                    end_media_id = await self._upload_and_cache_reference(
                        access_token, project_id, end_image_path
                    )
            except Exception as e:
                return None, f"Image upload failed: {str(e)[:200]}"

            # Build request body
            client_context = {
                "projectId": project_id,
                "tool": "PINHOLE",
                "userPaygateTier": "PAYGATE_TIER_TWO",
                "sessionId": f";{int(time.time() * 1000)}",
            }
            if token:
                client_context["recaptchaContext"] = {
                    "token": token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                }

            request_obj = {
                "aspectRatio": api_ratio,
                "seed": seed,
                "textInput": {
                    "structuredPrompt": {"parts": [{"text": prompt}]},
                },
                "videoModelKey": api_model,
                "metadata": {},
            }

            # Add reference/start/end image fields based on sub-mode
            if sub_mode == "ingredients" and ref_media_id:
                request_obj["referenceImages"] = [{
                    "mediaId": ref_media_id,
                    "imageUsageType": "IMAGE_USAGE_TYPE_ASSET",
                }]
            if sub_mode in ("frames_start", "frames_start_end") and start_media_id:
                request_obj["startImage"] = {
                    "mediaId": start_media_id,
                    "cropCoordinates": {"top": 0, "left": 0, "bottom": 1, "right": 1},
                }
            if sub_mode == "frames_start_end" and end_media_id:
                request_obj["endImage"] = {
                    "mediaId": end_media_id,
                    "cropCoordinates": {"top": 0, "left": 0, "bottom": 1, "right": 1},
                }

            body = {
                "mediaGenerationContext": {"batchId": batch_id},
                "clientContext": client_context,
                "requests": [request_obj],
                "useV2ModelConfig": True,
            }

            # Submit video generation request
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    endpoint,
                    headers={
                        "content-type": "text/plain;charset=UTF-8",
                        "authorization": f"Bearer {access_token}",
                    },
                    data=json.dumps(body),
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    resp_text = await resp.text()

                    if not resp.ok:
                        return None, _parse_api_error(resp.status, resp_text)

                    try:
                        data = json.loads(resp_text)
                    except json.JSONDecodeError:
                        return None, f"Invalid JSON: {resp_text[:200]}"

            # Extract media_id for polling
            media_list = data.get("media", []) if isinstance(data, dict) else []
            workflows = data.get("workflows", []) if isinstance(data, dict) else []
            media_id = ""
            if media_list and isinstance(media_list, list):
                media_id = media_list[0].get("name", "") if isinstance(media_list[0], dict) else ""
            if not media_id and workflows and isinstance(workflows, list):
                media_id = workflows[0].get("metadata", {}).get("primaryMediaId", "") if isinstance(workflows[0], dict) else ""

            if not media_id:
                # No media_id means immediate response (unlikely for video) or error
                self.jobs_completed += 1
                return data, None

            # Poll until video is complete
            self._log(f"[{self.slot_id}] Video submitted, polling: {media_id[:30]}...")
            poll_status, poll_error = await self._poll_video_status(
                access_token, media_id, project_id,
                poll_interval=5, max_polls=60,
            )

            if poll_status == "completed":
                # Return data with media_id so _download_and_save can build the redirect URL
                data["_video_media_id"] = media_id
                self.jobs_completed += 1
                return data, None
            else:
                return None, f"Video {poll_status}: {poll_error or 'unknown'}"

        except Exception as e:
            return None, f"Exception: {str(e)[:300]}"
        finally:
            self.is_busy = False

    async def _resolve_project_id(self, access_token) -> Optional[str]:
        """Resolve project ID with per-account lock — only one worker fetches at a time."""
        # Check cache again (another worker may have resolved it while we waited)
        cached = self._bridge.get_project_id(self.account_email)
        if cached:
            return cached

        # Get or create lock for this account
        if self.account_email not in ExtensionWorker._project_locks:
            ExtensionWorker._project_locks[self.account_email] = asyncio.Lock()

        lock = ExtensionWorker._project_locks[self.account_email]

        async with lock:
            # Double-check cache after acquiring lock
            cached = self._bridge.get_project_id(self.account_email)
            if cached:
                return cached

            self._log(f"[{self.slot_id}] Resolving project ID for {self.account_email}...")
            project_id = await self._create_project(access_token)
            if project_id:
                self._bridge.set_project_id(self.account_email, project_id)
                self._log(f"[{self.slot_id}] ✓ Project ID resolved: {project_id}")
            else:
                self._log(f"[{self.slot_id}] ✗ Could not resolve project ID")
            return project_id

    async def _create_project(self, access_token):
        """Get or create a Labs project via multiple API fallbacks."""
        headers = {"authorization": f"Bearer {access_token}"}

        # ── Method 1: List projects via aisandbox API ──
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://aisandbox-pa.googleapis.com/v1/projects",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.ok:
                        data = await resp.json()
                        projects = data.get("projects", data.get("project", []))
                        if isinstance(projects, list) and projects:
                            import re as _re
                            pname = projects[0].get("name", "") or projects[0].get("projectId", "")
                            m = _re.search(r"([a-z0-9-]{16,})", pname, _re.IGNORECASE)
                            if m:
                                pid = m.group(1)
                                self._log(f"[{self.slot_id}] Project from API: {pid}")
                                return pid
        except Exception as e:
            self._log(f"[{self.slot_id}] Projects API error: {str(e)[:80]}")

        # ── Method 2: listFlows via trpc ──
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://labs.google/fx/api/trpc/backbone.listFlows",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.ok:
                        data = await resp.json()
                        flows = data.get("result", {}).get("data", {}).get("flows", [])
                        if flows:
                            pid = flows[0].get("name", "")
                            if pid:
                                self._log(f"[{self.slot_id}] Project from listFlows: {pid}")
                                return pid
        except Exception:
            pass

        # ── Method 3: Ask extension to click "New project" ──
        self._bridge.send_command("new_project", self.account_email)
        await asyncio.sleep(5)
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
            await asyncio.sleep(4)
            connected = self._bridge.get_connected_accounts()
            prev_count = len(connected)

            # Wait up to 20s for more accounts to trickle in
            stable_rounds = 0
            for _ in range(20):
                if self.qm.stop_requested or self.qm.force_stop_requested:
                    return
                await asyncio.sleep(1)
                connected = self._bridge.get_connected_accounts()
                if len(connected) == prev_count and prev_count > 0:
                    stable_rounds += 1
                    if stable_rounds >= 4:
                        break  # count stable for 4s — all profiles reported
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

                # ─── Dynamic account discovery ───
                # Check for new accounts that connected after initial worker creation
                current_accounts = self._bridge.get_connected_accounts()
                for email, info in current_accounts.items():
                    if email and email not in self._workers:
                        account_name = email or info.get("name", "unknown")
                        workers = []
                        for idx in range(1, slots_per_account + 1):
                            slot_id = f"{account_name}#e{idx}"
                            worker = ExtensionWorker(slot_id, account_name, self._bridge, self._log)
                            workers.append(worker)
                        self._workers[account_name] = workers
                        self._log(
                            f"[ExtMode] New account detected: {account_name} — "
                            f"{len(workers)} worker(s) added dynamically."
                        )

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
                    # Short timeout — don't wait forever
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*self._active_tasks, return_exceptions=True),
                            timeout=3.0,
                        )
                    except asyncio.TimeoutError:
                        self._log("[ExtMode] Some tasks didn't cancel in 3s — continuing.")
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

    async def _run_pipeline_job(self, worker: ExtensionWorker, job: dict):
        """Execute a pipeline job: Step 1 = generate image, Step 2 = generate video from it."""
        job_id = job["id"]
        prompt = job.get("prompt", "")
        model = job.get("model", "")
        queue_no = job.get("output_index") or job.get("queue_no")
        video_prompt = str(job.get("video_prompt") or "animate").strip() or "animate"
        video_model = str(job.get("video_model") or "").strip()
        video_sub_mode = str(job.get("video_sub_mode") or "ingredients").strip()
        video_ratio = str(job.get("video_ratio") or job.get("aspect_ratio") or "").strip()
        ratio = job.get("aspect_ratio", "IMAGE_ASPECT_RATIO_LANDSCAPE")

        # Parse ref_paths for image step
        ref_paths = []
        ref_paths_raw = job.get("ref_paths") or ""
        if isinstance(ref_paths_raw, str) and ref_paths_raw.strip():
            try:
                parsed = json.loads(ref_paths_raw)
                if isinstance(parsed, list):
                    ref_paths = [str(p).strip() for p in parsed if str(p).strip()]
            except (json.JSONDecodeError, TypeError):
                ref_paths = [ref_paths_raw.strip()]
        single_ref = str(job.get("ref_path") or "").strip()
        if single_ref and single_ref not in ref_paths:
            ref_paths.insert(0, single_ref)

        sub_mode = _normalize_video_sub_mode(video_sub_mode=video_sub_mode)
        if sub_mode not in ("ingredients", "frames_start"):
            sub_mode = "ingredients"

        self._log(f"[{worker.slot_id}] Pipeline: image({model}) -> video({sub_mode})")

        try:
            # ── Step 1: Generate image ──
            self._log(f"[{worker.slot_id}] Pipeline Step 1: Generating image...")
            img_result, img_error = await worker.generate_image(
                prompt, model, ratio,
                ref_paths=ref_paths if ref_paths else None,
            )

            if img_error or not img_result:
                update_job_status(job_id, "failed", account=worker.account_email, error=img_error or "Image generation failed")
                self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, img_error or "Image generation failed")
                self._log(f"[{worker.slot_id}] Pipeline Step 1 FAILED: {(img_error or '')[:200]}")
                return

            # Extract media ID from generated image
            media_list = img_result.get("media", []) if isinstance(img_result, dict) else []
            generated_media_id = ""
            for item in media_list:
                if isinstance(item, dict):
                    generated_media_id = item.get("name", "")
                    if generated_media_id:
                        break

            if not generated_media_id:
                workflows = img_result.get("workflows", []) if isinstance(img_result, dict) else []
                if workflows and isinstance(workflows[0], dict):
                    generated_media_id = workflows[0].get("metadata", {}).get("primaryMediaId", "")

            if not generated_media_id:
                update_job_status(job_id, "failed", account=worker.account_email, error="Pipeline: no media ID from image generation")
                self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, "Pipeline: no media ID from image generation")
                return

            self._log(f"[{worker.slot_id}] Step 1 done: {generated_media_id[:20]}...")

            # ── Step 2: Generate video from image ──
            self._log(f"[{worker.slot_id}] Pipeline Step 2: Generating video ({sub_mode})...")

            # Use the generated image media ID as reference or start image
            # We pass it directly as a media ID (not file path) — need to build generate_video call manually
            api_ratio = _resolve_video_ratio(video_ratio)
            api_model = _resolve_video_model_for_sub_mode(sub_mode, video_model, video_model, video_ratio)
            endpoint = VIDEO_ENDPOINTS.get(sub_mode, VIDEO_API_URL)
            seed = random.randint(100000, 999999)
            batch_id = str(uuid.uuid4())

            # Get fresh token for video step
            bridge_result = await worker._bridge.request_token(
                worker.account_email, "VIDEO_GENERATION", timeout=30
            )
            if bridge_result.get("error"):
                update_job_status(job_id, "failed", account=worker.account_email, error=f"Pipeline Step 2 bridge error: {bridge_result['error']}")
                self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, f"Pipeline Step 2 bridge error: {bridge_result['error']}")
                return

            token = bridge_result.get("token")
            access_token = bridge_result.get("access_token")
            project_id = bridge_result.get("project_id") or worker._bridge.get_project_id(worker.account_email)

            if not access_token:
                update_job_status(job_id, "failed", account=worker.account_email, error="Pipeline Step 2: no access token")
                self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, "Pipeline Step 2: no access token")
                return

            if not project_id:
                project_id = await worker._resolve_project_id(access_token)

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

            request_obj = {
                "aspectRatio": api_ratio,
                "seed": seed,
                "textInput": {
                    "structuredPrompt": {"parts": [{"text": video_prompt}]},
                },
                "videoModelKey": api_model,
                "metadata": {},
            }

            if sub_mode == "ingredients":
                request_obj["referenceImages"] = [{
                    "mediaId": generated_media_id,
                    "imageUsageType": "IMAGE_USAGE_TYPE_ASSET",
                }]
            elif sub_mode == "frames_start":
                request_obj["startImage"] = {
                    "mediaId": generated_media_id,
                    "cropCoordinates": {"top": 0, "left": 0, "bottom": 1, "right": 1},
                }

            body = {
                "mediaGenerationContext": {"batchId": batch_id},
                "clientContext": client_context,
                "requests": [request_obj],
                "useV2ModelConfig": True,
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    endpoint,
                    headers={
                        "content-type": "text/plain;charset=UTF-8",
                        "authorization": f"Bearer {access_token}",
                    },
                    data=json.dumps(body),
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    resp_text = await resp.text()
                    if not resp.ok:
                        err = _parse_api_error(resp.status, resp_text)
                        update_job_status(job_id, "failed", account=worker.account_email, error=f"Pipeline Step 2: {err}")
                        self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, f"Pipeline Step 2: {err}")
                        return
                    data = json.loads(resp_text)

            # Extract video media_id
            vid_media_list = data.get("media", []) if isinstance(data, dict) else []
            vid_media_id = ""
            if vid_media_list and isinstance(vid_media_list, list) and isinstance(vid_media_list[0], dict):
                vid_media_id = vid_media_list[0].get("name", "")
            if not vid_media_id:
                vid_workflows = data.get("workflows", []) if isinstance(data, dict) else []
                if vid_workflows and isinstance(vid_workflows[0], dict):
                    vid_media_id = vid_workflows[0].get("metadata", {}).get("primaryMediaId", "")

            if not vid_media_id:
                update_job_status(job_id, "failed", account=worker.account_email, error="Pipeline Step 2: no video media ID")
                self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, "Pipeline Step 2: no video media ID")
                return

            # Poll video
            self._log(f"[{worker.slot_id}] Video submitted, polling: {vid_media_id[:20]}...")
            poll_status, poll_error = await worker._poll_video_status(
                access_token, vid_media_id, project_id,
                poll_interval=5, max_polls=60,
            )

            if poll_status != "completed":
                err = f"Pipeline video {poll_status}: {poll_error or 'unknown'}"
                update_job_status(job_id, "failed", account=worker.account_email, error=err)
                self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, err)
                return

            # Download video
            data["_video_media_id"] = vid_media_id
            output_path, dl_error = await self._download_and_save(
                worker, job_id, data, queue_no=queue_no
            )

            if dl_error:
                update_job_status(job_id, "failed", account=worker.account_email, error=f"Pipeline download: {dl_error}")
                self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, f"Pipeline download: {dl_error}")
                return

            update_job_status(job_id, "completed", account=worker.account_email)
            self.qm.signals.job_updated.emit(job_id, "completed", worker.account_email, "")
            self._log(f"[{worker.slot_id}] Pipeline job {job_id[:6]}... completed! ({output_path})")

        except asyncio.CancelledError:
            update_job_status(job_id, "pending", account="")
            self.qm.signals.job_updated.emit(job_id, "pending", "", "")
        except Exception as e:
            err = str(e)[:300]
            update_job_status(job_id, "failed", account=worker.account_email, error=err)
            self.qm.signals.job_updated.emit(job_id, "failed", worker.account_email, err)
            self._log(f"[{worker.slot_id}] Pipeline FAILED: {err}")

    async def _run_job(self, worker: ExtensionWorker, job: dict):
        """Execute a single job with retries."""
        job_id = job["id"]
        job_type = job.get("job_type", "image")
        prompt = job.get("prompt", "")
        model = job.get("model", "")
        queue_no = job.get("output_index") or job.get("queue_no")

        # Pipeline jobs have their own 2-step flow
        if job_type == "pipeline":
            return await self._run_pipeline_job(worker, job)

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
                    # Extract video-specific fields from job
                    video_sub_mode = str(job.get("video_sub_mode") or "").strip() or "text_to_video"
                    ref_path = str(job.get("ref_path") or "").strip() or None
                    start_image_path = str(job.get("start_image_path") or "").strip() or None
                    end_image_path = str(job.get("end_image_path") or "").strip() or None
                    # ref_paths may also contain the reference for video
                    if not ref_path:
                        ref_paths_raw = job.get("ref_paths") or ""
                        if isinstance(ref_paths_raw, str) and ref_paths_raw.strip():
                            try:
                                parsed = json.loads(ref_paths_raw)
                                if isinstance(parsed, list) and parsed:
                                    ref_path = str(parsed[0]).strip()
                            except (json.JSONDecodeError, TypeError):
                                ref_path = ref_paths_raw.strip()

                    result, error = await worker.generate_video(
                        prompt, video_model, ratio,
                        video_sub_mode=video_sub_mode,
                        ref_path=ref_path,
                        start_image_path=start_image_path,
                        end_image_path=end_image_path,
                    )
                else:
                    ratio = job.get("aspect_ratio", "IMAGE_ASPECT_RATIO_LANDSCAPE")
                    # Parse ref_paths from job (JSON list or single path)
                    ref_paths = []
                    ref_paths_raw = job.get("ref_paths") or ""
                    if isinstance(ref_paths_raw, str) and ref_paths_raw.strip():
                        try:
                            parsed = json.loads(ref_paths_raw)
                            if isinstance(parsed, list):
                                ref_paths = [str(p).strip() for p in parsed if str(p).strip()]
                        except (json.JSONDecodeError, TypeError):
                            ref_paths = [ref_paths_raw.strip()]
                    # Also check single ref_path
                    single_ref = str(job.get("ref_path") or "").strip()
                    if single_ref and single_ref not in ref_paths:
                        ref_paths.insert(0, single_ref)

                    result, error = await worker.generate_image(
                        prompt, model, ratio,
                        references=job.get("reference_media_ids"),
                        ref_paths=ref_paths if ref_paths else None,
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

                # no_recaptcha_enterprise → tab doesn't have reCAPTCHA loaded
                # Extension already auto-reloads the tab, just wait for it
                if "no_recaptcha_enterprise" in last_error:
                    self._log(f"[{worker.slot_id}] Tab has no reCAPTCHA — waiting for auto-reload...")
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        update_job_status(job_id, "pending", account="")
                        self.qm.signals.job_updated.emit(job_id, "pending", "", "")
                        return
                    await asyncio.sleep(8)  # wait for tab reload + reCAPTCHA init
                    continue

                # reCAPTCHA score/token failure → reload tab
                if "recaptcha" in last_error.lower() or "captcha" in last_error.lower():
                    self._bridge.send_command("reload_tab", worker.account_email)
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        update_job_status(job_id, "pending", account="")
                        self.qm.signals.job_updated.emit(job_id, "pending", "", "")
                        return
                    await asyncio.sleep(5)
                    continue

                # 400 "invalid argument" — same prompt won't fix on retry, fail after 2 attempts
                if "400" in last_error and attempt >= 1:
                    self._log(f"[{worker.slot_id}] Same 400 error twice — skipping prompt.")
                    break  # exit retry loop → mark as failed

                if attempt < max_retries:
                    # Check stop before retry sleep
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        update_job_status(job_id, "pending", account="")
                        self.qm.signals.job_updated.emit(job_id, "pending", "", "")
                        return
                    await asyncio.sleep(min(10 * (attempt + 1), 15))  # cap at 15s

            except asyncio.CancelledError:
                # Task was cancelled by stop — re-queue job
                update_job_status(job_id, "pending", account="")
                self.qm.signals.job_updated.emit(job_id, "pending", "", "")
                return

            except Exception as e:
                last_error = str(e)[:300]
                self._log(f"[{worker.slot_id}] Attempt {attempt + 1} exception: {last_error}")
                if attempt < max_retries:
                    if self.qm.stop_requested or self.qm.force_stop_requested:
                        update_job_status(job_id, "pending", account="")
                        self.qm.signals.job_updated.emit(job_id, "pending", "", "")
                        return
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

        # For video: use _video_media_id set after polling
        if not fife_url:
            video_media_id = api_data.get("_video_media_id", "") if isinstance(api_data, dict) else ""
            if video_media_id:
                fife_url = (
                    f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?"
                    f"name={video_media_id}"
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
