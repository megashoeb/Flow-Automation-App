import importlib
import os
import sys
from pathlib import Path

from src.core.app_paths import get_app_data_dir
from src.core.runtime_stdio import ensure_std_streams


ensure_std_streams()


def get_cloakbrowser_cache_dir() -> Path:
    """Return a writable CloakBrowser cache dir for dev and packaged builds."""
    if getattr(sys, "frozen", False):
        cache_dir = get_app_data_dir() / "cloakbrowser_cache"
    else:
        cache_dir = Path.home() / ".cloakbrowser"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir.resolve()


def configure_cloakbrowser_environment() -> Path:
    """Set the cache dir env var before any cloakbrowser import happens."""
    cache_dir = get_cloakbrowser_cache_dir()
    os.environ["CLOAKBROWSER_CACHE_DIR"] = str(cache_dir)
    return cache_dir


def load_cloakbrowser_api(force_reload=False):
    """
    Lazily import cloakbrowser pieces.

    Returns a dict with stable keys so callers can safely check availability
    without importing cloakbrowser at module load time.
    """
    configure_cloakbrowser_environment()

    result = {
        "available": False,
        "module": None,
        "package_version": "unknown",
        "download_module": None,
        "binary_info": None,
        "launch": None,
        "launch_async": None,
        "persistent": None,
        "persistent_async": None,
        "ensure_binary": None,
    }

    try:
        cloakbrowser = importlib.import_module("cloakbrowser")
        if force_reload:
            cloakbrowser = importlib.reload(cloakbrowser)
        download_module = importlib.import_module("cloakbrowser.download")
        if force_reload:
            download_module = importlib.reload(download_module)
    except Exception:
        return result

    result.update(
        {
            "available": True,
            "module": cloakbrowser,
            "package_version": str(getattr(cloakbrowser, "__version__", "unknown")),
            "download_module": download_module,
            "binary_info": getattr(cloakbrowser, "binary_info", None),
            "launch": getattr(cloakbrowser, "launch", None),
            "launch_async": getattr(cloakbrowser, "launch_async", None),
            "persistent": getattr(cloakbrowser, "launch_persistent_context", None),
            "persistent_async": getattr(cloakbrowser, "launch_persistent_context_async", None),
            "ensure_binary": getattr(download_module, "ensure_binary", None),
        }
    )
    return result


def is_cloakbrowser_available():
    return bool(load_cloakbrowser_api().get("available"))
