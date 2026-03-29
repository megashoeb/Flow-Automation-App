import os
import urllib.request
from pathlib import Path

from src.core.cloakbrowser_support import (
    load_cloakbrowser_api,
)


def download_cloakbrowser_with_progress(cache_dir, progress_callback):
    """
    Download CloakBrowser binary with simple progress callbacks.
    Falls back to cloakbrowser's built-in ensure_binary when needed.
    """
    callback = progress_callback if callable(progress_callback) else (lambda _percent, _status: None)
    target_cache_dir = Path(cache_dir).resolve()
    target_cache_dir.mkdir(parents=True, exist_ok=True)

    try:
        cloak_api = load_cloakbrowser_api()
        cloak_binary_info = cloak_api.get("binary_info")
        cloak_ensure_binary = cloak_api.get("ensure_binary")
        cloak_download_module = cloak_api.get("download_module")

        info = cloak_binary_info() if callable(cloak_binary_info) else {}
        if bool((info or {}).get("installed")):
            callback(100, "CloakBrowser binary already installed!")
            return True

        download_mod = cloak_download_module
        if download_mod is None:
            callback(10, "Downloading CloakBrowser binary...")
            if not callable(cloak_ensure_binary):
                raise RuntimeError("CloakBrowser installer is unavailable.")
            cloak_ensure_binary()
            callback(100, "CloakBrowser binary ready!")
            return True

        callback(0, "Preparing CloakBrowser download...")
        version = None
        if info:
            version = info.get("version") or info.get("chromium_version")
        get_url = getattr(download_mod, "get_download_url", None)
        get_fallback_url = getattr(download_mod, "get_fallback_download_url", None)
        get_binary_dir = getattr(download_mod, "get_binary_dir", None)
        get_binary_path = getattr(download_mod, "get_binary_path", None)
        get_archive_ext = getattr(download_mod, "get_archive_ext", None)
        verify_download = getattr(download_mod, "_verify_download_checksum", None)
        extract_archive = getattr(download_mod, "_extract_archive", None)

        if not all([get_url, get_binary_dir, get_binary_path, get_archive_ext, extract_archive]):
            callback(10, "Downloading CloakBrowser binary...")
            if not callable(cloak_ensure_binary):
                raise RuntimeError("CloakBrowser installer is unavailable.")
            cloak_ensure_binary()
            callback(100, "CloakBrowser binary ready!")
            return True

        binary_dir = Path(get_binary_dir(version))
        binary_path = Path(get_binary_path(version))
        archive_ext = str(get_archive_ext())
        tmp_path = target_cache_dir / f"cloakbrowser_download{archive_ext}"

        urls = [get_url(version)]
        if callable(get_fallback_url):
            try:
                fallback_url = get_fallback_url(version)
                if fallback_url and fallback_url not in urls:
                    urls.append(fallback_url)
            except Exception:
                pass

        last_error = None
        for index, url in enumerate(urls, start=1):
            try:
                callback(5, f"Downloading CloakBrowser ({index}/{len(urls)})...")
                request = urllib.request.Request(url)
                with urllib.request.urlopen(request, timeout=120) as response, open(tmp_path, "wb") as handle:
                    total_size = int(response.headers.get("Content-Length", 0) or 0)
                    downloaded = 0
                    last_percent = -1
                    block_size = 1024 * 64

                    while True:
                        block = response.read(block_size)
                        if not block:
                            break
                        handle.write(block)
                        downloaded += len(block)
                        if total_size > 0:
                            percent = min(90, int((downloaded / total_size) * 90))
                            if percent != last_percent:
                                size_mb = downloaded / (1024 * 1024)
                                total_mb = total_size / (1024 * 1024)
                                callback(percent, f"Downloading CloakBrowser... {size_mb:.0f}/{total_mb:.0f} MB")
                                last_percent = percent

                callback(92, "Extracting CloakBrowser binary...")
                if callable(verify_download):
                    verify_download(tmp_path, version)
                binary_dir.parent.mkdir(parents=True, exist_ok=True)
                extract_archive(tmp_path, binary_dir, binary_path)
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

                callback(95, "Finalizing CloakBrowser setup...")
                if not callable(cloak_ensure_binary):
                    raise RuntimeError("CloakBrowser installer is unavailable.")
                cloak_ensure_binary()
                callback(100, "CloakBrowser binary ready!")
                return True
            except Exception as exc:
                last_error = exc
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                continue

        callback(-1, f"Download failed: {str(last_error)[:80]}")
        return False
    except Exception as exc:
        callback(-1, f"Download failed: {str(exc)[:80]}")
        return False
