"""
Browser profile cleaner — removes accumulated junk that degrades reCAPTCHA scores.

After 3-4 days of use, cached service workers, IndexedDB tracking data, and
GPU caches make the profile look like a bot. Cleaning these while preserving
cookies and login data restores reCAPTCHA scores without reinstalling.
"""

import os
import shutil

# Root-level directories safe to delete
CLEANABLE_ROOT_DIRS = [
    "Cache",
    "Code Cache",
    "GPUCache",
    "GrShaderCache",
    "GraphiteDawnCache",
    "DawnGraphiteCache",
    "DawnWebGPUCache",
    "ShaderCache",
    "Service Worker",
    "blob_storage",
    "BrowserMetrics",
    "Crashpad",
    "Feature Engagement Tracker",
    "BudgetDatabase",
    "optimization_guide_hint_cache",
    "optimization_guide_model_and_features_store",
    "optimization_guide_prediction_model_downloads",
]

# Root-level files safe to delete
CLEANABLE_ROOT_FILES = [
    "BrowserMetrics-spare.pma",
    "CrashpadMetrics-active.pma",
]

# Default/ subdirectories safe to delete
CLEANABLE_DEFAULT_DIRS = [
    "Cache",
    "Code Cache",
    "GPUCache",
    "GrShaderCache",
    "GraphiteDawnCache",
    "DawnGraphiteCache",
    "DawnWebGPUCache",
    "ShaderCache",
    "Service Worker",
    "blob_storage",
    "File System",
    "IndexedDB",
    "BrowsingTopicsSiteData",
    "BrowsingTopicsState",
    "BudgetDatabase",
    "Feature Engagement Tracker",
    "DIPS",
]

# Default/ files safe to delete
CLEANABLE_DEFAULT_FILES = [
    "BrowsingTopicsSiteData-journal",
    "DIPS-journal",
    "DIPS-wal",
]


def _get_dir_size(path):
    """Get total size of directory in bytes."""
    total = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, f))
                except Exception:
                    pass
    except Exception:
        pass
    return total


def _remove_item(path):
    """Remove a file or directory. Returns bytes freed."""
    try:
        if os.path.isdir(path):
            size = _get_dir_size(path)
            shutil.rmtree(path, ignore_errors=True)
            return size
        elif os.path.isfile(path):
            size = os.path.getsize(path)
            os.remove(path)
            return size
    except Exception:
        pass
    return 0


def clean_profile(session_path, log_fn=None):
    """
    Clean accumulated junk from browser profile.
    Preserves cookies, login data, and session files.
    Returns (deleted_count, freed_bytes).
    """
    if not session_path or not os.path.exists(session_path):
        return 0, 0

    deleted = 0
    freed = 0

    # Clean root-level junk directories
    for name in CLEANABLE_ROOT_DIRS:
        p = os.path.join(session_path, name)
        if os.path.exists(p):
            freed += _remove_item(p)
            deleted += 1

    # Clean root-level junk files
    for name in CLEANABLE_ROOT_FILES:
        p = os.path.join(session_path, name)
        if os.path.exists(p):
            freed += _remove_item(p)
            deleted += 1

    # Clean Default/ subdirectories
    default_dir = os.path.join(session_path, "Default")
    if os.path.isdir(default_dir):
        for name in CLEANABLE_DEFAULT_DIRS:
            p = os.path.join(default_dir, name)
            if os.path.exists(p):
                freed += _remove_item(p)
                deleted += 1

        for name in CLEANABLE_DEFAULT_FILES:
            p = os.path.join(default_dir, name)
            if os.path.exists(p):
                freed += _remove_item(p)
                deleted += 1

    if log_fn and deleted > 0:
        freed_mb = freed / (1024 * 1024)
        log_fn(f"[CLEAN] Profile cleaned: {deleted} items removed, {freed_mb:.1f}MB freed.")

    return deleted, freed


def clean_derived_profiles(session_path, log_fn=None):
    """Clean _cloak, _multitab, _token_server, _shared_browser derived profiles."""
    if not session_path:
        return 0, 0
    total_deleted = 0
    total_freed = 0
    for suffix in ("_cloak", "_multitab", "_token_server", "_shared_browser"):
        derived = session_path + suffix
        if os.path.isdir(derived):
            d, f = clean_profile(derived, log_fn)
            total_deleted += d
            total_freed += f
    return total_deleted, total_freed


def needs_cleaning(session_path):
    """Check if profile needs cleaning based on cache size."""
    if not session_path or not os.path.exists(session_path):
        return False

    default_dir = os.path.join(session_path, "Default")

    # Service workers = always clean (caches Google detection scripts)
    if os.path.isdir(os.path.join(default_dir, "Service Worker")):
        return True

    # Cache > 100MB
    cache_dir = os.path.join(default_dir, "Cache")
    if os.path.isdir(cache_dir) and _get_dir_size(cache_dir) > 100 * 1024 * 1024:
        return True

    # IndexedDB > 50MB
    idb_dir = os.path.join(default_dir, "IndexedDB")
    if os.path.isdir(idb_dir) and _get_dir_size(idb_dir) > 50 * 1024 * 1024:
        return True

    # GrShaderCache or BrowserMetrics > 50MB
    for junk_dir in ("GrShaderCache", "GraphiteDawnCache", "BrowserMetrics", "Crashpad"):
        junk_path = os.path.join(session_path, junk_dir)
        if os.path.isdir(junk_path) and _get_dir_size(junk_path) > 50 * 1024 * 1024:
            return True

    return False
