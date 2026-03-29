import os
import sys
from pathlib import Path


APP_DIR_NAME = "G-Labs"


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_bundle_root() -> Path:
    return Path(getattr(sys, "_MEIPASS", get_project_root())).resolve()


def get_app_data_dir() -> Path:
    if sys.platform == "darwin":
        base = (Path.home() / "Library" / "Application Support" / APP_DIR_NAME)
    elif getattr(sys, "frozen", False):
        if sys.platform.startswith("win"):
            base_root = Path(os.environ.get("APPDATA") or (Path.home() / "AppData" / "Roaming"))
        else:
            base_root = Path(os.environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share"))
        base = base_root / APP_DIR_NAME
    else:
        base = get_project_root()

    (base / "data" / "sessions").mkdir(parents=True, exist_ok=True)
    (base / "data" / "session_clones").mkdir(parents=True, exist_ok=True)
    (base / "data" / "db").mkdir(parents=True, exist_ok=True)
    (base / "outputs").mkdir(parents=True, exist_ok=True)
    return base.resolve()


def get_data_dir() -> Path:
    data_dir = get_app_data_dir() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_sessions_dir() -> Path:
    sessions_dir = get_data_dir() / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    return sessions_dir


def get_session_clones_dir() -> Path:
    clones_dir = get_data_dir() / "session_clones"
    clones_dir.mkdir(parents=True, exist_ok=True)
    return clones_dir


def get_outputs_dir() -> Path:
    outputs_dir = get_app_data_dir() / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    return outputs_dir


def get_jobs_db_path() -> Path:
    return get_data_dir() / "jobs.db"


def get_project_cache_path() -> Path:
    return get_data_dir() / "project_cache.json"
