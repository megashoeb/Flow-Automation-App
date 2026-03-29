import json
import os
import platform
import signal
import subprocess
import sys
import threading
from pathlib import Path

from src.core.app_paths import get_app_data_dir, get_data_dir, get_session_clones_dir, get_sessions_dir

# Hide console windows on Windows for all subprocess calls.
_SUBPROCESS_NO_WINDOW = {}
if platform.system() == "Windows":
    _SUBPROCESS_NO_WINDOW = {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)}


def _get_pid_file_path() -> Path:
    pid_file = get_data_dir() / "tracked_pids.json"
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    return pid_file


class ProcessTracker:
    """
    Tracks browser processes spawned by this app.
    Cleanup only targets tracked PIDs, never arbitrary system browsers.
    """

    def __init__(self, pid_file=None):
        self._lock = threading.Lock()
        self._pids = set()
        self._pid_file = Path(pid_file or _get_pid_file_path())
        self._pid_file.parent.mkdir(parents=True, exist_ok=True)

    def _persist_unlocked(self):
        try:
            if self._pids:
                tmp_path = self._pid_file.with_suffix(".tmp")
                tmp_path.write_text(json.dumps(sorted(self._pids)), encoding="utf-8")
                tmp_path.replace(self._pid_file)
            elif self._pid_file.exists():
                self._pid_file.unlink()
        except Exception:
            pass

    def save(self):
        with self._lock:
            self._persist_unlocked()

    def register(self, pid):
        try:
            normalized = int(pid)
        except Exception:
            return
        if normalized <= 0:
            return
        with self._lock:
            self._pids.add(normalized)
            self._persist_unlocked()

    def unregister(self, pid):
        try:
            normalized = int(pid)
        except Exception:
            return
        with self._lock:
            self._pids.discard(normalized)
            self._persist_unlocked()

    def is_tracked(self, pid):
        try:
            normalized = int(pid)
        except Exception:
            return False
        with self._lock:
            return normalized in self._pids

    def _terminate_pid(self, pid):
        try:
            normalized = int(pid)
        except Exception:
            return False
        if normalized <= 0:
            return False

        try:
            if platform.system() == "Windows":
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(normalized)],
                    capture_output=True,
                    timeout=5,
                    text=True,
                    **_SUBPROCESS_NO_WINDOW,
                )
            else:
                os.kill(normalized, signal.SIGTERM)
            return True
        except (ProcessLookupError, PermissionError, subprocess.TimeoutExpired):
            return False
        except Exception:
            return False

    def _candidate_data_dirs(self):
        candidates = {
            str(get_app_data_dir().resolve()),
            str(get_sessions_dir().resolve()),
            str(get_session_clones_dir().resolve()),
        }

        exe_dir = Path(sys.argv[0]).resolve().parent if sys.argv and sys.argv[0] else None
        if exe_dir is not None:
            candidates.add(str((exe_dir / "data" / "sessions").resolve()))
            candidates.add(str((exe_dir / "data" / "session_clones").resolve()))

        cwd = Path.cwd()
        candidates.add(str((cwd / "data" / "sessions").resolve()))
        candidates.add(str((cwd / "data" / "session_clones").resolve()))

        if platform.system() == "Windows":
            appdata = os.environ.get("APPDATA")
            if appdata:
                candidates.add(str((Path(appdata) / "G-Labs").resolve()))

        return [entry for entry in candidates if entry]

    def _kill_session_lockers(self):
        if platform.system() != "Windows":
            return 0

        killed = 0
        try:
            result = subprocess.run(
                [
                    "wmic",
                    "process",
                    "where",
                    "(name='chrome.exe' or name='chromium.exe')",
                    "get",
                    "ProcessId,CommandLine",
                    "/format:csv",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                **_SUBPROCESS_NO_WINDOW,
            )
        except Exception:
            return 0

        normalized_dirs = []
        for data_dir in self._candidate_data_dirs():
            try:
                normalized_dirs.append(str(Path(data_dir).resolve()).replace("\\", "/").lower())
            except Exception:
                normalized_dirs.append(str(data_dir).replace("\\", "/").lower())

        for raw_line in str(result.stdout or "").splitlines():
            line = str(raw_line or "").strip()
            if not line or "ProcessId" in line:
                continue

            normalized_line = line.replace("\\", "/").lower()
            is_ours = any(data_dir and data_dir in normalized_line for data_dir in normalized_dirs)
            if "cloakbrowser" in normalized_line or ".cloakbrowser" in normalized_line:
                is_ours = True
            if not is_ours:
                continue

            pid_str = line.split(",")[-1].strip()
            try:
                pid = int(pid_str)
            except Exception:
                continue
            if pid <= 0:
                continue

            alive_before = self.is_alive(pid)
            if alive_before:
                self._terminate_pid(pid)
            if alive_before and not self.is_alive(pid):
                killed += 1

        return killed

    def kill_pid(self, pid):
        try:
            normalized = int(pid)
        except Exception:
            return False
        if normalized <= 0 or not self.is_tracked(normalized):
            return False

        alive_before = self.is_alive(normalized)
        terminated = self._terminate_pid(normalized) if alive_before else False
        alive_after = self.is_alive(normalized)
        if not alive_after:
            self.unregister(normalized)
        return bool(alive_before and (terminated or not alive_after))

    def kill_all(self):
        with self._lock:
            tracked = list(self._pids)

        killed = 0
        survivors = set()
        for pid in tracked:
            alive_before = self.is_alive(pid)
            if alive_before:
                self._terminate_pid(pid)
            if self.is_alive(pid):
                survivors.add(pid)
            elif alive_before:
                killed += 1

        with self._lock:
            self._pids = survivors
            self._persist_unlocked()
        killed += self._kill_session_lockers()
        return killed

    def is_alive(self, pid):
        try:
            normalized = int(pid)
        except Exception:
            return False
        if normalized <= 0:
            return False

        try:
            if platform.system() == "Windows":
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {normalized}"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    **_SUBPROCESS_NO_WINDOW,
                )
                output = str(result.stdout or "")
                return str(normalized) in output and "No tasks are running" not in output
            os.kill(normalized, 0)
            return True
        except Exception:
            return False

    def load_and_kill_stale(self):
        if not self._pid_file.exists():
            return 0

        killed = 0
        survivors = set()
        try:
            old_pids = json.loads(self._pid_file.read_text(encoding="utf-8"))
        except Exception:
            old_pids = []

        for pid in old_pids:
            try:
                normalized = int(pid)
            except Exception:
                continue
            alive_before = self.is_alive(normalized)
            if alive_before:
                self._terminate_pid(normalized)
            if self.is_alive(normalized):
                survivors.add(normalized)
            elif alive_before:
                killed += 1

        with self._lock:
            self._pids = survivors
            self._persist_unlocked()
        return killed

    @property
    def count(self):
        with self._lock:
            return len(self._pids)

    @property
    def pids(self):
        with self._lock:
            return list(sorted(self._pids))


process_tracker = ProcessTracker()


def cleanup_session_locks(session_path):
    """
    Delete Chrome/Chromium lock files from a session directory.
    These stale files prevent new browser instances from accessing the profile.
    Safe to delete — Chrome recreates them on every launch.

    On macOS, SingletonLock is often a symlink; os.path.exists() returns False
    for broken symlinks, so os.path.lexists() is used to catch both cases.
    """
    if not session_path or not os.path.isdir(session_path):
        return 0

    lock_rel_paths = (
        "SingletonLock",
        "SingletonCookie",
        "SingletonSocket",
        "lockfile",
        os.path.join("Default", "LOCK"),
    )
    deleted = 0
    for rel in lock_rel_paths:
        target = os.path.join(session_path, rel)
        try:
            if os.path.lexists(target):
                os.remove(target)
                deleted += 1
        except Exception:
            pass
    return deleted
