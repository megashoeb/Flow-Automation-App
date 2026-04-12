import os
import sys
import signal
import platform
import subprocess
import time
import multiprocessing

# ── PyInstaller freeze support (MUST be first) ──
# Without this, .app/.exe builds spawn infinite child processes
multiprocessing.freeze_support()

if sys.stdout is None:
    sys.stdout = open(os.devnull, "w")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w")


# ── Auto-install missing dependencies on startup (once only) ──
def _ensure_dependencies():
    """Install ONLY truly missing packages from requirements.txt. Runs once."""
    # Guard: skip if already ran, or if frozen exe, or if flag file exists
    if getattr(_ensure_dependencies, "_done", False):
        return
    _ensure_dependencies._done = True

    # Skip in any packaged/frozen build (.exe, .app, PyInstaller)
    if getattr(sys, "frozen", False) or getattr(sys, "_MEIPASS", None) or hasattr(sys, "_MEIPASS"):
        return  # .exe build — pip not available

    req_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "requirements.txt")
    if not os.path.exists(req_file):
        return

    # Lock file to prevent install loop across multiple launches
    lock_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".deps_installing")
    if os.path.exists(lock_file):
        try:
            # If lock is older than 5 minutes, stale — remove it
            age = time.time() - os.path.getmtime(lock_file)
            if age < 300:
                print("[Auto-Install] Another install in progress, skipping.")
                return
            os.remove(lock_file)
        except Exception:
            return

    try:
        import pkg_resources
        with open(req_file, "r") as f:
            reqs = [
                line.strip() for line in f
                if line.strip() and not line.startswith("#")
            ]
        # Only install truly MISSING packages, not version mismatches
        missing = []
        for req in reqs:
            try:
                pkg_resources.require(req)
            except pkg_resources.DistributionNotFound:
                # Package not installed at all — need to install
                missing.append(req)
            except pkg_resources.VersionConflict:
                # Package exists but wrong version — skip (don't loop)
                pass
        if not missing:
            return

        # Create lock file
        try:
            with open(lock_file, "w") as f:
                f.write(str(os.getpid()))
        except Exception:
            pass

        print(f"[Auto-Install] Installing missing: {', '.join(missing)}")
        base_cmd = [sys.executable, "-m", "pip", "install"]
        installed = False
        for extra_flags in [[], ["--break-system-packages"], ["--user"]]:
            try:
                subprocess.check_call(
                    base_cmd + extra_flags + missing,
                    stdout=subprocess.DEVNULL if not os.environ.get("DEBUG") else None,
                    stderr=subprocess.STDOUT,
                    timeout=120,
                )
                installed = True
                break
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                continue
        if installed:
            print("[Auto-Install] Done.")
        else:
            print("[Auto-Install] Failed. Run manually: pip3 install -r requirements.txt")

        # Remove lock file
        try:
            os.remove(lock_file)
        except Exception:
            pass
    except Exception as e:
        print(f"[Auto-Install] Warning: {e}")
        try:
            os.remove(lock_file)
        except Exception:
            pass

_ensure_dependencies()


from src.core.runtime_stdio import ensure_std_streams
ensure_std_streams()
from PySide6.QtWidgets import QApplication
from src.core.app_paths import get_app_data_dir
from src.ui.main_window import MainWindow

# Phase 1: Fluent theme foundation — applies globally to all Qt widgets.
# Theme accent matches existing palette (#2563EB = Tailwind blue-600).
# Existing custom QSS in _apply_modern_theme() layers on top.
try:
    from qfluentwidgets import setTheme, setThemeColor, Theme
    _FLUENT_AVAILABLE = True
except Exception:
    _FLUENT_AVAILABLE = False


APP_DATA_DIR = str(get_app_data_dir())


def _handle_exit_signal(signum, frame):
    """Handle Cmd+Q, SIGTERM, SIGINT — force exit without crash."""
    try:
        from src.core.process_tracker import process_tracker
        process_tracker.kill_all()
    except Exception:
        pass
    os._exit(0)


def _acquire_single_instance_lock():
    """Prevent multiple app instances using a socket lock."""
    import socket
    _lock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Bind to a fixed local port — only one process can hold it
        _lock_socket.bind(("127.0.0.1", 47291))
        _lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return _lock_socket  # Keep reference alive to hold the lock
    except OSError:
        # Port already in use — another instance is running
        print("[G-Labs] App is already running. Exiting duplicate instance.")
        sys.exit(0)


def main():
    # ── Single instance guard — prevents multiple windows ──
    _lock = _acquire_single_instance_lock()

    # Register signal handlers — catches Cmd+Q, Ctrl+C, kill.
    signal.signal(signal.SIGINT, _handle_exit_signal)
    signal.signal(signal.SIGTERM, _handle_exit_signal)
    if platform.system() == "Darwin":
        signal.signal(signal.SIGHUP, _handle_exit_signal)

    app = QApplication(sys.argv)

    # Last-chance cleanup before Qt exits (any exit path).
    def _on_about_to_quit():
        try:
            from src.core.process_tracker import process_tracker
            process_tracker.kill_all()
        except Exception:
            pass

    app.aboutToQuit.connect(_on_about_to_quit)

    # Set global application style
    app.setStyle("Fusion")

    # Phase 1: Apply Fluent dark theme globally. Must happen AFTER
    # QApplication is created but BEFORE any widget is instantiated.
    # setThemeColor accents buttons/focus rings/nav highlights.
    if _FLUENT_AVAILABLE:
        try:
            setTheme(Theme.DARK)
            setThemeColor("#2563EB")
        except Exception:
            pass

    window = MainWindow()
    window.show()

    sys.exit(app.exec())

if __name__ == "__main__":
    main()
