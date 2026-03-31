import os
import sys
import signal
import platform

if sys.stdout is None:
    sys.stdout = open(os.devnull, "w")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w")

from src.core.runtime_stdio import ensure_std_streams
ensure_std_streams()
from PySide6.QtWidgets import QApplication
from src.core.app_paths import get_app_data_dir
from src.ui.main_window import MainWindow


APP_DATA_DIR = str(get_app_data_dir())


def _handle_exit_signal(signum, frame):
    """Handle Cmd+Q, SIGTERM, SIGINT — force exit without crash."""
    try:
        from src.core.process_tracker import process_tracker
        process_tracker.kill_all()
    except Exception:
        pass
    os._exit(0)


def main():
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

    window = MainWindow()
    window.show()

    sys.exit(app.exec())

if __name__ == "__main__":
    main()
