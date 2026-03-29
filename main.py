import os
import sys

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

def main():
    # Qt6/PySide6 handles high-DPI scaling automatically;
    # the legacy AA_UseHighDpiPixmaps / AA_EnableHighDpiScaling
    # attributes were removed in Qt6 and are no longer needed.

    app = QApplication(sys.argv)
    
    # Set global application style
    app.setStyle("Fusion")
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
