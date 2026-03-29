import os
import sys


def ensure_std_streams():
    """PyInstaller --windowed can leave stdout/stderr as None; give libraries a writable sink."""
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w")
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w")

