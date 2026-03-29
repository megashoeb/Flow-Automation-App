import os
import sys
import tarfile
from pathlib import Path
from typing import Optional


def _cache_root() -> Path:
    if sys.platform.startswith("win"):
        base = Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Caches"
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME") or (Path.home() / ".cache"))
    return base / "G-Labs-Automation-Studio"


def _extract_browsers_archive(base_path: Path) -> Optional[Path]:
    archive_path = base_path / 'playwright-browsers.tar.gz'
    if not archive_path.exists():
        return None

    target_root = _cache_root() / "playwright-browsers"
    marker = target_root / ".extracted"

    if marker.exists():
        return target_root

    target_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as bundle:
        if hasattr(tarfile, 'data_filter'):
            bundle.extractall(target_root, filter='data')
        else:
            # Python < 3.12 fallback: manual path traversal check
            for member in bundle.getmembers():
                member_path = Path(target_root / member.name).resolve()
                if not str(member_path).startswith(str(target_root.resolve())):
                    raise RuntimeError(f"Tar member {member.name!r} escapes target directory")
            bundle.extractall(target_root)
    marker.write_text("ok")
    return target_root


base_path = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
browsers_path = base_path / "playwright-browsers"
if browsers_path.exists():
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(browsers_path))
else:
    extracted = _extract_browsers_archive(base_path)
    if extracted is not None:
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(extracted))
