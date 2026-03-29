# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from PyInstaller.utils.hooks import collect_all, collect_submodules


project_dir = Path(SPECPATH).resolve()

pw_datas, pw_binaries, pw_hiddenimports = collect_all("playwright")
pyside_datas, pyside_binaries, pyside_hiddenimports = collect_all("PySide6")
stealth_datas, stealth_binaries, stealth_hiddenimports = collect_all("playwright_stealth")
fakeua_datas, fakeua_binaries, fakeua_hiddenimports = collect_all("fake_useragent")
cloak_datas, cloak_binaries, cloak_hiddenimports = collect_all("cloakbrowser")

datas = [
    (str(project_dir / "assets"), "assets"),
] + pw_datas + pyside_datas + stealth_datas + fakeua_datas + cloak_datas

browsers_archive = project_dir / "playwright-browsers.tar.gz"
if browsers_archive.exists():
    datas.append((str(browsers_archive), "."))

hiddenimports = [
    "PySide6",
    "PySide6.QtCore",
    "PySide6.QtGui",
    "PySide6.QtWidgets",
    "playwright",
    "playwright.async_api",
    "playwright_stealth",
    "fake_useragent",
    "cloakbrowser",
    "cloakbrowser.download",
    "cloakbrowser.config",
    "httpx",
    "aiohttp",
    "sqlite3",
    "src.core.account_manager",
    "src.core.app_paths",
    "src.core.bot_engine",
    "src.core.cloakbrowser_support",
    "src.core.cloak_downloader",
    "src.core.fingerprint_generator",
    "src.core.queue_manager",
    "src.core.runtime_stdio",
    "src.db.db_manager",
    "src.ui.main_window",
] + pw_hiddenimports + pyside_hiddenimports + stealth_hiddenimports + fakeua_hiddenimports + cloak_hiddenimports

a = Analysis(
    ["main.py"],
    pathex=[str(project_dir)],
    binaries=pw_binaries + pyside_binaries + stealth_binaries + fakeua_binaries + cloak_binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[str(project_dir / "runtime_playwright.py")],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="G-Labs Automation Studio",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    icon=str(project_dir / "assets" / "icon.ico"),
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="G-Labs Automation Studio",
)
