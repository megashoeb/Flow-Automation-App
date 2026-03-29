# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
import os
import playwright
from PyInstaller.utils.hooks import collect_submodules

project_dir = Path(SPECPATH).resolve()
driver_dir = Path(playwright.__file__).resolve().parent / 'driver'
browsers_dir = Path(os.environ.get('PLAYWRIGHT_BROWSERS_PATH', project_dir / 'playwright-browsers')).resolve()

hiddenimports = [
    'PySide6',
    'PySide6.QtCore',
    'PySide6.QtGui',
    'PySide6.QtWidgets',
    'playwright',
    'playwright.async_api',
    'src.db.db_manager',
    'src.core.account_manager',
    'src.core.bot_engine',
    'src.core.queue_manager',
    'src.ui.main_window',
] + collect_submodules('playwright')

datas = [
    (str(project_dir / 'data'), 'data'),
    (str(project_dir / 'outputs'), 'outputs'),
    (str(project_dir / 'assets'), 'assets'),
]
if driver_dir.exists():
    datas.append((str(driver_dir), 'playwright/driver'))
browsers_archive = project_dir / 'playwright-browsers.tar.gz'
if browsers_archive.exists():
    datas.append((str(browsers_archive), '.'))

a = Analysis(
    ['main.py'],
    pathex=[str(project_dir)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[str(project_dir / 'runtime_playwright.py')],
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
    name='G-Labs Automation Studio',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    icon=str(project_dir / 'assets' / 'icon.icns'),
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='G-Labs Automation Studio',
)
app = BUNDLE(
    coll,
    name='G-Labs Automation Studio.app',
    icon=str(project_dir / 'assets' / 'icon.icns'),
    bundle_identifier='com.megashoeb.glabsautomationstudio',
)
