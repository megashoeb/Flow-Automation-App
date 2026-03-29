@echo off
setlocal
cd /d %~dp0
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
set PLAYWRIGHT_BROWSERS_PATH=%CD%\playwright-browsers
python -m playwright install chromium
if exist playwright-browsers.tar.gz del /f /q playwright-browsers.tar.gz
python -c "import tarfile, pathlib; root=pathlib.Path('playwright-browsers'); tf=tarfile.open('playwright-browsers.tar.gz','w:gz'); [tf.add(str(p), arcname=str(p.relative_to(root))) for p in root.rglob('*')]; tf.close()"
python -m PyInstaller --noconfirm --clean build_win.spec
echo.
echo Build complete. Check dist\ for G-Labs Automation Studio.exe / folder.
endlocal
