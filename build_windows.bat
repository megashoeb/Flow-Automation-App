@echo off
setlocal
cd /d %~dp0

echo ========================================
echo  G-Labs Automation Studio - Windows Build
echo ========================================
echo.

set "PLAYWRIGHT_BROWSERS_PATH=%CD%\playwright-browsers"

python -c "import PyInstaller" >nul 2>&1
if errorlevel 1 (
    echo Installing PyInstaller...
    python -m pip install pyinstaller || goto :error
) else (
    echo PyInstaller already installed.
)

python -c "import PySide6, playwright, playwright.async_api, playwright_stealth, fake_useragent, cloakbrowser, httpx, aiohttp" >nul 2>&1
if errorlevel 1 (
    echo Installing dependencies...
    python -m pip install -r requirements.txt || goto :error
    python -m pip install cloakbrowser || goto :error
) else (
    echo Dependencies already available.
)
echo.

echo Ensuring CloakBrowser package...
python -m pip install cloakbrowser || goto :error
echo Downloading CloakBrowser binary...
python -m cloakbrowser install || goto :error
echo.

dir /b "%PLAYWRIGHT_BROWSERS_PATH%\chromium-*" >nul 2>&1
if errorlevel 1 (
    echo Installing Playwright browsers...
    python -m playwright install chromium || goto :error
) else (
    echo Playwright Chromium already installed.
)
echo.

if exist playwright-browsers.tar.gz del /f /q playwright-browsers.tar.gz
echo Packaging Playwright browsers...
python -c "import tarfile, pathlib; root=pathlib.Path('playwright-browsers'); tf=tarfile.open('playwright-browsers.tar.gz','w:gz'); [tf.add(str(p), arcname=str(p.relative_to(root))) for p in root.rglob('*')]; tf.close()" || goto :error
echo.

echo Building .exe...
python -m PyInstaller --noconfirm --clean build_windows.spec || goto :error
echo.

if not exist "dist\G-Labs Automation Studio\data" mkdir "dist\G-Labs Automation Studio\data"
if not exist "dist\G-Labs Automation Studio\data\sessions" mkdir "dist\G-Labs Automation Studio\data\sessions"
if not exist "dist\G-Labs Automation Studio\data\session_clones" mkdir "dist\G-Labs Automation Studio\data\session_clones"
xcopy /E /I /Y "assets" "dist\G-Labs Automation Studio\assets" >nul
> "dist\G-Labs Automation Studio\data\README.txt" (
    echo Runtime data is stored in %%APPDATA%%\G-Labs when the packaged app runs.
    echo These folders are provided only as placeholders in the distribution package.
)

echo ========================================
echo  Build complete!
echo  Output: dist\G-Labs Automation Studio\
echo  NOTE: CloakBrowser binary will auto-download on first run if cache is missing.
echo ========================================
pause
exit /b 0

:error
echo.
echo ========================================
echo  Build failed.
echo ========================================
pause
exit /b 1
