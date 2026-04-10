@echo off
echo ========================================
echo G-Labs Automation Studio - Update Script
echo ========================================
echo.

echo [1/3] Pulling latest code from GitHub...
git pull origin main
if errorlevel 1 (
    echo ERROR: git pull failed
    pause
    exit /b 1
)

echo.
echo [2/3] Syncing updated source files to G-Labs-App-Release...
xcopy /E /Y /I src "G-Labs-App-Release\src" >nul
copy /Y main.py "G-Labs-App-Release\main.py" >nul
copy /Y requirements.txt "G-Labs-App-Release\requirements.txt" >nul
copy /Y runtime_playwright.py "G-Labs-App-Release\runtime_playwright.py" >nul

echo.
echo [3/3] Update complete!
echo Your data (accounts, sessions, outputs) is preserved.
echo.
pause
