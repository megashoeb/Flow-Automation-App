#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== G-Labs Automation Studio - Mac Build ==="

if command -v python3.11 >/dev/null 2>&1; then
    PYTHON_BIN="python3.11"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
else
    echo "Python 3.11 not found. Install it from python.org first."
    exit 1
fi

PYTHON_VERSION="$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [ "$PYTHON_VERSION" != "3.11" ]; then
    echo "Detected $PYTHON_BIN = Python $PYTHON_VERSION"
    echo "Please install Python 3.11 and run this script again."
    exit 1
fi

export PYTHONIOENCODING="utf-8"
export PIP_NO_COMPILE="1"

echo "Creating virtual environment..."
"$PYTHON_BIN" -m venv venv
source venv/bin/activate

echo "Installing dependencies..."
python -m pip install --upgrade pip
python -m pip install --no-compile -r requirements.txt
python -m pip install --no-compile cloakbrowser
python -m pip install --no-compile pyinstaller

echo "Downloading CloakBrowser binary..."
python -m cloakbrowser install

echo "Building .app..."
pyinstaller --noconfirm --clean --onedir --windowed \
    --name="G-Labs Automation Studio" \
    --icon="assets/icon.icns" \
    --osx-bundle-identifier="com.glabs.automationstudio" \
    --hidden-import=cloakbrowser \
    --hidden-import=cloakbrowser.download \
    --hidden-import=cloakbrowser.config \
    --hidden-import=PySide6.QtCore \
    --hidden-import=PySide6.QtWidgets \
    --hidden-import=PySide6.QtGui \
    --runtime-hook=runtime_playwright.py \
    --add-data="src:src" \
    --add-data="assets:assets" \
    main.py

APP_PATH="dist/G-Labs Automation Studio.app"
if [ -d "$APP_PATH" ] && [ -f "assets/icon.icns" ]; then
    echo "Applying app icon..."
    mkdir -p "$APP_PATH/Contents/Resources"
    cp "assets/icon.icns" "$APP_PATH/Contents/Resources/icon.icns"
    /usr/libexec/PlistBuddy -c "Set :CFBundleIconFile icon.icns" "$APP_PATH/Contents/Info.plist" >/dev/null 2>&1 || \
    /usr/libexec/PlistBuddy -c "Add :CFBundleIconFile string icon.icns" "$APP_PATH/Contents/Info.plist" >/dev/null 2>&1 || true
    touch "$APP_PATH"
fi

echo ""
echo "Build complete!"
echo "App location: dist/G-Labs Automation Studio/"
echo "Run: open 'dist/G-Labs Automation Studio/G-Labs Automation Studio.app'"
