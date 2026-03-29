#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

echo "========================================"
echo " G-Labs Automation Studio - macOS Build"
echo "========================================"
echo ""

ARCH="$(uname -m)"
echo "Detected architecture: $ARCH"
echo ""

echo "Installing dependencies..."
python3 -m pip install pyinstaller
python3 -m pip install -r requirements.txt
echo ""

export PLAYWRIGHT_BROWSERS_PATH="$PWD/playwright-browsers"
echo "Installing Playwright browsers..."
python3 -m playwright install chromium
echo ""

echo "Packaging Playwright browsers..."
rm -f playwright-browsers.tar.gz
tar -czf playwright-browsers.tar.gz -C playwright-browsers .
echo ""

echo "Building .app for $ARCH..."
python3 -m PyInstaller --noconfirm --clean build_macos.spec

APP_PATH="dist/G-Labs Automation Studio.app"
if [ -d "$APP_PATH" ] && [ -f "assets/icon.icns" ]; then
    echo "Refreshing app icon resources..."
    mkdir -p "$APP_PATH/Contents/Resources"
    cp "assets/icon.icns" "$APP_PATH/Contents/Resources/icon.icns"
    /usr/libexec/PlistBuddy -c "Set :CFBundleIconFile icon.icns" "$APP_PATH/Contents/Info.plist" >/dev/null 2>&1 || \
    /usr/libexec/PlistBuddy -c "Add :CFBundleIconFile string icon.icns" "$APP_PATH/Contents/Info.plist" >/dev/null 2>&1 || true
    touch "$APP_PATH"
fi
echo ""
echo "========================================"
echo " Build complete!"
echo " Output: dist/G-Labs Automation Studio.app"
echo " Architecture: $ARCH"
echo "========================================"
