#!/bin/bash
set -e

echo "========================================"
echo "G-Labs Automation Studio - Update Script"
echo "========================================"
echo ""

echo "[1/3] Pulling latest code from GitHub..."
git pull origin main

echo ""
echo "[2/3] Syncing updated source files to G-Labs-App-Release..."
cp -r src/* G-Labs-App-Release/src/
cp main.py G-Labs-App-Release/main.py
cp requirements.txt G-Labs-App-Release/requirements.txt
cp runtime_playwright.py G-Labs-App-Release/runtime_playwright.py 2>/dev/null || true

echo ""
echo "[3/3] Update complete!"
echo "Your data (accounts, sessions, outputs) is preserved."
echo ""
