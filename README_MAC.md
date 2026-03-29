# G-Labs Automation Studio - Mac Build Guide

## Requirements
- macOS 12 or later
- Python 3.10 or later from python.org
- Google Chrome installed

## Build Steps
1. Open Terminal.
2. Change into this folder:
   `cd G-Labs-Mac-Build`
3. Make the build script executable:
   `chmod +x build_mac.sh`
4. Run the build:
   `./build_mac.sh`
5. Wait for the build to finish.
6. The app will be created in:
   `dist/G-Labs Automation Studio.app`

## First Run
- CloakBrowser will download its Chromium binary on first use if needed.
- macOS may block the unsigned app at first launch.
- If that happens, right-click the app, choose `Open`, then confirm.
- You can also allow it in `System Settings -> Privacy & Security`.
- If you rebuilt an older copy, delete the previous `.app` first so Finder refreshes the icon.

## Running Without Building
```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m pip install cloakbrowser
python3 main.py
```

## Notes
- Runtime data on macOS is stored in `~/Library/Application Support/G-Labs/`.
- CloakBrowser uses `~/.cloakbrowser/` for its own cached browser binary.
- `build_mac.sh` installs dependencies, downloads the CloakBrowser binary, and builds the app in one pass.
- The build script now forces `assets/icon.icns` into the app bundle and refreshes the macOS bundle icon metadata after build.
