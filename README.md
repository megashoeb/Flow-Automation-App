# G-Labs Automation Studio

Distributable source package for building the desktop app on macOS and Windows.

## Included
- `main.py`
- `src/`
- `data/sessions/` (empty)
- `data/session_clones/` (empty)
- `outputs/` (empty)
- `assets/icon.icns` and `assets/icon.ico`
- `build_mac.sh`, `build_macos.sh`, `build_win.bat`, `build_windows.bat`
- `build_mac.spec`, `build_macos.spec`, `build_win.spec`, `build_windows.spec`
- `runtime_playwright.py`

## System Requirements
- Python 3.10+
- 8 GB RAM minimum
- 16 GB RAM recommended for multi-account / parallel work
- macOS 13+ or Windows 10/11
- Chromium-compatible Playwright browser support
- Google Chrome installed for `real_chrome` CDP mode

## First Run Setup
1. Create and activate a virtual environment.
2. Install dependencies:
   - `python -m pip install -r requirements.txt`
3. Install Playwright Chromium:
   - `python -m playwright install chromium`
4. Start the app:
   - `python main.py`
5. Open `Account Manager` and log in at least one Google account.

## Build From Source
### macOS
1. Open Terminal in this folder.
2. Run:
   - `chmod +x build_macos.sh`
   - `./build_macos.sh`
3. Output is created under `dist/`.
4. M-series Macs produce an `arm64` app build; Intel Macs produce an `x86_64` app build.

### Windows
1. Open Terminal (PowerShell or Command Prompt) in this folder.
2. Run:
   - PowerShell: `.\build_windows.bat`
   - Command Prompt: `build_windows.bat`
3. Output is created under `dist\`.

## Packaging Notes
- The build scripts set `PLAYWRIGHT_BROWSERS_PATH` to a local `playwright-browsers/` folder before installing Chromium.
- Chromium is archived into `playwright-browsers.tar.gz` before bundling, and the packaged app extracts it on first launch into an OS-specific cache folder.
- The packaged Windows app stores writable runtime data under `%APPDATA%\G-Labs` for `jobs.db`, sessions, session clones, and outputs.
- The packaged macOS app stores writable runtime data under `~/Library/Application Support/G-Labs`.
- Default packaged outputs on macOS are saved under `~/Library/Application Support/G-Labs/outputs/`.
- Unsigned macOS builds may require first-launch bypass via right-click `Open`, or `xattr -cr "/Applications/G-Labs Automation Studio.app"`.
- Google Chrome must be installed on the user's PC because the default browser mode is `real_chrome` with CDP.
- Build macOS artifacts on macOS, and build Windows artifacts on Windows.
- `assets/` contains generic placeholder icons. Replace them with your branded icons if needed.

## Dependencies
- PySide6 6.10.2
- Playwright 1.58.0
- PyInstaller 6+
