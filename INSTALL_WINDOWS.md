# Flow Automation App - Windows Install Guide

## Step 1: Install Git (skip if already installed)

1. Open this link: https://git-scm.com/download/win
2. Download will start automatically
3. Run the installer — click Next on everything (keep all defaults)
4. Click Install, then Finish

## Step 2: Install Python (skip if already installed)

1. Open this link: https://www.python.org/downloads/
2. Click the big yellow "Download Python" button
3. Run the installer
4. IMPORTANT: Check the box "Add python.exe to PATH" at the bottom of the installer
5. Click "Install Now"
6. Click Close when done

## Step 3: Build the App

1. Open PowerShell (press Windows key, type "PowerShell", click it)
2. Copy-paste this entire command and press Enter:

```
cd ~/Desktop; if(Test-Path Flow-Automation-App){cd Flow-Automation-App; git pull}else{git clone https://github.com/megashoeb/Flow-Automation-App.git; cd Flow-Automation-App}; .\build_windows.bat
```

3. Wait for the build to finish (10-15 minutes first time)
4. When done, the app will be at:
   Desktop > Flow-Automation-App > dist > G-Labs Automation Studio > G-Labs Automation Studio.exe

## Step 4: Run the App

Double-click: Desktop > Flow-Automation-App > dist > G-Labs Automation Studio > G-Labs Automation Studio.exe

---

## How to Update (when new version is available)

1. Open PowerShell
2. Copy-paste this command and press Enter:

```
cd ~/Desktop/Flow-Automation-App; git pull; .\build_windows.bat
```

3. Wait for build to finish
4. Run the same .exe again — it will be updated

---

## Quick Reference

| What | Command |
|------|---------|
| First time install | Step 1 + Step 2 + Step 3 (above) |
| Update to latest | `cd ~/Desktop/Flow-Automation-App; git pull; .\build_windows.bat` |
| Run without building | `cd ~/Desktop/Flow-Automation-App; python main.py` |
| App location | `Desktop\Flow-Automation-App\dist\G-Labs Automation Studio\G-Labs Automation Studio.exe` |
