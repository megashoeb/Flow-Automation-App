# Flow Automation App - Mac Install Guide

## How to Install

**Step 1:** Open Terminal in your MacBook (press Cmd + Space, type "Terminal", hit Enter)

**Step 2:** Copy and paste the below commands and hit Enter:

```
cd ~/Downloads
git clone https://github.com/megashoeb/Flow-Automation-App.git
cd Flow-Automation-App
chmod +x build_macos.sh
./build_macos.sh
```

**Step 3:** Wait for downloading and installation to be complete (10-15 minutes first time)

**Step 4:** Once installation is completed, close the Terminal window

**Step 5:** Go to Downloads > Flow-Automation-App > dist > G-Labs Automation Studio

**Step 6:** Open the App > Account Manager > Login your Pro or Ultra Account, Enjoy!

> **Note:** If macOS blocks the app on first launch, right-click the app > click "Open" > click "Open" again to confirm. You can also allow it in System Settings > Privacy & Security.

---

## How to Update (If Available)

**Step 1:** Right-click on the Flow-Automation-App folder > click "New Terminal at Folder"

**Step 2:** Run the below command:

```
git pull
chmod +x build_macos.sh
./build_macos.sh
```

**Step 3:** Wait for the update and rebuild to complete, Enjoy!

---

## Error and Fix

Open Terminal and check if "git" is installed or not by entering the below command:

```
git --version
```

If "not found" appears, run this command to install it:

```
xcode-select --install
```

A popup will appear — click Install and wait 5-10 minutes.

Once installed, continue with the install steps:

```
cd ~/Downloads
git clone https://github.com/megashoeb/Flow-Automation-App.git
cd Flow-Automation-App
chmod +x build_macos.sh
./build_macos.sh
```
