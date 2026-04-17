// G-Labs Studio Helper — Popup UI

function updateStatus(status) {
  const badge = document.getElementById("statusBadge");
  const dot = document.getElementById("statusDot");
  const text = document.getElementById("statusText");

  if (status.connected) {
    badge.className = "status-badge connected";
    dot.className = "dot green";
    text.textContent = "Connected to Studio";
  } else {
    badge.className = "status-badge disconnected";
    dot.className = "dot red";
    text.textContent = status.lastError
      ? `Disconnected (${status.lastError})`
      : "Waiting for Studio...";
  }

  // Token count
  document.getElementById("tokenCount").textContent = status.tokenCount || 0;

  // Accounts list
  const container = document.getElementById("accountsList");
  if (status.accounts && status.accounts.length > 0) {
    container.innerHTML = status.accounts
      .map((acc) => {
        const heldSec = (status.ecosystem?.heldAccounts || {})[acc.email];
        const count = (status.ecosystem?.todayCounts || {})[acc.email] || 0;
        let badge = "";
        if (heldSec && heldSec > 0) {
          const hrs = Math.floor(heldSec / 3600);
          const mins = Math.floor((heldSec % 3600) / 60);
          badge = `<div class="name" style="color:#ef5350">🔒 Held (${hrs}h ${mins}m)</div>`;
        } else if (count > 0) {
          badge = `<div class="name" style="color:#66bb6a">🌱 ${count} activities today</div>`;
        }
        return `
      <div class="account-card">
        <div class="icon">${(acc.email || "?")[0].toUpperCase()}</div>
        <div class="info">
          <div class="email">${acc.email || "Unknown"}</div>
          ${badge || `<div class="name">${acc.name || ""}</div>`}
        </div>
      </div>`;
      })
      .join("");
  } else {
    container.innerHTML = '<div class="no-accounts">No accounts detected.<br>Open labs.google.com and log in.</div>';
  }

  // Ecosystem state
  const eco = status.ecosystem || { directive: "disabled", running: false };
  const toggle = document.getElementById("ecoToggle");
  const dot = document.getElementById("ecoDot");
  const text = document.getElementById("ecoStatusText");
  const isOn = eco.directive !== "disabled";
  toggle.classList.toggle("on", isOn);
  if (eco.directive === "active") {
    dot.className = "eco-dot active";
    text.textContent = eco.running
      ? `Active — ${eco.currentSite} (${eco.currentAccount?.split("@")[0] || ""})`
      : "Active — idle, next activity coming";
  } else if (eco.directive === "paused") {
    dot.className = "eco-dot paused";
    text.textContent = "Paused — generation running";
  } else {
    dot.className = "eco-dot disabled";
    text.textContent = "Disabled — toggle ON to build trust";
  }
}

// Get status from background
function refresh() {
  chrome.runtime.sendMessage({ type: "getStatus" }, (response) => {
    if (response) updateStatus(response);
  });
}

// Open Labs tab
document.getElementById("btnOpenLabs").addEventListener("click", () => {
  chrome.runtime.sendMessage({ type: "openLabsTab" });
});

// Refresh accounts
document.getElementById("btnRefresh").addEventListener("click", () => {
  chrome.runtime.sendMessage({ type: "detectAccounts" }, (response) => {
    refresh();
  });
});

// Ecosystem toggle
document.getElementById("ecoToggle").addEventListener("click", () => {
  const toggle = document.getElementById("ecoToggle");
  const newState = !toggle.classList.contains("on");
  chrome.runtime.sendMessage(
    { type: "ecosystemToggle", enabled: newState },
    () => setTimeout(refresh, 500)
  );
});

// Initial load
refresh();

// Auto-refresh every 3s while popup is open
setInterval(refresh, 3000);
