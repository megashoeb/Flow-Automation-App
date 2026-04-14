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
      .map(
        (acc) => `
      <div class="account-card">
        <div class="icon">${(acc.email || "?")[0].toUpperCase()}</div>
        <div class="info">
          <div class="email">${acc.email || "Unknown"}</div>
          <div class="name">${acc.name || ""}</div>
        </div>
      </div>
    `
      )
      .join("");
  } else {
    container.innerHTML = '<div class="no-accounts">No accounts detected.<br>Open labs.google.com and log in.</div>';
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

// Initial load
refresh();

// Auto-refresh every 3s while popup is open
setInterval(refresh, 3000);
