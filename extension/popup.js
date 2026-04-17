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
        let actionBtn = "";
        if (heldSec && heldSec > 0) {
          const hrs = Math.floor(heldSec / 3600);
          const mins = Math.floor((heldSec % 3600) / 60);
          badge = `<div class="name" style="color:#ef5350">🔒 Held (${hrs}h ${mins}m left)</div>`;
          actionBtn = `
            <div style="margin-top:6px;display:flex;gap:4px;">
              <button class="btn btn-secondary force-btn"
                data-email="${acc.email}"
                style="margin:0;padding:4px 8px;font-size:10px;flex:1;">
                ⚠ Force Enable
              </button>
              <button class="btn btn-secondary release-btn"
                data-email="${acc.email}"
                style="margin:0;padding:4px 8px;font-size:10px;flex:1;">
                Release
              </button>
            </div>`;
        } else if (count > 0) {
          badge = `<div class="name" style="color:#66bb6a">🌱 ${count} activities today</div>`;
        }
        return `
      <div class="account-card" style="flex-direction:column;align-items:stretch;">
        <div style="display:flex;align-items:center;gap:8px;">
          <div class="icon">${(acc.email || "?")[0].toUpperCase()}</div>
          <div class="info">
            <div class="email">${acc.email || "Unknown"}</div>
            ${badge || `<div class="name">${acc.name || ""}</div>`}
          </div>
        </div>
        ${actionBtn}
      </div>`;
      })
      .join("");

    // Wire action buttons
    document.querySelectorAll(".force-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        const email = e.target.dataset.email;
        if (!confirm(
          `⚠ Warning: Account '${email}' was flagged by reCAPTCHA.\n\n` +
          `Force-enabling it may cause permanent flagging.\n\n` +
          `Continue?`
        )) return;
        fetch("http://127.0.0.1:18924/ecosystem", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ force_enable_account: email, enable: true }),
        }).then(() => setTimeout(refresh, 300));
      });
    });
    document.querySelectorAll(".release-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        const email = e.target.dataset.email;
        chrome.runtime.sendMessage(
          { type: "ecosystemReleaseAccount", account: email },
          () => setTimeout(refresh, 300)
        );
      });
    });
  } else {
    container.innerHTML = '<div class="no-accounts">No accounts detected.<br>Open labs.google.com and log in.</div>';
  }

  // Ecosystem state — toggle reflects LOCAL enabled flag, not just directive.
  // This way the toggle stays consistent with what the user chose, even if
  // the bridge (app) is offline.
  const eco = status.ecosystem || { directive: "disabled", running: false, enabled: false };
  const ecoToggle = document.getElementById("ecoToggle");
  const ecoDot = document.getElementById("ecoDot");
  const ecoText = document.getElementById("ecoStatusText");
  const isOn = !!eco.enabled;
  ecoToggle.classList.toggle("on", isOn);

  if (!eco.bridgeOnline && isOn) {
    // Toggle is ON but bridge is offline — ecosystem will activate when app starts
    ecoDot.className = "eco-dot paused";
    ecoText.textContent = "Waiting for app to start...";
  } else if (eco.directive === "active") {
    ecoDot.className = "eco-dot active";
    ecoText.textContent = eco.running
      ? `Active — ${eco.currentSite} (${eco.currentAccount?.split("@")[0] || ""})`
      : "Active — idle, next activity coming";
  } else if (eco.directive === "paused") {
    ecoDot.className = "eco-dot paused";
    ecoText.textContent = "Paused — generation running";
  } else {
    ecoDot.className = "eco-dot disabled";
    ecoText.textContent = "Disabled — toggle ON to build trust";
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
