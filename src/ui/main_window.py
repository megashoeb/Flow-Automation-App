import sys
import os
import asyncio
import subprocess
import shutil
import uuid
import time
import re
import json
from pathlib import Path
from urllib.parse import urlparse
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QTextEdit, QPlainTextEdit, QPushButton, QComboBox, QLabel,
    QTableWidget, QTableWidgetItem, QTableView, QHeaderView, QAbstractItemView,
    QSplitter, QGroupBox, QLineEdit, QTabWidget, QScrollArea,
    QMessageBox, QFileDialog, QSpinBox, QCheckBox, QFrame, QSizePolicy, QProgressBar,
    QProgressDialog, QDialog,
    QGraphicsDropShadowEffect, QDoubleSpinBox
)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QSize, QObject, QRunnable, QThreadPool, QRectF
from PySide6.QtGui import QColor, QIcon, QPixmap, QFont, QPainter, QPen, QTextCursor

from src.db.db_manager import (
    get_accounts,
    add_account,
    remove_account,
    remove_account_by_id,
    update_account_name_by_id,
    update_account_proxy_by_id,
    update_account_session_by_id,
    add_jobs_bulk,
    get_all_jobs,
    get_failed_jobs,
    get_output_directory,
    get_float_setting,
    get_int_setting,
    get_bool_setting,
    get_setting,
    set_setting,
    set_account_flag,
    clear_account_flags,
    clear_failed_jobs,
    clear_completed_jobs,
    update_job_status,
    update_job_prompt,
    update_pending_jobs_generation_settings,
    retry_failed_jobs_to_top,
)
from src.core.account_manager import AccountManager
from src.core.app_paths import get_app_data_dir, get_outputs_dir, get_project_cache_path, get_session_clones_dir, get_sessions_dir
from src.core.bot_engine import GoogleLabsBot
from src.core.process_tracker import process_tracker
from src.core.queue_manager import AsyncQueueManager
from src.ui.queue_model import QueueTableModel

class LoginWorker(QThread):
    log_msg = Signal(str)
    download_progress = Signal(int, str)
    download_complete = Signal(bool, str)
    session_saved = Signal(str, str, str)
    warmup_progress = Signal(str, int, str)
    warmup_complete = Signal(str, bool, str)
    finished_login = Signal(str, str, str) # name, session_path, detected_email
    
    def __init__(self, account_name, proxy=""):
        super().__init__()
        self.account_name = account_name
        self.proxy = str(proxy or "").strip()

    def stop(self):
        self.requestInterruption()
        
    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            name, session_path, detected_email = loop.run_until_complete(
                AccountManager.login_and_save_session(
                    self.account_name,
                    lambda m: self.log_msg.emit(m),
                    download_progress_callback=lambda percent, status: self.download_progress.emit(int(percent), str(status)),
                    download_complete_callback=lambda success, message: self.download_complete.emit(bool(success), str(message)),
                    session_saved_callback=lambda name, session_path, detected_email: self.session_saved.emit(
                        str(name),
                        str(session_path),
                        str(detected_email),
                    ),
                    warmup_progress_callback=lambda name, percent, status: self.warmup_progress.emit(
                        str(name),
                        int(percent),
                        str(status),
                    ),
                    warmup_complete_callback=lambda name, success, message: self.warmup_complete.emit(
                        str(name),
                        bool(success),
                        str(message),
                    ),
                    should_stop=lambda: self.isInterruptionRequested(),
                    proxy=self.proxy,
                )
            )
            if not self.isInterruptionRequested():
                self.finished_login.emit(name, session_path, detected_email)
        except Exception as e:
            if not self.isInterruptionRequested():
                label = self.account_name if str(self.account_name or "").strip() else "AUTO-GMAIL"
                self.log_msg.emit(f"[{label}] Error during login: {str(e)}")
        finally:
            loop.close()


class LoginCheckWorker(QThread):
    log_msg = Signal(str)
    single_result = Signal(int, dict)
    result_ready = Signal(dict)

    def __init__(self, accounts):
        super().__init__()
        self.accounts = list(accounts or [])

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = {}
        try:
            for account in self.accounts:
                account_id = int(account.get("id") or 0)
                account_name = str(account.get("name") or f"Account {account_id}")
                session_path = str(account.get("session_path") or "")
                proxy = str(account.get("proxy") or "")
                try:
                    result = loop.run_until_complete(
                        AccountManager.check_account_login_status(session_path, proxy=proxy)
                    )
                except Exception as e:
                    result = {
                        "logged_in": False,
                        "email": "",
                        "expires": "",
                        "error": str(e),
                    }

                results[account_id] = result
                self.single_result.emit(account_id, result)
                state_text = "logged in" if result.get("logged_in") else "logged out"
                self.log_msg.emit(f"[ACCOUNTS] {account_name}: {state_text}")
        finally:
            loop.close()

        self.result_ready.emit(results)


class CleanupThread(QThread):
    """Kept for backward compat but no longer used by closeEvent."""
    def __init__(self, queue_manager, parent=None):
        super().__init__(parent)
        self.queue_manager = queue_manager

    def run(self):
        try:
            if self.queue_manager and self.queue_manager.isRunning():
                try:
                    self.queue_manager.stop()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            process_tracker.kill_all()
        except Exception:
            pass


class CloakUpdateWorker(QThread):
    status_changed = Signal(str, str)
    finished = Signal(bool, str)

    def __init__(self, install_mode=False, parent=None):
        super().__init__(parent)
        self.install_mode = bool(install_mode)

    @staticmethod
    def _configure_cloak_env():
        if getattr(sys, "frozen", False):
            cache_dir = get_app_data_dir() / "cloakbrowser_cache"
        else:
            cache_dir = Path.home() / ".cloakbrowser"
        cache_dir.mkdir(parents=True, exist_ok=True)
        os.environ["CLOAKBROWSER_CACHE_DIR"] = str(cache_dir.resolve())

    def run(self):
        import importlib

        self._configure_cloak_env()

        try:
            is_frozen = getattr(sys, "frozen", False)

            if is_frozen:
                self.status_changed.emit(
                    "⏳ Checking binary updates (pip update not available in .exe)...",
                    "#F59E0B",
                )
            else:
                self.status_changed.emit("⏳ Updating CloakBrowser package...", "#60A5FA")
                python_exe = sys.executable
                pip_cmd = [
                    python_exe,
                    "-m",
                    "pip",
                    "install",
                    "cloakbrowser",
                    "--upgrade",
                    "--quiet",
                ]
                try:
                    _no_win = {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)} if sys.platform.startswith("win") else {}
                    result = subprocess.run(
                        pip_cmd,
                        capture_output=True,
                        text=True,
                        timeout=120,
                        **_no_win,
                    )
                    if result.returncode != 0:
                        pip_cmd_with_break = list(pip_cmd) + ["--break-system-packages"]
                        result = subprocess.run(
                            pip_cmd_with_break,
                            capture_output=True,
                            text=True,
                            timeout=120,
                            **_no_win,
                        )
                        if result.returncode != 0:
                            error_msg = (result.stderr or result.stdout or "Unknown error").strip()[:100]
                            self.status_changed.emit(f"⚠ pip update failed: {error_msg}", "#F59E0B")
                except subprocess.TimeoutExpired:
                    self.status_changed.emit("⚠ pip update timed out. Checking binary...", "#F59E0B")
                except FileNotFoundError:
                    self.status_changed.emit("⚠ pip not found. Checking binary...", "#F59E0B")

            self.status_changed.emit("⏳ Checking for binary updates...", "#60A5FA")
            importlib.invalidate_caches()

            try:
                cloakbrowser = importlib.import_module("cloakbrowser")
                cloakbrowser = importlib.reload(cloakbrowser)
            except ImportError:
                self.finished.emit(False, "CloakBrowser not installed. Install via pip first.")
                return

            try:
                binary_info = getattr(cloakbrowser, "binary_info")
                ensure_binary = importlib.import_module("cloakbrowser.download").ensure_binary
            except Exception as exc:
                self.finished.emit(False, f"CloakBrowser update components unavailable: {str(exc)[:100]}")
                return

            info_before = binary_info() or {}
            version_before = str(info_before.get("version") or "none")

            self.status_changed.emit("⏳ Downloading latest binary if available...", "#60A5FA")
            ensure_binary()

            info_after = binary_info() or {}
            version_after = str(info_after.get("version") or "none")
            installed = bool(info_after.get("installed"))
            pkg_version = str(getattr(cloakbrowser, "__version__", "unknown"))

            if not installed:
                self.finished.emit(False, "Binary download failed.")
                return

            if version_before != version_after:
                self.finished.emit(True, f"Updated! {version_before} → {version_after}")
            else:
                self.finished.emit(
                    True,
                    f"Already on latest version (v{pkg_version}, binary {version_after})",
                )
        except Exception as exc:
            error_str = str(exc)[:100]
            lower_error = error_str.lower()
            if any(token in lower_error for token in ("connection", "urlerror", "getaddrinfo", "timed out", "network")):
                self.finished.emit(False, "No internet connection. Try again later.")
            else:
                self.finished.emit(False, f"Update failed: {error_str}")


class ProxyConfigDialog(QDialog):
    def __init__(self, account_name, current_proxy=None, parent=None):
        super().__init__(parent)
        self.account_name = str(account_name or "").strip()
        self.setWindowTitle(f"Proxy Settings - {self.account_name or 'Account'}")
        self.setMinimumWidth(450)
        self.setStyleSheet(
            """
            QDialog { background: #1E293B; color: #F8FAFC; }
            QLabel { color: #E2E8F0; }
            QLineEdit, QComboBox {
                background: #0F172A;
                color: #F8FAFC;
                border: 1px solid #334155;
                border-radius: 6px;
                padding: 8px 10px;
            }
            QCheckBox { color: #F8FAFC; }
            """
        )

        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        self.chk_enable = QCheckBox("Enable Proxy for this account")
        self.chk_enable.setStyleSheet("font-weight: 600; font-size: 13px;")
        self.chk_enable.toggled.connect(self._toggle_fields)
        layout.addWidget(self.chk_enable)

        self.fields_widget = QWidget()
        form = QFormLayout(self.fields_widget)
        form.setSpacing(10)

        self.cmb_protocol = QComboBox()
        self.cmb_protocol.addItems(["HTTP", "HTTPS", "SOCKS4", "SOCKS5"])
        self.cmb_protocol.setCurrentText("SOCKS5")
        form.addRow("Protocol:", self.cmb_protocol)

        self.txt_host = QLineEdit()
        self.txt_host.setPlaceholderText("e.g. proxy.example.com or 45.xx.xx.xx")
        form.addRow("Host:", self.txt_host)

        self.txt_port = QLineEdit()
        self.txt_port.setPlaceholderText("e.g. 1080, 8080, 3128")
        self.txt_port.setMaximumWidth(120)
        form.addRow("Port:", self.txt_port)

        self.chk_auth = QCheckBox("Requires username/password")
        self.chk_auth.toggled.connect(self._toggle_auth)
        form.addRow("", self.chk_auth)

        self.txt_user = QLineEdit()
        self.txt_user.setPlaceholderText("Username")
        self.lbl_user = QLabel("Username:")
        self.txt_pass = QLineEdit()
        self.txt_pass.setPlaceholderText("Password")
        self.txt_pass.setEchoMode(QLineEdit.Password)
        self.lbl_pass = QLabel("Password:")
        self.chk_show_pass = QCheckBox("Show password")
        self.chk_show_pass.toggled.connect(
            lambda checked: self.txt_pass.setEchoMode(
                QLineEdit.Normal if checked else QLineEdit.Password
            )
        )

        form.addRow(self.lbl_user, self.txt_user)
        form.addRow(self.lbl_pass, self.txt_pass)
        form.addRow("", self.chk_show_pass)
        layout.addWidget(self.fields_widget)

        self.lbl_preview = QLabel("")
        self.lbl_preview.setStyleSheet(
            "color: #94A3B8; font-size: 11px; padding: 8px; background: #0F172A; border-radius: 4px;"
        )
        self.lbl_preview.setWordWrap(True)
        layout.addWidget(self.lbl_preview)

        self.btn_test = QPushButton("Test Proxy Connection")
        self.btn_test.setStyleSheet(
            """
            QPushButton {
                background: rgba(59, 130, 246, 0.15);
                color: #3B82F6;
                border: 1px solid #3B82F6;
                border-radius: 6px;
                padding: 8px;
                font-weight: 600;
            }
            """
        )
        self.btn_test.clicked.connect(self._test_proxy)
        layout.addWidget(self.btn_test)

        self.lbl_test_result = QLabel("")
        self.lbl_test_result.setWordWrap(True)
        layout.addWidget(self.lbl_test_result)

        btn_row = QHBoxLayout()
        self.btn_save = QPushButton("Save")
        self.btn_save.setStyleSheet(
            """
            QPushButton {
                background: #3B82F6;
                color: white;
                border-radius: 6px;
                padding: 10px 24px;
                font-weight: 700;
            }
            """
        )
        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.setStyleSheet(
            "color: #94A3B8; border: 1px solid #475569; border-radius: 6px; padding: 10px 24px;"
        )
        self.btn_save.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)
        btn_row.addStretch()
        btn_row.addWidget(self.btn_cancel)
        btn_row.addWidget(self.btn_save)
        layout.addLayout(btn_row)

        for widget in (self.txt_host, self.txt_port, self.txt_user, self.txt_pass):
            widget.textChanged.connect(self._update_preview)
        self.cmb_protocol.currentTextChanged.connect(self._update_preview)

        if current_proxy:
            self._load_proxy(current_proxy)

        self._toggle_auth(False)
        self.fields_widget.setVisible(False)
        self._update_preview()

    def _toggle_fields(self, enabled):
        self.fields_widget.setVisible(bool(enabled))
        self.btn_test.setEnabled(bool(enabled))
        self._update_preview()

    def _toggle_auth(self, checked):
        visible = bool(checked)
        self.txt_user.setVisible(visible)
        self.lbl_user.setVisible(visible)
        self.txt_pass.setVisible(visible)
        self.lbl_pass.setVisible(visible)
        self.chk_show_pass.setVisible(visible)
        self._update_preview()

    def _update_preview(self):
        if not self.chk_enable.isChecked():
            self.lbl_preview.setText("Proxy: Disabled (direct connection)")
            return
        url = self.get_proxy_url()
        if url:
            self.lbl_preview.setText(f"Proxy URL: {url}")
        else:
            self.lbl_preview.setText("Fill host and port")

    def get_proxy_url(self):
        if not self.chk_enable.isChecked():
            return ""

        protocol = self.cmb_protocol.currentText().strip().lower()
        host = self.txt_host.text().strip()
        port = self.txt_port.text().strip()
        if not host or not port:
            return ""

        if self.chk_auth.isChecked():
            user = self.txt_user.text().strip()
            pwd = self.txt_pass.text().strip()
            if user and pwd:
                return f"{protocol}://{user}:{pwd}@{host}:{port}"

        return f"{protocol}://{host}:{port}"

    def _load_proxy(self, proxy_url):
        parsed = urlparse(str(proxy_url or "").strip())
        if not parsed.scheme:
            return
        self.chk_enable.setChecked(True)
        protocol = parsed.scheme.upper()
        if protocol in {"HTTP", "HTTPS", "SOCKS4", "SOCKS5"}:
            self.cmb_protocol.setCurrentText(protocol)
        self.txt_host.setText(parsed.hostname or "")
        self.txt_port.setText(str(parsed.port) if parsed.port else "")
        if parsed.username:
            self.chk_auth.setChecked(True)
            self.txt_user.setText(parsed.username)
            self.txt_pass.setText(parsed.password or "")

    def _test_proxy(self):
        url = self.get_proxy_url()
        if not url:
            self.lbl_test_result.setText("Fill proxy details first")
            self.lbl_test_result.setStyleSheet("color: #EF4444;")
            return

        self.lbl_test_result.setText("Testing...")
        self.lbl_test_result.setStyleSheet("color: #F59E0B;")
        QApplication.processEvents()

        try:
            import requests
        except Exception as exc:
            self.lbl_test_result.setText(f"Failed: requests not available ({exc})")
            self.lbl_test_result.setStyleSheet("color: #EF4444;")
            return

        try:
            proxies = {"http": url, "https": url}
            response = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=10)
            if response.status_code == 200:
                ip_value = response.json().get("origin", "unknown")
                self.lbl_test_result.setText(f"Connected. Proxy IP: {ip_value}")
                self.lbl_test_result.setStyleSheet("color: #22C55E;")
            else:
                self.lbl_test_result.setText(f"HTTP {response.status_code}")
                self.lbl_test_result.setStyleSheet("color: #EF4444;")
        except Exception as exc:
            self.lbl_test_result.setText(f"Failed: {str(exc)[:120]}")
            self.lbl_test_result.setStyleSheet("color: #EF4444;")


class BulkQueueAddWorker(QThread):
    progress = Signal(int, int)
    completed = Signal(int)
    failed = Signal(str)

    def __init__(self, job_specs, parent=None):
        super().__init__(parent)
        self.job_specs = list(job_specs or [])

    def run(self):
        try:
            inserted = add_jobs_bulk(
                self.job_specs,
                progress_cb=lambda done, total: self.progress.emit(int(done), int(total)),
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.completed.emit(int(inserted or 0))


class WorkerSignals(QObject):
    finished = Signal(object)
    error = Signal(str)


class BackgroundTask(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    def run(self):
        try:
            result = self.fn(*self.args, **self.kwargs)
        except Exception as exc:
            self.signals.error.emit(str(exc))
            return
        self.signals.finished.emit(result)


class UIUpdateThrottler(QObject):
    def __init__(self, parent=None, interval_ms=250):
        super().__init__(parent)
        self._pending = {}
        self._timer = QTimer(self)
        self._timer.setInterval(max(50, int(interval_ms)))
        self._timer.timeout.connect(self._flush)
        self._timer.start()

    def schedule(self, key, callback):
        if callable(callback):
            self._pending[str(key)] = callback

    def _flush(self):
        if not self._pending:
            return
        updates = list(self._pending.items())
        self._pending.clear()
        for _key, callback in updates:
            try:
                callback()
            except Exception:
                pass


class LogBuffer(QObject):
    def __init__(self, text_edit, parent=None, interval_ms=200):
        super().__init__(parent)
        self.text_edit = text_edit
        self._pending = []
        self._timer = QTimer(self)
        self._timer.setInterval(max(50, int(interval_ms)))
        self._timer.timeout.connect(self.flush)
        self._timer.start()

    def append(self, message):
        if message is None:
            return
        self._pending.append(str(message))

    def clear(self):
        self._pending.clear()
        if self.text_edit is not None:
            self.text_edit.clear()

    def flush(self):
        if not self._pending or self.text_edit is None:
            return
        payload = "\n".join(self._pending)
        self._pending.clear()
        cursor = self.text_edit.textCursor()
        cursor.movePosition(QTextCursor.End)
        if self.text_edit.document().blockCount() > 0:
            cursor.insertText("\n")
        cursor.insertText(payload)
        self.text_edit.setTextCursor(cursor)
        scrollbar = self.text_edit.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())


class VirtualLiveGridWidget(QWidget):
    CARD_SIZES = {
        "small": (156, 210),
        "medium": (216, 286),
        "large": (280, 360),
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self._jobs = []
        self._card_size = "medium"
        self._columns = 1
        self._spacing = 12
        self.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent, True)
        self.setAutoFillBackground(False)

    def set_card_size(self, size_key):
        normalized = str(size_key or "medium").strip().lower()
        if normalized not in self.CARD_SIZES:
            normalized = "medium"
        if self._card_size != normalized:
            self._card_size = normalized
            self._reflow()

    def set_jobs(self, jobs):
        self._jobs = [dict(job or {}) for job in list(jobs or [])]
        self._reflow()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._reflow()

    def sizeHint(self):
        width = max(320, self.width() or 320)
        height = max(220, self.minimumHeight() or 220)
        return QSize(width, height)

    def _reflow(self):
        card_w, card_h = self.CARD_SIZES.get(self._card_size, self.CARD_SIZES["medium"])
        available_width = max(320, self.width() or self.parentWidget().width() if self.parentWidget() else 320)
        self._columns = max(1, (available_width + self._spacing) // (card_w + self._spacing))
        rows = max(1, (len(self._jobs) + self._columns - 1) // self._columns)
        total_height = rows * (card_h + self._spacing) + self._spacing
        self.setMinimumHeight(total_height)
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.fillRect(event.rect(), QColor("#0F172A"))

        card_w, card_h = self.CARD_SIZES.get(self._card_size, self.CARD_SIZES["medium"])
        row_height = card_h + self._spacing
        first_row = max(0, event.rect().top() // max(1, row_height))
        last_row = max(first_row, (event.rect().bottom() // max(1, row_height)) + 1)

        status_colors = {
            "pending": QColor("#94A3B8"),
            "running": QColor("#3B82F6"),
            "completed": QColor("#22C55E"),
            "failed": QColor("#EF4444"),
            "moderated": QColor("#F59E0B"),
        }

        prompt_font = QFont()
        prompt_font.setPointSize(10)
        prompt_font.setBold(True)
        meta_font = QFont()
        meta_font.setPointSize(9)
        header_font = QFont()
        header_font.setPointSize(9)
        header_font.setBold(True)

        for row in range(first_row, last_row + 1):
            for col in range(self._columns):
                idx = row * self._columns + col
                if idx >= len(self._jobs):
                    break

                job = self._jobs[idx]
                x = self._spacing + col * (card_w + self._spacing)
                y = self._spacing + row * row_height
                rect = QRectF(x, y, card_w, card_h)

                status = str(job.get("status") or "pending").strip().lower()
                status_color = status_colors.get(status, QColor("#94A3B8"))
                queue_no = job.get("output_index") if job.get("is_retry") else job.get("queue_no")
                if queue_no is None:
                    queue_no = idx + 1
                prompt = str(job.get("prompt") or "(No prompt)")
                meta = str(job.get("account") or "Waiting in queue")
                progress = str(job.get("progress") or "")

                painter.setPen(QPen(QColor("#334155"), 1))
                painter.setBrush(QColor("#1A2744") if job.get("is_retry") else QColor("#1E293B"))
                painter.drawRoundedRect(rect, 10, 10)

                painter.setFont(header_font)
                painter.setPen(QColor("#F8FAFC"))
                painter.drawText(QRectF(x + 12, y + 12, card_w - 24, 20), Qt.AlignLeft | Qt.AlignVCenter, f"#{queue_no}")
                painter.setPen(status_color)
                painter.drawText(QRectF(x + 12, y + 12, card_w - 24, 20), Qt.AlignRight | Qt.AlignVCenter, status.upper())

                painter.setBrush(QColor("#0F172A"))
                painter.setPen(QPen(QColor("#334155"), 1))
                painter.drawRoundedRect(QRectF(x + 12, y + 40, card_w - 24, max(72, int(card_h * 0.36))), 8, 8)
                painter.setPen(status_color if status == "completed" else QColor("#64748B"))
                painter.drawText(
                    QRectF(x + 12, y + 40, card_w - 24, max(72, int(card_h * 0.36))),
                    Qt.AlignCenter,
                    "DONE" if status == "completed" else ("FAIL" if status in ("failed", "moderated") else "LIVE"),
                )

                painter.setFont(prompt_font)
                painter.setPen(QColor("#F8FAFC"))
                prompt_rect = QRectF(x + 12, y + 126, card_w - 24, 42)
                prompt_text = painter.fontMetrics().elidedText(prompt.replace("\n", " "), Qt.ElideRight, max(40, int(prompt_rect.width() * 2)))
                painter.drawText(prompt_rect, Qt.TextWordWrap, prompt_text)

                painter.setFont(meta_font)
                painter.setPen(QColor("#94A3B8"))
                painter.drawText(QRectF(x + 12, y + card_h - 52, card_w - 24, 18), Qt.AlignLeft | Qt.AlignVCenter, meta[:48])
                painter.drawText(
                    QRectF(x + 12, y + card_h - 30, card_w - 24, 18),
                    Qt.AlignLeft | Qt.AlignVCenter,
                    progress[:48] if progress else str(job.get("job_type") or "image").title(),
                )

        painter.end()


class ThrottledTableUpdater(QObject):
    def __init__(self, parent, flush_callback, interval_ms=200):
        super().__init__(parent)
        self.flush_callback = flush_callback
        self.pending_job_ids = set()
        self.timer = QTimer(self)
        self.timer.setInterval(max(50, int(interval_ms)))
        self.timer.timeout.connect(self._flush)
        self.timer.start()

    def queue_job(self, job_id):
        if job_id:
            self.pending_job_ids.add(str(job_id))

    def queue_many(self, job_ids):
        for job_id in list(job_ids or []):
            self.queue_job(job_id)

    def _flush(self):
        if not self.pending_job_ids:
            return
        pending = list(self.pending_job_ids)
        self.pending_job_ids.clear()
        self.flush_callback(pending)


class ThrottledStatsUpdater(QObject):
    def __init__(self, parent, update_callback, interval_ms=500):
        super().__init__(parent)
        self.update_callback = update_callback
        self._latest_jobs = None
        self._dirty = False
        self.timer = QTimer(self)
        self.timer.setInterval(max(100, int(interval_ms)))
        self.timer.timeout.connect(self._update)
        self.timer.start()

    def mark_dirty(self, jobs=None):
        if jobs is not None:
            self._latest_jobs = list(jobs)
        self._dirty = True

    def _update(self):
        if not self._dirty:
            return
        self._dirty = False
        self.update_callback(self._latest_jobs)


class BulkImageDropTable(QTableWidget):
    files_dropped = Signal(list)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setAcceptDrops(True)
        self.viewport().setAcceptDrops(True)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            return
        super().dragMoveEvent(event)

    def dropEvent(self, event):
        urls = event.mimeData().urls()
        paths = [url.toLocalFile() for url in urls if url.isLocalFile()]
        if paths:
            self.files_dropped.emit(paths)
            event.acceptProposedAction()
            return
        super().dropEvent(event)


class LiveJobCard(QFrame):
    CARD_SIZES = {
        "small": (156, 210),
        "medium": (216, 286),
        "large": (280, 360),
    }

    def __init__(self, job_data, card_size="medium", parent=None):
        super().__init__(parent)
        self.job_data = dict(job_data or {})
        self.card_size = str(card_size or "medium")
        self.setObjectName("liveJobCard")
        self._build_ui()
        self._apply_card_size()
        self.update_job(job_data or {})

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(6)

        self.preview_label = QLabel()
        self.preview_label.setAlignment(Qt.AlignCenter)
        self.preview_label.setObjectName("livePreview")
        layout.addWidget(self.preview_label)

        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(6)

        self.lbl_job_num = QLabel("#?")
        self.lbl_job_num.setObjectName("liveJobNumber")
        header_layout.addWidget(self.lbl_job_num)
        header_layout.addStretch()

        self.lbl_status = QLabel("")
        self.lbl_status.setObjectName("liveJobStatus")
        header_layout.addWidget(self.lbl_status)
        layout.addLayout(header_layout)

        self.job_progress = QProgressBar()
        self.job_progress.setRange(0, 100)
        self.job_progress.setTextVisible(False)
        self.job_progress.setFixedHeight(6)
        self.job_progress.setObjectName("liveJobProgress")
        self.job_progress.setVisible(False)
        layout.addWidget(self.job_progress)

        self.lbl_prompt = QLabel("")
        self.lbl_prompt.setWordWrap(True)
        self.lbl_prompt.setObjectName("liveJobPrompt")
        layout.addWidget(self.lbl_prompt)

        self.lbl_meta = QLabel("")
        self.lbl_meta.setWordWrap(True)
        self.lbl_meta.setObjectName("liveJobMeta")
        layout.addWidget(self.lbl_meta)
        layout.addStretch()

    def _apply_card_size(self):
        width, height = self.CARD_SIZES.get(self.card_size, self.CARD_SIZES["medium"])
        self.setFixedSize(width, height)
        preview_height = max(96, width - 28)
        if self.card_size == "large":
            preview_height = max(140, width - 34)
        self.preview_label.setFixedHeight(preview_height)

    def _set_preview_text(self, text, *, bg="#0F172A", color="#64748B", border="#334155"):
        self.preview_label.setPixmap(QPixmap())
        self.preview_label.setText(text)
        self.preview_label.setStyleSheet(
            f"background: {bg}; color: {color}; border: 1px solid {border}; "
            "border-radius: 8px; font-size: 30px; font-weight: 700;"
        )

    def _set_status_style(self, text, color):
        self.lbl_status.setText(text)
        self.lbl_status.setStyleSheet(
            f"color: {color}; font-size: 11px; font-weight: 700; background: transparent; border: none;"
        )

    def _estimate_progress(self):
        status = str(self.job_data.get("status") or "pending").strip().lower()
        if status == "completed":
            return 100
        if status != "running":
            return 0

        job_type = str(self.job_data.get("job_type") or "").strip().lower()
        step = str(self.job_data.get("progress_step") or "").strip().lower()
        poll_count = max(0, int(self.job_data.get("progress_poll_count") or 0))

        if job_type == "pipeline":
            if step == "image":
                return min(30, 10 + (poll_count * 8))
            if step == "download":
                return 95
            return 30 + min(65, poll_count * 7)

        if step == "image":
            return min(90, 20 + (poll_count * 25))
        if step == "download":
            return 95
        return min(95, max(8, poll_count * 10))

    @staticmethod
    def _extract_video_thumbnail(video_path):
        target = str(video_path or "").strip()
        if not target or not os.path.exists(target):
            return None
        thumb_path = f"{target}_thumb.jpg"
        if os.path.exists(thumb_path):
            return thumb_path
        try:
            _no_window = {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)} if sys.platform.startswith("win") else {}
            subprocess.run(
                [
                    "ffmpeg",
                    "-i",
                    target,
                    "-vf",
                    "select=eq(n\\,0)",
                    "-frames:v",
                    "1",
                    "-y",
                    thumb_path,
                ],
                capture_output=True,
                timeout=8,
                check=False,
                **_no_window,
            )
        except Exception:
            return None
        return thumb_path if os.path.exists(thumb_path) else None

    def _load_preview(self, output_path):
        resolved = str(output_path or "").strip()
        if not resolved or not os.path.exists(resolved):
            self._set_preview_text("✅", bg="#112020", color="#22C55E", border="#22C55E")
            return

        preview_width = max(96, self.preview_label.width() - 8)
        preview_height = max(96, self.preview_label.height() - 8)
        source_path = resolved
        suffix = Path(resolved).suffix.lower()
        if suffix == ".mp4":
            thumb_path = self._extract_video_thumbnail(resolved)
            if thumb_path:
                source_path = thumb_path
            else:
                self._set_preview_text("🎬", bg="#112020", color="#22C55E", border="#22C55E")
                return

        pixmap = QPixmap(source_path)
        if pixmap.isNull():
            fallback = "🎬" if suffix == ".mp4" else "🖼"
            self._set_preview_text(fallback, bg="#112020", color="#22C55E", border="#22C55E")
            return

        scaled = pixmap.scaled(preview_width, preview_height, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self.preview_label.setText("")
        self.preview_label.setStyleSheet("background: #0F172A; border: 1px solid #334155; border-radius: 8px;")
        self.preview_label.setPixmap(scaled)

    def update_job(self, job_data):
        self.job_data = dict(job_data or {})
        queue_no = self.job_data.get("queue_no") or self.job_data.get("index") or "?"
        self.lbl_job_num.setText(f"Job {queue_no}")

        prompt = str(self.job_data.get("prompt") or "").strip()
        prompt_snippet = prompt if len(prompt) <= 56 else (prompt[:53] + "...")
        self.lbl_prompt.setText(prompt_snippet or "(No prompt)")
        self.lbl_prompt.setToolTip(prompt or "(No prompt)")

        status = str(self.job_data.get("status") or "pending").strip().lower()
        output_path = str(self.job_data.get("output_path") or "").strip()
        error_text = str(self.job_data.get("error") or "").strip()
        progress_step = str(self.job_data.get("progress_step") or "").strip().lower()
        poll_count = max(0, int(self.job_data.get("progress_poll_count") or 0))

        self.job_progress.setVisible(False)
        self.job_progress.setValue(0)
        self.job_progress.setStyleSheet(
            "QProgressBar { background: #334155; border: none; border-radius: 3px; } "
            "QProgressBar::chunk { background: #3B82F6; border-radius: 3px; }"
        )

        if status == "completed":
            self._set_status_style("✅ Done", "#22C55E")
            self.lbl_meta.setText(os.path.basename(output_path) if output_path else "Output ready")
            self.lbl_meta.setToolTip(output_path or "")
            self.job_progress.setValue(100)
            self.job_progress.setStyleSheet(
                "QProgressBar { background: #334155; border: none; border-radius: 3px; } "
                "QProgressBar::chunk { background: #22C55E; border-radius: 3px; }"
            )
            self.job_progress.setVisible(True)
            self._load_preview(output_path)
            return

        if status == "failed":
            self._set_status_style("❌ Failed", "#EF4444")
            meta = error_text if len(error_text) <= 54 else (error_text[:51] + "...")
            self.lbl_meta.setText(meta or "Generation failed")
            self.lbl_meta.setToolTip(error_text or "Generation failed")
            self._set_preview_text("❌", bg="#1F1A2A", color="#EF4444", border="#EF4444")
            return

        if status == "running":
            progress_value = self._estimate_progress()
            if progress_step == "image":
                status_text = "🖼 Image"
                meta = "Generating source image..."
            elif progress_step == "download":
                status_text = "⬇ Download"
                meta = "Finalizing output..."
            else:
                status_text = f"🔄 {progress_value}%"
                meta = f"poll {poll_count}/10" if poll_count > 0 else "Submitting..."
                if str(self.job_data.get("job_type") or "").strip().lower() == "pipeline":
                    status_text = f"🎬 {progress_value}%"
            self._set_status_style(status_text, "#3B82F6")
            self.lbl_meta.setText(meta)
            self.lbl_meta.setToolTip(meta)
            self.job_progress.setValue(progress_value)
            self.job_progress.setVisible(True)
            self._set_preview_text("🔄", bg="#0F172A", color="#3B82F6", border="#334155")
            return

        self._set_status_style("⏳ Queued", "#94A3B8")
        self.lbl_meta.setText("Waiting in queue")
        self.lbl_meta.setToolTip("Waiting in queue")
        self._set_preview_text("⏳", bg="#0F172A", color="#475569", border="#334155")


class SidebarNav(QFrame):
    page_selected = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("sidebarNav")
        self.setFixedWidth(180)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 14, 10, 14)
        layout.setSpacing(6)

        self.lbl_title = QLabel("G-Labs\nAutomation")
        self.lbl_title.setObjectName("sidebarTitle")
        layout.addWidget(self.lbl_title)

        top_divider = QFrame()
        top_divider.setFrameShape(QFrame.HLine)
        top_divider.setObjectName("sidebarDivider")
        layout.addWidget(top_divider)

        self.nav_buttons = {}
        nav_items = [
            ("dashboard", "Image Generation"),
            ("video", "Video Generation"),
            ("accounts", "Account Manager"),
            ("live", "Live Generation"),
            ("failed", "Failed Jobs"),
            ("settings", "Settings"),
        ]
        for key, label in nav_items:
            btn = QPushButton(label)
            btn.setObjectName("sidebarNavButton")
            btn.setCheckable(True)
            btn.setCursor(Qt.PointingHandCursor)
            btn.clicked.connect(lambda checked=False, nav_key=key: self._emit_nav(nav_key))
            layout.addWidget(btn)
            self.nav_buttons[key] = btn

        layout.addStretch(1)

        bottom_divider = QFrame()
        bottom_divider.setFrameShape(QFrame.HLine)
        bottom_divider.setObjectName("sidebarDivider")
        layout.addWidget(bottom_divider)

        self.lbl_pending = QLabel("Pending: 0")
        self.lbl_pending.setObjectName("sidebarStat")
        self.lbl_running = QLabel("Running: 0")
        self.lbl_running.setObjectName("sidebarStatRunning")
        self.lbl_done = QLabel("Done: 0")
        self.lbl_done.setObjectName("sidebarStatDone")
        self.btn_failed = QPushButton("Failed: 0")
        self.btn_failed.setObjectName("sidebarFailedButton")
        self.btn_failed.setCursor(Qt.PointingHandCursor)
        self.btn_failed.clicked.connect(lambda: self._emit_nav("failed"))
        self.lbl_session = QLabel("Session: 0 images")
        self.lbl_session.setObjectName("sidebarSession")

        layout.addWidget(self.lbl_pending)
        layout.addWidget(self.lbl_running)
        layout.addWidget(self.lbl_done)
        layout.addWidget(self.btn_failed)
        layout.addSpacing(4)
        layout.addWidget(self.lbl_session)

        self.set_active("dashboard")

    def _emit_nav(self, key):
        self.set_active(key)
        self.page_selected.emit(str(key))

    def set_active(self, key):
        current_key = str(key or "")
        for btn_key, button in self.nav_buttons.items():
            button.setChecked(btn_key == current_key)

    def update_stats(self, pending, running, done, failed, session_total):
        self.lbl_pending.setText(f"Pending: {int(pending or 0)}")
        self.lbl_running.setText(f"Running: {int(running or 0)}")
        self.lbl_done.setText(f"Done: {int(done or 0)}")
        self.btn_failed.setText(f"Failed: {int(failed or 0)}")
        self.lbl_session.setText(f"Session: {int(session_total or 0)} images")
        if int(failed or 0) > 0:
            self.btn_failed.setProperty("hasFailures", True)
        else:
            self.btn_failed.setProperty("hasFailures", False)
        self.btn_failed.style().unpolish(self.btn_failed)
        self.btn_failed.style().polish(self.btn_failed)


class MainWindow(QMainWindow):
    warmup_progress_signal = Signal(str, int, str)
    warmup_complete_signal = Signal(str, bool, str)

    def __init__(self):
        super().__init__()
        self._kill_zombie_browsers(startup=True)
        self._cleanup_stale_locks()
        app = QApplication.instance()
        if app is not None and str(app.style().objectName()).lower() != "fusion":
            app.setStyle("Fusion")
        self.setObjectName("mainWindow")
        self.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent, True)
        self.setAutoFillBackground(True)
        self.setWindowTitle("G-Labs Multi-Account Automation App")
        self.resize(1280, 860)
        self.setMinimumSize(1024, 600)
        self.setWindowState(Qt.WindowMaximized)
        
        self.central_widget = QWidget()
        self.central_widget.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent, True)
        self.central_widget.setAutoFillBackground(True)
        self.setCentralWidget(self.central_widget)
        self.main_layout = QHBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        self.tabs = QTabWidget()
        self.tabs.setObjectName("mainTabs")
        self.tabs.setDocumentMode(True)
        self.tabs.tabBar().setObjectName("mainAppTabBar")
        self.tabs.tabBar().setDrawBase(False)
        self.tabs.tabBar().hide()

        self.sidebar = SidebarNav(self)
        self.sidebar.page_selected.connect(self._on_sidebar_page_selected)
        self.main_layout.addWidget(self.sidebar)

        self.sidebar_divider = QFrame()
        self.sidebar_divider.setFrameShape(QFrame.VLine)
        self.sidebar_divider.setObjectName("sidebarShellDivider")
        self.sidebar_divider.setFixedWidth(1)
        self.main_layout.addWidget(self.sidebar_divider)

        self.main_layout.addWidget(self.tabs, 1)
        
        # Tabs
        self.tab_dashboard = QWidget()
        self.tab_accounts = QWidget()
        self.tab_live_generation = QWidget()
        self.tab_failed_jobs = QWidget()
        self.tab_settings = QWidget()
        
        self.tabs.addTab(self.tab_dashboard, "Dashboard")
        self.tabs.addTab(self.tab_accounts, "Account Manager")
        self.tabs.addTab(self.tab_live_generation, "Live Generation")
        self.tabs.addTab(self.tab_failed_jobs, "Failed Jobs")
        self.tabs.addTab(self.tab_settings, "Settings")
        
        self.queue_manager = None
        self.pending_clear_all = False
        self.queue_running = False
        self.queue_paused = False
        self.queue_stopping = False
        self._app_closing = False
        self._pending_settings_sync_ready = False
        self.account_runtime_state = {}
        self.account_login_state = {}
        self._runtime_auth_status = {}  # account_name -> "expired" (cleared on success)
        self.warmup_widgets = {}
        self.active_warmup_progress = {}
        self._pending_login_add = None
        self.failed_prompt_edits = {}
        self.login_worker = None
        self.login_check_worker = None
        self.relogin_worker = None
        self._completion_times = []
        self._generation_start_time = None
        self._terminal_job_states = {}
        self._account_status_auto_check_done = False
        self.bulk_queue_add_worker = None
        self.bulk_add_progress_dialog = None
        self._bulk_add_success_logs = []
        self._bulk_add_after_success = None
        self.thread_pool = QThreadPool.globalInstance()
        self._background_tasks = set()
        self._cleanup_thread = None
        self._cleanup_started = False
        self.ui_throttler = UIUpdateThrottler(self, interval_ms=250)
        self._queue_row_map = {}
        self._queue_job_order = []
        self._queue_row_snapshots = {}
        self._latest_queue_jobs = []
        self._latest_accounts = []
        self._queue_snapshot_inflight = False
        self._queue_snapshot_requested = False
        self._failed_jobs_dirty = True
        self._failed_jobs_inflight = False
        self._latest_failed_jobs = []
        self._live_tab_dirty = True
        self._grid_scrolling = False
        self._pending_live_jobs = None
        self._grid_scroll_resume_timer = QTimer(self)
        self._grid_scroll_resume_timer.setSingleShot(True)
        self._grid_scroll_resume_timer.timeout.connect(self._on_grid_scroll_idle)
        self.account_runtime_timer = QTimer(self)
        self.account_runtime_timer.setInterval(1000)
        self.account_runtime_timer.timeout.connect(self._on_account_runtime_tick)
        self.account_status_timer = QTimer(self)
        self.account_status_timer.setInterval(10000)
        self.account_status_timer.timeout.connect(self._refresh_login_statuses)
        self.warmup_progress_signal.connect(self._on_warmup_progress)
        self.warmup_complete_signal.connect(self._on_warmup_complete)
        
        self.setup_dashboard()
        self.setup_accounts()
        self.setup_live_generation()
        self.setup_failed_jobs()
        self.setup_settings()
        self.table_updater = ThrottledTableUpdater(self, self._flush_queue_table_updates, interval_ms=200)
        self.stats_updater = ThrottledStatsUpdater(self, self._refresh_dashboard_stats, interval_ms=500)
        self.queue_snapshot_timer = QTimer(self)
        self.queue_snapshot_timer.setInterval(1000)
        self.queue_snapshot_timer.timeout.connect(self._request_queue_snapshot)
        self.queue_snapshot_timer.start()
        self.failed_jobs_refresh_timer = QTimer(self)
        self.failed_jobs_refresh_timer.setSingleShot(True)
        self.failed_jobs_refresh_timer.setInterval(400)
        self.failed_jobs_refresh_timer.timeout.connect(lambda: self._request_failed_jobs_refresh(force=False))
        self._apply_modern_theme()
        self._update_runtime_badges()
        self._pending_settings_sync_ready = True
        self.account_runtime_timer.start()
        self.account_status_timer.start()
        self.live_refresh_timer = QTimer(self)
        self.live_refresh_timer.setInterval(2000)
        self.live_refresh_timer.timeout.connect(self._schedule_live_refresh)
        self.live_refresh_timer.start()
        self.progress_timer = QTimer(self)
        self.progress_timer.setInterval(2000)
        self.progress_timer.timeout.connect(self._update_progress_display)
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self._sync_sidebar_selection()
        
    def setup_dashboard(self):
        root_layout = QVBoxLayout(self.tab_dashboard)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        self.dashboard_content = QWidget()
        self.dashboard_content.setObjectName("dashboardContent")

        layout = QVBoxLayout(self.dashboard_content)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(8)

        hero = QFrame()
        hero.setObjectName("dashboardTopBar")
        hero_layout = QHBoxLayout(hero)
        hero_layout.setContentsMargins(14, 10, 14, 10)
        hero_layout.setSpacing(12)

        hero_text_layout = QVBoxLayout()
        hero_text_layout.setSpacing(2)
        self.lbl_hero_title = QLabel("G-Labs Automation Studio")
        self.lbl_hero_title.setObjectName("heroTitle")
        self.lbl_hero_subtitle = QLabel("Compact generation workspace")
        self.lbl_hero_subtitle.setObjectName("heroSubtitle")
        hero_text_layout.addWidget(self.lbl_hero_title)
        hero_text_layout.addWidget(self.lbl_hero_subtitle)

        hero_meta_wrap = QWidget()
        hero_meta_layout = QHBoxLayout(hero_meta_wrap)
        hero_meta_layout.setContentsMargins(0, 0, 0, 0)
        hero_meta_layout.setSpacing(6)
        self.lbl_runtime_mode = QLabel("Mode: Hybrid")
        self.lbl_runtime_mode.setObjectName("metaBadge")
        self.lbl_runtime_parallel = QLabel("Parallel: 1/account")
        self.lbl_runtime_parallel.setObjectName("metaBadge")
        self.lbl_queue_status = QLabel("Queue: STOPPED")
        self.lbl_queue_status.setObjectName("metaBadge")
        self.lbl_session_stats = QLabel("Session: 0 images generated")
        self.lbl_session_stats.setStyleSheet(
            "background-color: #1E293B; color: #94A3B8; padding: 4px 12px; "
            "border-radius: 4px; font-size: 12px;"
        )
        hero_meta_layout.addWidget(self.lbl_runtime_mode)
        hero_meta_layout.addWidget(self.lbl_runtime_parallel)
        hero_meta_layout.addWidget(self.lbl_queue_status)
        hero_meta_layout.addWidget(self.lbl_session_stats)

        self.toolbar_actions_host = QWidget()
        self.toolbar_actions_layout = QHBoxLayout(self.toolbar_actions_host)
        self.toolbar_actions_layout.setContentsMargins(0, 0, 0, 0)
        self.toolbar_actions_layout.setSpacing(8)

        hero_layout.addLayout(hero_text_layout, stretch=1)
        hero_layout.addWidget(hero_meta_wrap, 0, Qt.AlignRight)
        hero_layout.addWidget(self.toolbar_actions_host, 0, Qt.AlignRight)
        layout.addWidget(hero)
        self._apply_card_shadow(hero, blur=28, y_offset=8)

        self.warning_container = QWidget()
        self.warning_container.setVisible(False)
        self.warning_container.setStyleSheet(
            "QWidget { background: #1A1520; border: 1px solid #F59E0B; border-radius: 8px; }"
        )
        warning_layout = QHBoxLayout(self.warning_container)
        warning_layout.setContentsMargins(12, 8, 12, 8)
        warning_layout.setSpacing(10)
        self.warning_banner = QLabel("")
        self.warning_banner.setWordWrap(True)
        self.warning_banner.setStyleSheet(
            "background: transparent; color: #F59E0B; border: none; "
            "padding: 0px; font-weight: 600; font-size: 13px;"
        )
        btn_dismiss_warning = QPushButton("✕")
        btn_dismiss_warning.setFixedSize(24, 24)
        btn_dismiss_warning.setCursor(Qt.PointingHandCursor)
        btn_dismiss_warning.setStyleSheet(
            "QPushButton { background: transparent; color: #F59E0B; border: none; font-size: 16px; font-weight: 700; }"
            "QPushButton:hover { color: #FCD34D; }"
        )
        btn_dismiss_warning.clicked.connect(lambda: self.warning_container.setVisible(False))
        warning_layout.addWidget(self.warning_banner, 1)
        warning_layout.addWidget(btn_dismiss_warning, 0, Qt.AlignTop)
        layout.addWidget(self.warning_container)

        stats_frame = QFrame()
        stats_frame.setObjectName("statsRow")
        stats_layout = QHBoxLayout(stats_frame)
        stats_layout.setContentsMargins(0, 0, 0, 0)
        stats_layout.setSpacing(12)

        def make_stat_card(title_text, accent, clickable=False):
            card = QFrame()
            card.setObjectName("statCard")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(14, 12, 14, 12)
            card_layout.setSpacing(4)
            if clickable:
                value = QPushButton("0")
                value.setCursor(Qt.PointingHandCursor)
                value.setToolTip("Click to view failed jobs")
                value.setStyleSheet(self._failed_stat_button_style(False))
                value.clicked.connect(self._go_to_failed_tab)
            else:
                value = QLabel("0")
                value.setObjectName("statValue")
                value.setStyleSheet(
                    f"color: {accent}; font-size: 28px; font-weight: 800; background: transparent; border: none;"
                )
            title = QLabel(title_text)
            title.setObjectName("statTitle")
            title.setStyleSheet(
                "color: #64748B; font-size: 11px; font-weight: 600; letter-spacing: 1px; "
                "text-transform: uppercase; background: transparent; border: none;"
            )
            if clickable:
                title.setStyleSheet(
                    "color: #EF4444; font-size: 12px; font-weight: 600; letter-spacing: 1px; "
                    "text-transform: uppercase; background: transparent; border: none;"
                )
            card_layout.addWidget(value)
            card_layout.addWidget(title)
            self._apply_card_shadow(card, blur=24, y_offset=8)
            return card, value

        pending_card, self.stat_pending = make_stat_card("Pending", "#94A3B8")
        running_card, self.stat_running = make_stat_card("Running", "#3B82F6")
        completed_card, self.stat_completed = make_stat_card("Done", "#22C55E")
        failed_card, self.btn_failed_count = make_stat_card("Failed", "#EF4444", clickable=True)
        self.stat_failed = self.btn_failed_count

        stats_layout.addWidget(pending_card, 1)
        stats_layout.addWidget(running_card, 1)
        stats_layout.addWidget(completed_card, 1)
        stats_layout.addWidget(failed_card, 1)
        stats_frame.setVisible(False)
        layout.addWidget(stats_frame)
        self.progress_widget = QWidget()
        progress_layout = QHBoxLayout(self.progress_widget)
        progress_layout.setContentsMargins(0, 2, 0, 2)
        progress_layout.setSpacing(10)
        self.overall_progress = QProgressBar()
        self.overall_progress.setRange(0, 100)
        self.overall_progress.setValue(0)
        self.overall_progress.setFixedHeight(18)
        self.overall_progress.setStyleSheet(
            "QProgressBar { border: 1px solid #334155; border-radius: 4px; background-color: #1E293B; "
            "text-align: center; color: white; font-size: 12px; font-weight: 600; } "
            "QProgressBar::chunk { background-color: #2563EB; border-radius: 3px; }"
        )
        self.lbl_progress_text = QLabel("0/0 (0%)")
        self.lbl_progress_text.setStyleSheet("color: #94A3B8; font-size: 11px; min-width: 92px;")
        self.lbl_speed = QLabel("Speed: --")
        self.lbl_speed.setStyleSheet("color: #60A5FA; font-size: 11px; min-width: 100px;")
        self.lbl_eta = QLabel("ETA: --")
        self.lbl_eta.setStyleSheet("color: #F59E0B; font-size: 11px; min-width: 90px;")
        progress_layout.addWidget(self.overall_progress, 1)
        progress_layout.addWidget(self.lbl_progress_text)
        progress_layout.addWidget(self.lbl_speed)
        progress_layout.addWidget(self.lbl_eta)
        layout.addWidget(self.progress_widget)
        self.current_ref_path = None
        self.current_ref_paths = []
        self.current_start_image_path = None
        self.current_end_image_path = None
        self.current_pipe_ref_paths = []
        self.bulk_panels = {}

        saved_slots = max(1, min(5, get_int_setting("slots_per_account", 3)))
        self._mode_tab_scrolls = {}

        self.mode_tabs = QTabWidget()
        self.mode_tabs.setObjectName("modeTabs")
        self.mode_tabs.setDocumentMode(True)
        self.mode_tabs.tabBar().setDrawBase(False)
        self.mode_tabs.setMinimumHeight(180)
        self.mode_tabs.setMaximumHeight(16777215)
        self.mode_tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.mode_tabs.currentChanged.connect(self._on_mode_tab_changed)

        self.mode_tab_image = QWidget()
        self.mode_tab_t2v = QWidget()
        self.mode_tab_ref = QWidget()
        self.mode_tab_frames = QWidget()
        self.mode_tab_pipeline = QWidget()
        self.mode_tabs.addTab(self.mode_tab_image, "Image")
        self.mode_tabs.addTab(self.mode_tab_t2v, "Video")
        self.mode_tabs.addTab(self.mode_tab_ref, "Video + Ref")
        self.mode_tabs.addTab(self.mode_tab_frames, "Video + Frames")
        self.mode_tabs.addTab(self.mode_tab_pipeline, "Image -> Video")

        self._setup_image_mode_tab(saved_slots)
        self._setup_video_t2v_tab(saved_slots)
        self._setup_video_ref_tab(saved_slots)
        self._setup_video_frames_tab(saved_slots)
        self._setup_pipeline_tab(saved_slots)
        self._remove_stray_mode_tab_buttons()
        self._adjust_mode_tabs_height()

        self.prompts_group = QGroupBox("Prompts Input")
        self.prompts_group.setObjectName("dashboardPanel")
        prompts_layout = QVBoxLayout(self.prompts_group)
        prompts_layout.setContentsMargins(12, 12, 12, 12)
        prompts_layout.setSpacing(8)
        prompts_header = QHBoxLayout()
        prompts_header.setContentsMargins(0, 0, 0, 0)
        prompts_header.setSpacing(8)
        self.lbl_prompts_title = QLabel("Enter Prompts")
        self.lbl_prompts_title.setStyleSheet("color: #F8FAFC; font-size: 12px; font-weight: 700;")
        self.btn_import_txt = QPushButton("Import TXT")
        self.btn_import_txt.setFixedHeight(26)
        self.btn_import_txt.setStyleSheet(
            "QPushButton { background-color: #334155; color: #94A3B8; font-size: 11px; "
            "border: 1px solid #475569; border-radius: 4px; padding: 0 10px; } "
            "QPushButton:hover { background-color: #475569; color: white; }"
        )
        self.btn_import_txt.clicked.connect(self._import_prompts_txt)
        prompts_header.addWidget(self.lbl_prompts_title)
        prompts_header.addStretch(1)
        prompts_header.addWidget(self.btn_import_txt)
        prompts_layout.addLayout(prompts_header)
        self.prompts_input = QTextEdit()
        self.prompts_input.setPlaceholderText(
            "Paste your prompts here, one per line...\n\n"
            "Example:\n"
            "A serene mountain landscape at sunset with golden light\n"
            "A futuristic city skyline with neon lights and flying cars\n"
            "A cozy coffee shop interior with warm lighting\n\n"
            "Tips:\n"
            "• One prompt per line\n"
            "• No limit on number of prompts\n"
            "• Detailed prompts = better results"
        )
        self.prompts_input.setMinimumHeight(80)
        self.prompts_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.prompts_input.verticalScrollBar().setSingleStep(6)
        prompts_layout.addWidget(self.prompts_input, 1)
        self.prompts_input.setPlaceholderText(
            "Paste your prompts here, one per line...\n\n"
            "Example:\n"
            "A serene mountain landscape at sunset with golden light\n"
            "A futuristic city skyline with neon lights and flying cars\n"
            "A cozy coffee shop interior with warm lighting\n\n"
            "Tips:\n"
            "- One prompt per line\n"
            "- No limit on number of prompts\n"
            "- Detailed prompts = better results"
        )
        self.btn_add_to_queue = QPushButton("＋  Add to Queue")
        self.btn_add_to_queue.setProperty("role", "primaryGradient")
        self.btn_add_to_queue.setText("+  Add to Queue")
        self.btn_add_to_queue.setFixedHeight(34)
        self.btn_add_to_queue.clicked.connect(self.add_prompts_to_queue)
        prompts_layout.addWidget(self.btn_add_to_queue)
        self._apply_card_shadow(self.prompts_group, blur=24, y_offset=8)
        self.prompts_input_widget = self.prompts_group

        queue_group = QGroupBox("Task Queue")
        queue_group.setObjectName("dashboardPanel")
        queue_layout = QVBoxLayout(queue_group)
        queue_layout.setContentsMargins(12, 12, 12, 12)
        queue_layout.setSpacing(8)
        queue_header_wrap = QHBoxLayout()
        queue_header_wrap.setContentsMargins(0, 0, 0, 0)
        queue_header_wrap.setSpacing(8)
        self.lbl_queue_title = QLabel("Task Queue")
        self.lbl_queue_title.setStyleSheet("color: #F8FAFC; font-size: 12px; font-weight: 700;")
        queue_header_wrap.addWidget(self.lbl_queue_title)
        queue_header_wrap.addStretch(1)
        queue_layout.addLayout(queue_header_wrap)
        self.queue_model = QueueTableModel(self)
        self.queue_table = QTableView()
        assert isinstance(self.queue_table, QTableView), "MUST use QTableView for large queues"
        self.queue_table.setModel(self.queue_model)
        self.queue_table.verticalHeader().setVisible(False)
        self.queue_table.setAlternatingRowColors(False)
        self.queue_table.verticalHeader().setDefaultSectionSize(42)
        self.queue_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.queue_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.queue_table.setShowGrid(True)
        queue_header = self.queue_table.horizontalHeader()
        queue_header.setMinimumSectionSize(40)
        queue_header.setStretchLastSection(False)
        queue_header.setSectionResizeMode(0, QHeaderView.Fixed)
        queue_header.setSectionResizeMode(1, QHeaderView.Stretch)
        queue_header.setSectionResizeMode(2, QHeaderView.Fixed)
        queue_header.setSectionResizeMode(3, QHeaderView.Stretch)
        queue_header.setSectionResizeMode(4, QHeaderView.Stretch)
        queue_header.setSectionResizeMode(5, QHeaderView.Fixed)
        queue_header.setSectionResizeMode(6, QHeaderView.Fixed)
        self.queue_table.setColumnWidth(0, 50)
        self.queue_table.setColumnWidth(2, 60)
        self.queue_table.setColumnWidth(5, 80)
        self.queue_table.setColumnWidth(6, 60)
        self.queue_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.queue_table.customContextMenuRequested.connect(self.show_queue_context_menu)
        self._configure_table_scrolling(self.queue_table)
        self.queue_table.setStyleSheet(
            """
            QTableView {
                background: #1E293B;
                color: #F8FAFC;
                border: 1px solid #334155;
                border-radius: 8px;
                gridline-color: #334155;
                selection-background-color: #2a3a5c;
                selection-color: #F8FAFC;
            }
            QTableView::item {
                padding: 6px 8px;
                color: #F8FAFC;
                background: #1E293B;
            }
            QTableView::item:alternate {
                background: #253145;
                color: #F8FAFC;
            }
            QTableView::item:selected {
                background: #2a3a5c;
                color: #FFFFFF;
            }
            QHeaderView::section {
                background: #0F172A;
                color: #94A3B8;
                font-weight: 600;
                padding: 8px;
                border: none;
                border-bottom: 1px solid #334155;
            }
            """
        )
        queue_layout.addWidget(self.queue_table)
        self._apply_card_shadow(queue_group, blur=24, y_offset=8)
        self.task_queue_widget = queue_group

        self.content_splitter = QSplitter(Qt.Horizontal)
        self.content_splitter.setChildrenCollapsible(False)
        self.content_splitter.setHandleWidth(8)
        self.content_splitter.addWidget(self.prompts_group)
        self.content_splitter.addWidget(queue_group)
        self.content_splitter.setStretchFactor(0, 35)
        self.content_splitter.setStretchFactor(1, 65)
        self.content_splitter.setSizes([380, 760])

        self.dashboard_body_splitter = QSplitter(Qt.Vertical)
        self.dashboard_body_splitter.setChildrenCollapsible(False)
        self.dashboard_body_splitter.setHandleWidth(6)
        self.dashboard_body_splitter.addWidget(self.mode_tabs)
        self.dashboard_body_splitter.addWidget(self.content_splitter)
        self.dashboard_body_splitter.setStretchFactor(0, 40)
        self.dashboard_body_splitter.setStretchFactor(1, 60)
        self.dashboard_body_splitter.setSizes([260, 520])
        self.dashboard_body_splitter.setStyleSheet(
            "QSplitter::handle:vertical { background: #334155; height: 3px; } "
            "QSplitter::handle:vertical:hover { background: #60A5FA; }"
        )
        layout.addWidget(self.dashboard_body_splitter, 1)

        self.logs_widget = QGroupBox("Live Logs")
        self.logs_widget.setTitle("")
        self.logs_widget.setObjectName("dashboardPanel")
        logs_layout = QVBoxLayout(self.logs_widget)
        logs_header = QHBoxLayout()
        self.lbl_logs_title = QLabel("Live Logs")
        self.lbl_logs_title.setStyleSheet("color: white; font-size: 14px; font-weight: 700;")
        logs_header.addWidget(self.lbl_logs_title)
        logs_header.addStretch()
        self.btn_clear_logs = QPushButton("Clear")
        self.btn_clear_logs.setFixedHeight(26)
        self.btn_clear_logs.setFixedWidth(60)
        self.btn_clear_logs.setStyleSheet(
            "QPushButton { background-color: #334155; color: #94A3B8; font-size: 11px; "
            "border: 1px solid #475569; border-radius: 3px; } "
            "QPushButton:hover { background-color: #475569; }"
        )
        self.btn_clear_logs.clicked.connect(self._clear_logs)
        logs_header.addWidget(self.btn_clear_logs)
        logs_layout.addLayout(logs_header)
        self.logs_output = QTextEdit()
        self.logs_output.setObjectName("logsOutput")
        self.logs_output.setReadOnly(True)
        self.logs_output.setUndoRedoEnabled(False)
        self.logs_output.setMinimumHeight(150)
        self.logs_output.document().setMaximumBlockCount(5000)
        self.logs_output.verticalScrollBar().setSingleStep(6)
        logs_layout.addWidget(self.logs_output)
        self.log_buffer = LogBuffer(self.logs_output, self, interval_ms=180)
        self._apply_card_shadow(self.logs_widget, blur=24, y_offset=8)

        self.btn_start = QPushButton("▶  Start Automation")
        self.btn_start.setText("Start Automation")
        self.btn_start.setFixedHeight(34)
        self.btn_start.setMinimumWidth(110)
        self.btn_start.setStyleSheet(
            "QPushButton { background-color: #2563EB; color: white; font-size: 13px; font-weight: 700; "
            "border: none; border-radius: 7px; padding: 0 18px; } "
            "QPushButton:hover { background-color: #3B82F6; } "
            "QPushButton:disabled { background-color: #1E3A5F; color: #64748B; }"
        )
        self.btn_start.clicked.connect(self.start_queue_manager)

        self.btn_pause = QPushButton("⏸  Pause")
        self.btn_pause.setText("Pause")
        self.btn_pause.setFixedHeight(30)
        self.btn_pause.setFixedWidth(84)
        self.btn_pause.setStyleSheet(
            "QPushButton { background-color: #D97706; color: white; font-size: 12px; font-weight: 600; "
            "border: none; border-radius: 6px; } "
            "QPushButton:hover { background-color: #F59E0B; } "
            "QPushButton:disabled { background-color: #4A3A1A; color: #64748B; }"
        )
        self.btn_pause.clicked.connect(self.pause_queue_manager)

        self.btn_resume = QPushButton("▶  Resume")
        self.btn_resume.setText("Resume")
        self.btn_resume.setFixedHeight(30)
        self.btn_resume.setFixedWidth(84)
        self.btn_resume.setStyleSheet(
            "QPushButton { background-color: #1D4ED8; color: white; font-size: 12px; font-weight: 600; "
            "border: none; border-radius: 6px; } "
            "QPushButton:hover { background-color: #3B82F6; } "
            "QPushButton:disabled { background-color: #1E3A5F; color: #64748B; }"
        )
        self.btn_resume.clicked.connect(self.resume_queue_manager)

        self.btn_stop = QPushButton("⏹  Stop")
        self.btn_stop.setText("Stop")
        self.btn_stop.setFixedHeight(30)
        self.btn_stop.setFixedWidth(84)
        self.btn_stop.setStyleSheet(
            "QPushButton { background-color: #DC2626; color: white; font-size: 12px; font-weight: 600; "
            "border: none; border-radius: 6px; } "
            "QPushButton:hover { background-color: #EF4444; } "
            "QPushButton:disabled { background-color: #4A1A1A; color: #64748B; }"
        )
        self.btn_stop.clicked.connect(self.stop_queue_manager)

        self.btn_clear_queue = QPushButton("Clear Queue")
        self.btn_clear_queue.setFixedHeight(26)
        self.btn_clear_queue.setStyleSheet(
            "QPushButton { background-color: #334155; color: #94A3B8; font-size: 11px; "
            "border: 1px solid #475569; border-radius: 4px; padding: 0 10px; } "
            "QPushButton:hover { background-color: #475569; color: white; }"
        )
        self.btn_clear_queue.clicked.connect(self.clear_queue)

        self.btn_clear_done = QPushButton("Clear Done")
        self.btn_clear_done.setFixedHeight(26)
        self.btn_clear_done.setStyleSheet(self.btn_clear_queue.styleSheet())
        self.btn_clear_done.clicked.connect(self.clear_completed_jobs_from_queue)

        self.toolbar_actions_layout.addWidget(self.btn_start)
        self.toolbar_actions_layout.addWidget(self.btn_pause)
        self.toolbar_actions_layout.addWidget(self.btn_resume)
        self.toolbar_actions_layout.addWidget(self.btn_stop)
        queue_header_wrap.addWidget(self.btn_clear_queue)
        queue_header_wrap.addWidget(self.btn_clear_done)

        self.main_splitter = QSplitter(Qt.Vertical)
        self.main_splitter.addWidget(self.dashboard_content)
        self.main_splitter.addWidget(self.logs_widget)
        self.main_splitter.setSizes([720, 280])
        self.main_splitter.setCollapsible(0, False)
        self.main_splitter.setCollapsible(1, True)
        self.main_splitter.setHandleWidth(6)
        self.main_splitter.setStyleSheet(
            "QSplitter::handle:vertical { background-color: #334155; height: 4px; margin: 2px 0; } "
            "QSplitter::handle:vertical:hover { background-color: #60A5FA; }"
        )
        root_layout.addWidget(self.main_splitter, 1)

        self._set_queue_controls_state("stopped")
        self._sync_generation_mode_ui()
        self._refresh_bulk_pairing_preview("ingredients")
        self._refresh_bulk_pairing_preview("frames_start")
        self.load_queue_table()
        QTimer.singleShot(0, self._scroll_active_mode_tab_to_top)

    def setup_live_generation(self):
        layout = QVBoxLayout(self.tab_live_generation)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(12)

        stats_bar = QHBoxLayout()
        stats_bar.setSpacing(10)
        self.live_stat_total = self._create_live_stat("0", "Total", "#94A3B8")
        self.live_stat_running = self._create_live_stat("0", "Running", "#3B82F6")
        self.live_stat_done = self._create_live_stat("0", "Done", "#22C55E")
        self.live_stat_failed = self._create_live_stat("0", "Failed", "#EF4444")
        self.live_stat_pending = self._create_live_stat("0", "Pending", "#F59E0B")
        for card in (
            self.live_stat_total,
            self.live_stat_running,
            self.live_stat_done,
            self.live_stat_failed,
            self.live_stat_pending,
        ):
            stats_bar.addWidget(card, 1)
        layout.addLayout(stats_bar)

        progress_wrap = QFrame()
        progress_wrap.setObjectName("dashboardPanel")
        progress_layout = QVBoxLayout(progress_wrap)
        progress_layout.setContentsMargins(14, 14, 14, 14)
        progress_layout.setSpacing(8)
        progress_title = QLabel("Overall Progress")
        progress_title.setObjectName("tabSectionTitle")
        progress_layout.addWidget(progress_title)
        self.live_progress_bar = QProgressBar()
        self.live_progress_bar.setRange(0, 100)
        self.live_progress_bar.setValue(0)
        self.live_progress_bar.setTextVisible(True)
        self.live_progress_bar.setFormat("No jobs")
        self.live_progress_bar.setFixedHeight(28)
        self.live_progress_bar.setObjectName("liveOverallProgress")
        progress_layout.addWidget(self.live_progress_bar)
        layout.addWidget(progress_wrap)
        self._apply_card_shadow(progress_wrap, blur=24, y_offset=8)

        grid_wrap = QFrame()
        grid_wrap.setObjectName("dashboardPanel")
        grid_layout = QVBoxLayout(grid_wrap)
        grid_layout.setContentsMargins(14, 14, 14, 14)
        grid_layout.setSpacing(10)

        grid_title = QLabel("Active Grid")
        grid_title.setObjectName("tabSectionTitle")
        grid_layout.addWidget(grid_title)

        self.grid_scroll = QScrollArea()
        self.grid_scroll.setWidgetResizable(True)
        self.grid_scroll.setFrameShape(QFrame.NoFrame)
        self.grid_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.grid_scroll.setObjectName("liveGridScroll")
        self.grid_scroll.verticalScrollBar().setSingleStep(6)
        self.grid_scroll.verticalScrollBar().valueChanged.connect(self._on_grid_scroll)
        self.live_grid_widget = VirtualLiveGridWidget()
        self.live_grid_widget.setObjectName("liveGridCanvas")
        self.grid_scroll.setWidget(self.live_grid_widget)
        grid_layout.addWidget(self.grid_scroll, 1)

        bottom_bar = QHBoxLayout()
        bottom_bar.setSpacing(10)
        bottom_bar.addWidget(QLabel("Grid Size:"))
        self.cmb_grid_size = self._create_setting_combo(
            [("Small", "small"), ("Medium", "medium"), ("Large", "large")],
            current_data="medium",
            trigger_sync=False,
        )
        self.cmb_grid_size.currentIndexChanged.connect(self._refresh_live_grid)
        bottom_bar.addWidget(self.cmb_grid_size)
        bottom_bar.addStretch()

        self.btn_open_outputs = QPushButton("📂 Open Outputs Folder")
        self.btn_open_outputs.setProperty("role", "browse")
        self.btn_open_outputs.clicked.connect(self._open_outputs_folder)
        bottom_bar.addWidget(self.btn_open_outputs)
        grid_layout.addLayout(bottom_bar)

        layout.addWidget(grid_wrap, 1)
        self._apply_card_shadow(grid_wrap, blur=24, y_offset=8)

        self._live_grid_size_key = str(self.cmb_grid_size.currentData() or "medium")
        self._refresh_live_grid()

    def _create_live_stat(self, count_text, label_text, color):
        card = QFrame()
        card.setObjectName("statCard")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(14, 12, 14, 12)
        card_layout.setSpacing(4)

        count_label = QLabel(str(count_text))
        count_label.setStyleSheet(
            f"color: {color}; font-size: 26px; font-weight: 800; background: transparent; border: none;"
        )
        label = QLabel(label_text)
        label.setStyleSheet(
            "color: #64748B; font-size: 11px; font-weight: 700; letter-spacing: 1px; background: transparent; border: none;"
        )
        card_layout.addWidget(count_label)
        card_layout.addWidget(label)
        card._count_label = count_label
        self._apply_card_shadow(card, blur=18, y_offset=6)
        return card

    def _default_outputs_dir(self):
        return str(get_outputs_dir())

    def _outputs_dir(self):
        return str(get_output_directory())

    def _browse_output_directory(self):
        current_dir = self._outputs_dir()
        selected_dir = QFileDialog.getExistingDirectory(
            self,
            "Select Outputs Folder",
            current_dir,
        )
        if not selected_dir:
            return
        normalized = os.path.abspath(os.path.expanduser(str(selected_dir)))
        self.output_dir_input.setText(normalized)

    def _reset_output_directory(self):
        self.output_dir_input.setText(self._default_outputs_dir())

    def _open_outputs_folder(self):
        outputs_dir = self._outputs_dir()
        os.makedirs(outputs_dir, exist_ok=True)
        try:
            if sys.platform == "darwin":
                subprocess.Popen(["open", outputs_dir])
            elif sys.platform.startswith("win"):
                subprocess.Popen(["explorer", outputs_dir])
            else:
                subprocess.Popen(["xdg-open", outputs_dir])
        except Exception as exc:
            QMessageBox.warning(self, "Open Outputs Failed", f"Could not open outputs folder:\n{exc}")

    def _refresh_live_grid(self):
        if not hasattr(self, "live_grid_widget"):
            return
        self._live_grid_size_key = str(self.cmb_grid_size.currentData() or "medium") if hasattr(self, "cmb_grid_size") else "medium"
        self.live_grid_widget.set_card_size(self._live_grid_size_key)
        self._apply_live_jobs(self._latest_queue_jobs)

    def _update_live_stats(self, jobs):
        if not hasattr(self, "live_progress_bar"):
            return

        total = len(jobs)
        running = sum(1 for job in jobs if str(job.get("status") or "").strip().lower() == "running")
        done = sum(1 for job in jobs if str(job.get("status") or "").strip().lower() == "completed")
        failed = sum(1 for job in jobs if str(job.get("status") or "").strip().lower() == "failed")
        pending = sum(1 for job in jobs if str(job.get("status") or "").strip().lower() == "pending")

        self.live_stat_total._count_label.setText(str(total))
        self.live_stat_running._count_label.setText(str(running))
        self.live_stat_done._count_label.setText(str(done))
        self.live_stat_failed._count_label.setText(str(failed))
        self.live_stat_pending._count_label.setText(str(pending))

        if total <= 0:
            self.live_progress_bar.setMaximum(100)
            self.live_progress_bar.setValue(0)
            self.live_progress_bar.setFormat("No jobs")
            return

        settled = done + failed
        percent = int((settled / total) * 100)
        self.live_progress_bar.setMaximum(total)
        self.live_progress_bar.setValue(settled)
        self.live_progress_bar.setFormat(f"{percent}% ({settled}/{total} complete)")

    def _apply_live_jobs(self, jobs):
        jobs = list(jobs or [])
        if not hasattr(self, "live_grid_widget"):
            return
        self._latest_queue_jobs = jobs
        if hasattr(self, "grid_scroll") and self.grid_scroll is not None:
            self.live_grid_widget.resize(max(320, self.grid_scroll.viewport().width()), self.live_grid_widget.height())
        self.live_grid_widget.set_jobs(jobs)
        self._update_live_stats(jobs)

    def _schedule_live_refresh(self):
        if not hasattr(self, "tab_live_generation"):
            return
        if self.tabs.currentWidget() is not self.tab_live_generation:
            return
        if self._grid_scrolling:
            return
        jobs = list(self._pending_live_jobs if self._pending_live_jobs is not None else self._latest_queue_jobs)
        self._pending_live_jobs = None
        self.ui_throttler.schedule("live_grid", lambda jobs=jobs: self._apply_live_jobs(jobs))

    def _on_grid_scroll(self, _value):
        self._grid_scrolling = True
        self._grid_scroll_resume_timer.start(500)

    def _on_grid_scroll_idle(self):
        self._grid_scrolling = False
        if self._pending_live_jobs is not None and self.tabs.currentWidget() is self.tab_live_generation:
            self.ui_throttler.schedule(
                "live_grid",
                lambda jobs=list(self._pending_live_jobs or []): self._apply_live_jobs(jobs),
            )
            self._pending_live_jobs = None

    def _clear_logs(self):
        if hasattr(self, "log_buffer"):
            self.log_buffer.clear()
            return
        if hasattr(self, "logs_output"):
            self.logs_output.clear()

    def _show_session_warning(self, message):
        if not hasattr(self, "warning_banner") or not hasattr(self, "warning_container"):
            return
        text = str(message or "").strip()
        if not text:
            self.warning_banner.clear()
            self.warning_container.setVisible(False)
            return
        self.warning_banner.setText(f"⚠ {text}")
        self.warning_container.setVisible(True)

    def _create_setting_combo(self, items, current_data=None, *, trigger_sync=True, min_height=38):
        combo = QComboBox()
        combo.setObjectName("settingInput")
        combo.setMinimumHeight(min_height)
        for item in items:
            if isinstance(item, tuple):
                combo.addItem(item[0], item[1])
            else:
                combo.addItem(str(item), item)
        if current_data is not None:
            idx = combo.findData(current_data)
            if idx < 0 and isinstance(current_data, str):
                idx = combo.findText(current_data)
            if idx >= 0:
                combo.setCurrentIndex(idx)
        if trigger_sync:
            combo.currentIndexChanged.connect(lambda _=None: self._on_generation_settings_changed())
        return combo

    def _create_parallel_combo(self, saved_slots):
        combo = self._create_setting_combo([(str(i), i) for i in range(1, 11)], current_data=max(1, min(10, saved_slots)), trigger_sync=False)
        combo.currentIndexChanged.connect(lambda _=None: self._update_runtime_badges())
        return combo

    def _make_setting_label(self, text):
        label = QLabel(text)
        label.setObjectName("settingLabel")
        label.setMinimumWidth(86)
        return label

    def _make_inline_row(self, *widgets):
        row = QWidget()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        for widget in widgets:
            if widget is None:
                continue
            stretch = 1 if isinstance(widget, (QComboBox, QLineEdit, QTextEdit)) else 0
            layout.addWidget(widget, stretch)
        layout.addStretch()
        return row

    def _apply_card_shadow(self, widget, *, blur=28, y_offset=10, color=QColor(15, 23, 42, 90)):
        effect = QGraphicsDropShadowEffect(widget)
        effect.setBlurRadius(blur)
        effect.setOffset(0, y_offset)
        effect.setColor(color)
        widget.setGraphicsEffect(effect)

    def _configure_table_scrolling(self, table):
        table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        table.verticalScrollBar().setSingleStep(6)
        table.horizontalScrollBar().setSingleStep(6)
        table.setMouseTracking(False)

    def _start_background_task(self, fn, *args, on_finished=None, on_error=None, **kwargs):
        task = BackgroundTask(fn, *args, **kwargs)
        self._background_tasks.add(task)

        def _cleanup(*_args):
            self._background_tasks.discard(task)

        task.signals.finished.connect(_cleanup, Qt.QueuedConnection)
        task.signals.error.connect(_cleanup, Qt.QueuedConnection)
        if callable(on_finished):
            task.signals.finished.connect(on_finished, Qt.QueuedConnection)
        if callable(on_error):
            task.signals.error.connect(on_error, Qt.QueuedConnection)
        else:
            task.signals.error.connect(self._on_background_task_error, Qt.QueuedConnection)
        self.thread_pool.start(task)
        return task

    def _on_background_task_error(self, error_msg):
        message = str(error_msg or "").strip()
        if message:
            self.append_log(f"[ERROR] {message}")

    @staticmethod
    def _slim_jobs_for_ui(jobs):
        result = []
        for job in list(jobs or []):
            row = dict(job or {})
            row.pop("image_bytes", None)
            row.pop("binary", None)
            if str(row.get("status") or "").strip().lower() == "completed":
                row.pop("ref_paths", None)
                prompt = str(row.get("prompt") or "")
                if len(prompt) > 160:
                    row["prompt"] = prompt[:157] + "..."
            result.append(row)
        return result

    def _cached_pending_count(self):
        return sum(1 for job in self._latest_queue_jobs if str(job.get("status") or "").strip().lower() == "pending")

    def _queue_job_status_text(self, job):
        status_text = str(job.get("status") or "").strip().lower() or "pending"
        if status_text == "failed" and self._is_moderated_failed_error(job.get("error")):
            return "moderated"
        return status_text

    def _queue_job_progress_text(self, job):
        status_text = self._queue_job_status_text(job)
        if status_text == "completed":
            return "Done"
        if status_text in ("failed", "moderated"):
            return "Failed"
        if status_text == "running":
            progress_step = str(job.get("progress_step") or "").strip().lower()
            poll_count = max(0, int(job.get("progress_poll_count") or 0))
            if progress_step == "download":
                return "Downloading"
            if progress_step == "image":
                return "Rendering"
            if progress_step == "video":
                return f"Polling {poll_count}"
            return "Running"

        type_text = str(job.get("job_type") or "image").strip().lower()
        progress_value = job.get("video_output_count") if type_text in ("video", "pipeline") else job.get("output_count")
        try:
            return f"x{max(1, int(progress_value or 1))}"
        except Exception:
            return "--"

    def _build_queue_row_snapshot(self, job):
        type_text = str(job.get("job_type") or "image").strip().lower()
        if type_text == "pipeline":
            display_type = "Pipeline"
        elif type_text == "video":
            display_type = "Video"
        else:
            display_type = "Image"

        model_text = str(job.get("model") or "")
        if type_text == "pipeline":
            video_model = str(job.get("video_model") or "").strip()
            if video_model:
                model_text = f"{model_text} -> {video_model}"

        display_no = job.get("output_index") if job.get("is_retry") else job.get("queue_no")
        if display_no is None:
            display_no = str(job.get("id") or "")[:8]
        display_no_text = f"{display_no} (RETRY)" if job.get("is_retry") else str(display_no)

        return {
            "job_id": str(job.get("id") or ""),
            "queue_no": display_no_text,
            "prompt": str(job.get("prompt") or ""),
            "job_type_display": display_type,
            "model_display": model_text,
            "account": str(job.get("account") or ""),
            "status": self._queue_job_status_text(job),
            "progress": self._queue_job_progress_text(job),
            "is_retry": bool(job.get("is_retry")),
            "retry_source": str(job.get("retry_source") or ""),
        }

    def _flush_queue_table_updates(self, job_ids):
        if not hasattr(self, "queue_model") or self.queue_model.rowCount() <= 0:
            return
        pending_updates = {}
        for job_id in list(job_ids or []):
            row = self._queue_row_map.get(str(job_id))
            snapshot = self._queue_row_snapshots.get(str(job_id))
            if row is None or snapshot is None:
                continue
            pending_updates[row] = snapshot
        self.queue_model.bulk_update(pending_updates)

    def _schedule_failed_jobs_refresh(self):
        self._failed_jobs_dirty = True
        if hasattr(self, "failed_jobs_refresh_timer") and self.tabs.currentWidget() is self.tab_failed_jobs:
            self.failed_jobs_refresh_timer.start()

    def _poll_queue_snapshot(self):
        self._request_queue_snapshot()

    def _request_queue_snapshot(self):
        if not hasattr(self, "queue_model"):
            return
        if self._queue_snapshot_inflight:
            self._queue_snapshot_requested = True
            return
        self._queue_snapshot_inflight = True
        self._queue_snapshot_requested = False
        self._start_background_task(
            get_all_jobs,
            on_finished=self._on_queue_snapshot_loaded,
            on_error=self._on_queue_snapshot_failed,
        )

    def _on_queue_snapshot_failed(self, error_msg):
        self._queue_snapshot_inflight = False
        self._on_background_task_error(error_msg)

    def _on_queue_snapshot_loaded(self, jobs):
        self._queue_snapshot_inflight = False
        slim_jobs = self._slim_jobs_for_ui(jobs)
        if self._queue_snapshot_requested:
            self._queue_snapshot_requested = False
            self._request_queue_snapshot()
        self.ui_throttler.schedule("queue_snapshot", lambda jobs=slim_jobs: self._apply_queue_snapshot(jobs))

    def _apply_queue_snapshot(self, jobs):
        jobs = list(jobs or [])
        self._latest_queue_jobs = jobs
        job_order = [str(job.get("id") or "") for job in jobs]
        snapshots = {job_id: self._build_queue_row_snapshot(job) for job_id, job in zip(job_order, jobs)}

        if self.queue_model.rowCount() != len(jobs) or self._queue_job_order != job_order:
            self._queue_job_order = job_order
            self._queue_row_map = {job_id: idx for idx, job_id in enumerate(self._queue_job_order)}
            queue_rows = [snapshots[job_id] for job_id in job_order]
            self._queue_row_snapshots = dict(snapshots)
            self.queue_model.set_jobs(queue_rows)
        else:
            changed_rows = {}
            for job_id in job_order:
                snapshot = snapshots[job_id]
                if snapshot != self._queue_row_snapshots.get(job_id):
                    row = self._queue_row_map.get(job_id)
                    if row is not None:
                        changed_rows[row] = snapshot
            self._queue_row_snapshots = dict(snapshots)
            if changed_rows:
                self.queue_model.bulk_update(changed_rows)

        self._refresh_dashboard_stats(jobs)
        if self.tabs.currentWidget() is self.tab_live_generation:
            if self._grid_scrolling:
                self._pending_live_jobs = jobs
            else:
                self._apply_live_jobs(jobs)
        else:
            self._live_tab_dirty = True

    def _request_failed_jobs_refresh(self, force=False):
        self._failed_jobs_dirty = True
        if not force and self.tabs.currentWidget() is not self.tab_failed_jobs:
            return
        if self._failed_jobs_inflight:
            return
        self._failed_jobs_inflight = True
        self._start_background_task(
            get_failed_jobs,
            on_finished=self._on_failed_jobs_loaded,
            on_error=self._on_failed_jobs_failed,
        )

    def _on_failed_jobs_failed(self, error_msg):
        self._failed_jobs_inflight = False
        self._on_background_task_error(error_msg)

    def _on_failed_jobs_loaded(self, jobs):
        self._failed_jobs_inflight = False
        self._latest_failed_jobs = list(jobs or [])
        if self.tabs.currentWidget() is self.tab_failed_jobs:
            self.ui_throttler.schedule(
                "failed_jobs",
                lambda jobs=list(self._latest_failed_jobs): self._populate_failed_jobs_table(jobs),
            )

    def _start_bulk_queue_add(self, job_specs, *, success_logs=None, after_success=None, progress_title="Adding prompts to queue..."):
        if self.bulk_queue_add_worker and self.bulk_queue_add_worker.isRunning():
            QMessageBox.information(self, "Queue Add In Progress", "Please wait for the current bulk add to finish.")
            return

        specs = list(job_specs or [])
        if not specs:
            return

        self._bulk_add_success_logs = list(success_logs or [])
        self._bulk_add_after_success = after_success
        self.bulk_queue_add_worker = BulkQueueAddWorker(specs, self)
        self.bulk_queue_add_worker.progress.connect(self._on_bulk_queue_add_progress)
        self.bulk_queue_add_worker.completed.connect(self._on_bulk_queue_add_completed)
        self.bulk_queue_add_worker.failed.connect(self._on_bulk_queue_add_failed)

        if len(specs) >= 50:
            self.bulk_add_progress_dialog = QProgressDialog(progress_title, "", 0, len(specs), self)
            self.bulk_add_progress_dialog.setCancelButton(None)
            self.bulk_add_progress_dialog.setWindowModality(Qt.WindowModal)
            self.bulk_add_progress_dialog.setMinimumDuration(0)
            self.bulk_add_progress_dialog.setAutoClose(False)
            self.bulk_add_progress_dialog.setAutoReset(False)
            self.bulk_add_progress_dialog.setValue(0)
            self.bulk_add_progress_dialog.show()

        if hasattr(self, "btn_add_to_queue"):
            self.btn_add_to_queue.setEnabled(False)
        self.bulk_queue_add_worker.start()

    def _on_bulk_queue_add_progress(self, done, total):
        if self.bulk_add_progress_dialog is not None:
            self.bulk_add_progress_dialog.setMaximum(max(1, int(total or 1)))
            self.bulk_add_progress_dialog.setValue(int(done or 0))

    def _on_bulk_queue_add_completed(self, inserted_count):
        if self.bulk_add_progress_dialog is not None:
            self.bulk_add_progress_dialog.setValue(self.bulk_add_progress_dialog.maximum())
            self.bulk_add_progress_dialog.close()
            self.bulk_add_progress_dialog.deleteLater()
            self.bulk_add_progress_dialog = None

        if hasattr(self, "btn_add_to_queue"):
            self.btn_add_to_queue.setEnabled(True)

        after_success = self._bulk_add_after_success
        self._bulk_add_after_success = None
        if callable(after_success):
            after_success()

        for msg in self._bulk_add_success_logs:
            self.append_log(msg)
        self._bulk_add_success_logs = []

        self.load_queue_table()

        if self.bulk_queue_add_worker is not None:
            self.bulk_queue_add_worker.deleteLater()
            self.bulk_queue_add_worker = None

    def _on_bulk_queue_add_failed(self, error_msg):
        if self.bulk_add_progress_dialog is not None:
            self.bulk_add_progress_dialog.close()
            self.bulk_add_progress_dialog.deleteLater()
            self.bulk_add_progress_dialog = None
        if hasattr(self, "btn_add_to_queue"):
            self.btn_add_to_queue.setEnabled(True)
        QMessageBox.critical(self, "Queue Add Failed", str(error_msg or "Could not add prompts to queue."))
        self._bulk_add_success_logs = []
        self._bulk_add_after_success = None
        if self.bulk_queue_add_worker is not None:
            self.bulk_queue_add_worker.deleteLater()
            self.bulk_queue_add_worker = None

    def _make_status_badge(self, text, status_text):
        status = str(status_text or "").strip().lower()
        styles = {
            "pending": ("#94A3B8", "#1D2535", "#475569"),
            "running": ("#3B82F6", "#1A2744", "#3B82F6"),
            "completed": ("#22C55E", "#132432", "#22C55E"),
            "failed": ("#EF4444", "#1F1A2A", "#EF4444"),
            "moderated": ("#F59E0B", "#1C1E2A", "#F59E0B"),
        }
        fg, bg, border = styles.get(status, ("#94A3B8", "#1D2535", "#475569"))

        wrap = QWidget()
        layout = QHBoxLayout(wrap)
        layout.setContentsMargins(6, 2, 6, 2)
        layout.setSpacing(0)

        badge = QLabel(str(text or "").upper())
        badge.setAlignment(Qt.AlignCenter)
        badge.setStyleSheet(
            f"color: {fg}; background: {bg}; border: 1px solid {border}; "
            "border-radius: 6px; padding: 3px 8px; font-size: 11px; font-weight: 700;"
        )
        layout.addWidget(badge, alignment=Qt.AlignCenter)
        return wrap

    def _create_tab_scroll_content(self, tab_widget, max_h):
        outer_layout = QVBoxLayout(tab_widget)
        outer_layout.setContentsMargins(0, 0, 0, 0)
        outer_layout.setSpacing(0)
        tab_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        tab_widget.setMinimumHeight(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        scroll.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        scroll.verticalScrollBar().setSingleStep(6)
        scroll.setProperty("preferredMaxHeight", int(max_h))
        scroll.setAlignment(Qt.AlignTop | Qt.AlignLeft)

        viewport = QWidget()
        viewport.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        viewport_layout = QVBoxLayout(viewport)
        viewport_layout.setContentsMargins(0, 0, 0, 0)
        viewport_layout.setSpacing(0)

        content = QWidget()
        content.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        layout = QVBoxLayout(content)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)
        layout.setAlignment(Qt.AlignTop)
        viewport_layout.addWidget(content, 0, Qt.AlignTop)
        viewport_layout.addStretch(1)

        scroll.setWidget(viewport)
        outer_layout.addWidget(scroll, 1)
        self._mode_tab_scrolls[tab_widget] = scroll
        return layout, scroll

    def _create_tab_section_title(self, text):
        label = QLabel(text)
        label.setObjectName("tabSectionTitle")
        return label

    def _create_separator(self):
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setObjectName("tabSeparator")
        line.setFixedHeight(1)
        return line

    def _toggle_bulk_section(self, mode_key, checked):
        panel = self._bulk_panel(mode_key)
        if not panel:
            return
        panel["content"].setVisible(bool(checked))
        panel["toggle"].setText("📦 Bulk Image Matching  ▼" if checked else "📦 Bulk Image Matching  ▶")

    def _toggle_pipeline_bulk_section(self, checked):
        is_open = bool(checked)
        if hasattr(self, "pipe_bulk_content"):
            self.pipe_bulk_content.setVisible(is_open)
        if hasattr(self, "pipe_bulk_toggle"):
            self.pipe_bulk_toggle.setText("📦 Bulk Pipeline Prompts  ▼" if is_open else "📦 Bulk Pipeline Prompts  ▶")

    def _update_pipeline_count(self):
        if not hasattr(self, "pipe_lbl_count"):
            return

        img_lines = []
        vid_lines = []
        if hasattr(self, "pipe_txt_img_prompts"):
            img_lines = [line for line in self.pipe_txt_img_prompts.toPlainText().strip().split("\n") if line.strip()]
        if hasattr(self, "pipe_txt_vid_prompts"):
            vid_lines = [line for line in self.pipe_txt_vid_prompts.toPlainText().strip().split("\n") if line.strip()]

        img_count = len(img_lines)
        vid_count = len(vid_lines)

        if img_count == 0:
            self.pipe_lbl_count.setText("")
            self.pipe_lbl_count.setStyleSheet("color: #64748B; font-size: 12px; padding: 4px 0;")
        elif vid_count == 0:
            self.pipe_lbl_count.setText(f"Image Prompts: {img_count}  |  Video Prompts: 0 (all will use 'animate')")
            self.pipe_lbl_count.setStyleSheet("color: #F59E0B; font-size: 12px; padding: 4px 0;")
        elif img_count == vid_count:
            self.pipe_lbl_count.setText(f"Image Prompts: {img_count}  |  Video Prompts: {vid_count}  Matched")
            self.pipe_lbl_count.setStyleSheet("color: #22C55E; font-size: 12px; font-weight: 600; padding: 4px 0;")
        elif vid_count < img_count:
            diff = img_count - vid_count
            self.pipe_lbl_count.setText(
                f"Image Prompts: {img_count}  |  Video Prompts: {vid_count}  {diff} video prompt(s) missing (will use 'animate')"
            )
            self.pipe_lbl_count.setStyleSheet("color: #F59E0B; font-size: 12px; padding: 4px 0;")
        else:
            diff = vid_count - img_count
            self.pipe_lbl_count.setText(
                f"Image Prompts: {img_count}  |  Video Prompts: {vid_count}  {diff} extra video prompt(s) ignored"
            )
            self.pipe_lbl_count.setStyleSheet("color: #F59E0B; font-size: 12px; padding: 4px 0;")

    def _update_pipeline_video_quality_options(self, mode):
        if not hasattr(self, "pipe_cmb_vid_quality"):
            return
        normalized_mode = str(mode or "ingredients").strip().lower() or "ingredients"
        current_value = str(self.pipe_cmb_vid_quality.currentData() or "").strip()
        self.pipe_cmb_vid_quality.blockSignals(True)
        self.pipe_cmb_vid_quality.clear()

        items = [
            ("Veo 3.1 - Fast", "Veo 3.1 - Fast"),
            ("Veo 3.1 - Lite", "Veo 3.1 - Lite"),
            ("Veo 3.1 - Fast [Lower Pri]", "Veo 3.1 - Fast [Lower Pri]"),
        ]
        if normalized_mode == "frames_start":
            items.append(("Veo 3.1 - Quality", "Veo 3.1 - Quality"))

        for label, value in items:
            self.pipe_cmb_vid_quality.addItem(label, value)

        restored_idx = self.pipe_cmb_vid_quality.findData(current_value)
        if restored_idx < 0:
            restored_idx = 0
        self.pipe_cmb_vid_quality.setCurrentIndex(restored_idx)
        self.pipe_cmb_vid_quality.blockSignals(False)

    def _on_pipeline_video_mode_changed(self, _index):
        mode = str(self.pipe_cmb_vid_mode.currentData() or "ingredients")
        self._update_pipeline_video_quality_options(mode)
        self._on_generation_settings_changed()

    def _create_path_row(self, button_text, browse_handler, clear_handler, clear_label="Clear"):
        row = QFrame()
        row.setObjectName("referenceRow")
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(8, 8, 8, 8)
        row_layout.setSpacing(10)
        browse_btn = QPushButton(button_text)
        browse_btn.setProperty("role", "browse")
        browse_btn.setMinimumHeight(38)
        browse_btn.clicked.connect(browse_handler)
        label = QLabel("None")
        label.setObjectName("refStatusLabel")
        clear_btn = QPushButton(clear_label)
        clear_btn.setObjectName("refClearButton")
        clear_btn.setProperty("role", "danger")
        clear_btn.setMinimumHeight(34)
        clear_btn.clicked.connect(clear_handler)
        clear_btn.setVisible(False)
        row_layout.addWidget(browse_btn)
        row_layout.addWidget(label, 1)
        row_layout.addWidget(clear_btn)
        return row, browse_btn, label, clear_btn

    def _setup_image_mode_tab(self, saved_slots):
        layout, self.img_tab_scroll = self._create_tab_scroll_content(self.mode_tab_image, 230)
        layout.addWidget(self._create_tab_section_title("Image Settings"))

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
        form.setFormAlignment(Qt.AlignTop)
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.img_cmb_model = self._create_setting_combo([
            ("Imagen 4", "Imagen 4"),
            ("Nano Banana Pro", "Nano Banana Pro"),
            ("Nano Banana 2", "Nano Banana 2"),
        ], current_data="Imagen 4")
        self.img_cmb_ratio = self._create_setting_combo([
            ("Landscape (16:9)", "Landscape (16:9)"),
            ("Standard (4:3)", "Standard (4:3)"),
            ("Square (1:1)", "Square (1:1)"),
            ("Portrait (3:4)", "Portrait (3:4)"),
            ("Tall Portrait (9:16)", "Tall Portrait (9:16)"),
        ], current_data="Landscape (16:9)")
        self.img_cmb_outputs = self._create_setting_combo([("x1", 1), ("x2", 2), ("x3", 3), ("x4", 4)], current_data=1)
        self.img_cmb_parallel = self._create_parallel_combo(saved_slots)

        form.addRow(self._make_setting_label("Model:"), self.img_cmb_model)
        form.addRow(
            self._make_setting_label("Ratio:"),
            self._make_inline_row(
                self.img_cmb_ratio,
                self._make_setting_label("Outputs:"),
                self.img_cmb_outputs,
            ),
        )
        form.addRow(self._make_setting_label("Parallel:"), self.img_cmb_parallel)
        layout.addLayout(form)

        refs_header = QHBoxLayout()
        self.img_btn_add_refs = QPushButton("+ Add Reference Image(s)")
        self.img_btn_add_refs.setProperty("role", "browse")
        self.img_btn_add_refs.clicked.connect(self.select_reference_image)
        self.lbl_ref_status = QLabel("None")
        self.lbl_ref_status.setObjectName("settingHint")
        self.btn_clear_ref = QPushButton("Clear All")
        self.btn_clear_ref.setObjectName("refClearButton")
        self.btn_clear_ref.setProperty("role", "danger")
        self.btn_clear_ref.clicked.connect(self.clear_reference_image)
        self.btn_clear_ref.setVisible(False)
        refs_header.addWidget(self.img_btn_add_refs)
        refs_header.addWidget(self.lbl_ref_status, 1)
        refs_header.addWidget(self.btn_clear_ref)
        refs_widget = QWidget()
        refs_widget.setLayout(refs_header)
        form.addRow(self._make_setting_label("Reference:"), refs_widget)

        self.ref_items_container = QWidget()
        self.ref_items_layout = QVBoxLayout(self.ref_items_container)
        self.ref_items_layout.setContentsMargins(0, 0, 0, 0)
        self.ref_items_layout.setSpacing(6)
        self.ref_items_container.setVisible(False)
        layout.addWidget(self.ref_items_container)
        layout.addStretch()

    def _setup_video_t2v_tab(self, saved_slots):
        layout, self.t2v_tab_scroll = self._create_tab_scroll_content(self.mode_tab_t2v, 230)
        layout.addWidget(self._create_tab_section_title("Text-to-Video Settings"))

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
        form.setFormAlignment(Qt.AlignTop)
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.t2v_cmb_quality = self._create_setting_combo([
            ("Veo 3.1 - Fast", "Veo 3.1 - Fast"),
            ("Veo 3.1 - Lite", "Veo 3.1 - Lite"),
            ("Veo 3.1 - Fast [Lower Pri]", "Veo 3.1 - Fast [Lower Pri]"),
            ("Veo 3.1 - Quality", "Veo 3.1 - Quality"),
        ], current_data="Veo 3.1 - Fast")
        self.t2v_cmb_ratio = self._create_setting_combo([
            ("Landscape (16:9)", "Landscape (16:9)"),
            ("Square (1:1)", "Square (1:1)"),
            ("Portrait (9:16)", "Portrait (9:16)"),
        ], current_data="Landscape (16:9)")
        self.t2v_cmb_outputs = self._create_setting_combo([("x1", 1), ("x2", 2), ("x3", 3), ("x4", 4)], current_data=1)
        self.t2v_cmb_upscale = self._create_setting_combo([
            ("720p", "none"),
            ("1080p (Free)", "1080p"),
            ("4K (+50)", "4k"),
        ], current_data="none")
        self.t2v_cmb_parallel = self._create_parallel_combo(saved_slots)

        form.addRow(self._make_setting_label("Quality:"), self.t2v_cmb_quality)
        form.addRow(
            self._make_setting_label("Ratio:"),
            self._make_inline_row(
                self.t2v_cmb_ratio,
                self._make_setting_label("Outputs:"),
                self.t2v_cmb_outputs,
            ),
        )
        form.addRow(
            self._make_setting_label("Upscale:"),
            self._make_inline_row(
                self.t2v_cmb_upscale,
                self._make_setting_label("Parallel:"),
                self.t2v_cmb_parallel,
            ),
        )
        layout.addLayout(form)
        layout.addStretch()

    def _setup_video_ref_tab(self, saved_slots):
        layout, self.ref_tab_scroll = self._create_tab_scroll_content(self.mode_tab_ref, 260)
        layout.addWidget(self._create_tab_section_title("Video + Reference Settings"))

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
        form.setFormAlignment(Qt.AlignTop)
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.ref_cmb_quality = self._create_setting_combo([
            ("Veo 3.1 - Fast", "Veo 3.1 - Fast"),
            ("Veo 3.1 - Lite", "Veo 3.1 - Lite"),
            ("Veo 3.1 - Fast [Lower Pri]", "Veo 3.1 - Fast [Lower Pri]"),
        ], current_data="Veo 3.1 - Fast")
        self.ref_cmb_ratio = self._create_setting_combo([
            ("Landscape (16:9)", "Landscape (16:9)"),
            ("Square (1:1)", "Square (1:1)"),
            ("Portrait (9:16)", "Portrait (9:16)"),
        ], current_data="Landscape (16:9)")
        self.ref_cmb_outputs = self._create_setting_combo([("x1", 1), ("x2", 2), ("x3", 3), ("x4", 4)], current_data=1)
        self.ref_cmb_upscale = self._create_setting_combo([
            ("720p", "none"),
            ("1080p (Free)", "1080p"),
            ("4K (+50)", "4k"),
        ], current_data="none")
        self.ref_cmb_parallel = self._create_parallel_combo(saved_slots)

        form.addRow(self._make_setting_label("Quality:"), self.ref_cmb_quality)
        form.addRow(
            self._make_setting_label("Ratio:"),
            self._make_inline_row(
                self.ref_cmb_ratio,
                self._make_setting_label("Outputs:"),
                self.ref_cmb_outputs,
            ),
        )
        form.addRow(
            self._make_setting_label("Upscale:"),
            self._make_inline_row(
                self.ref_cmb_upscale,
                self._make_setting_label("Parallel:"),
                self.ref_cmb_parallel,
            ),
        )

        self.ref_single_row, self.btn_ref_single_browse, self.lbl_ref_single, self.btn_ref_single_clear = self._create_path_row(
            "Browse Reference Image",
            self.select_single_reference_image,
            self.clear_single_reference_image,
        )
        form.addRow(self._make_setting_label("Reference:"), self.ref_single_row)
        layout.addLayout(form)

        layout.addWidget(self._create_separator())
        self.ref_bulk_group = self._create_bulk_panel("ingredients")
        layout.addWidget(self.ref_bulk_group)
        layout.addStretch()

    def _setup_video_frames_tab(self, saved_slots):
        layout, self.frames_tab_scroll = self._create_tab_scroll_content(self.mode_tab_frames, 280)
        layout.addWidget(self._create_tab_section_title("Frames Settings"))

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
        form.setFormAlignment(Qt.AlignTop)
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.frm_cmb_mode = self._create_setting_combo([
            ("Start Only", "frames_start"),
            ("Start + End", "frames_start_end"),
        ], current_data="frames_start")
        self.frm_cmb_mode.currentIndexChanged.connect(lambda _=None: self._sync_generation_mode_ui())
        self.frm_cmb_quality = self._create_setting_combo([
            ("Veo 3.1 - Fast", "Veo 3.1 - Fast"),
            ("Veo 3.1 - Lite", "Veo 3.1 - Lite"),
            ("Veo 3.1 - Fast [Lower Pri]", "Veo 3.1 - Fast [Lower Pri]"),
            ("Veo 3.1 - Quality", "Veo 3.1 - Quality"),
        ], current_data="Veo 3.1 - Fast")
        self.frm_cmb_ratio = self._create_setting_combo([
            ("Landscape (16:9)", "Landscape (16:9)"),
            ("Square (1:1)", "Square (1:1)"),
            ("Portrait (9:16)", "Portrait (9:16)"),
        ], current_data="Landscape (16:9)")
        self.frm_cmb_outputs = self._create_setting_combo([("x1", 1), ("x2", 2), ("x3", 3), ("x4", 4)], current_data=1)
        self.frm_cmb_upscale = self._create_setting_combo([
            ("720p", "none"),
            ("1080p (Free)", "1080p"),
            ("4K (+50)", "4k"),
        ], current_data="none")
        self.frm_cmb_parallel = self._create_parallel_combo(saved_slots)

        form.addRow(
            self._make_setting_label("Frame Mode:"),
            self._make_inline_row(
                self.frm_cmb_mode,
                self._make_setting_label("Quality:"),
                self.frm_cmb_quality,
            ),
        )
        form.addRow(
            self._make_setting_label("Ratio:"),
            self._make_inline_row(
                self.frm_cmb_ratio,
                self._make_setting_label("Outputs:"),
                self.frm_cmb_outputs,
            ),
        )
        form.addRow(
            self._make_setting_label("Upscale:"),
            self._make_inline_row(
                self.frm_cmb_upscale,
                self._make_setting_label("Parallel:"),
                self.frm_cmb_parallel,
            ),
        )

        self.start_row, self.btn_start_image, self.lbl_start_image, self.btn_clear_start_image = self._create_path_row(
            "Browse Start Image",
            self.select_start_image,
            self.clear_start_image,
        )
        self.end_row, self.btn_end_image, self.lbl_end_image, self.btn_clear_end_image = self._create_path_row(
            "Browse End Image",
            self.select_end_image,
            self.clear_end_image,
        )
        form.addRow(self._make_setting_label("Start Image:"), self.start_row)
        form.addRow(self._make_setting_label("End Image:"), self.end_row)
        layout.addLayout(form)

        self.frm_bulk_separator = self._create_separator()
        layout.addWidget(self.frm_bulk_separator)
        self.frm_bulk_group = self._create_bulk_panel("frames_start")
        layout.addWidget(self.frm_bulk_group)
        layout.addStretch()

    def _setup_pipeline_tab(self, saved_slots):
        layout, self.pipeline_tab_scroll = self._create_tab_scroll_content(self.mode_tab_pipeline, 300)
        layout.addWidget(self._create_tab_section_title("Image -> Video Pipeline"))

        step1_title = self._create_tab_section_title("Step 1: Image Generation")
        layout.addWidget(step1_title)

        form1 = QFormLayout()
        form1.setSpacing(12)
        form1.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
        form1.setFormAlignment(Qt.AlignTop)
        form1.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.pipe_cmb_img_model = self._create_setting_combo([
            ("Imagen 4", "Imagen 4"),
            ("Nano Banana Pro", "Nano Banana Pro"),
            ("Nano Banana 2", "Nano Banana 2"),
        ], current_data="Imagen 4")
        self.pipe_cmb_img_ratio = self._create_setting_combo([
            ("Landscape (16:9)", "Landscape (16:9)"),
            ("Standard (4:3)", "Standard (4:3)"),
            ("Square (1:1)", "Square (1:1)"),
            ("Portrait (3:4)", "Portrait (3:4)"),
            ("Tall Portrait (9:16)", "Tall Portrait (9:16)"),
        ], current_data="Landscape (16:9)")
        form1.addRow(self._make_setting_label("Image Model:"), self.pipe_cmb_img_model)
        form1.addRow(self._make_setting_label("Image Ratio:"), self.pipe_cmb_img_ratio)

        pipe_ref_header = QHBoxLayout()
        self.pipe_btn_add_refs = QPushButton("+ Add Reference Image(s)")
        self.pipe_btn_add_refs.setProperty("role", "browse")
        self.pipe_btn_add_refs.clicked.connect(self.select_pipeline_reference_images)
        self.pipe_lbl_ref_status = QLabel("None")
        self.pipe_lbl_ref_status.setObjectName("settingHint")
        self.pipe_btn_clear_refs = QPushButton("Clear All")
        self.pipe_btn_clear_refs.setObjectName("refClearButton")
        self.pipe_btn_clear_refs.setProperty("role", "danger")
        self.pipe_btn_clear_refs.clicked.connect(self.clear_pipeline_reference_images)
        self.pipe_btn_clear_refs.setVisible(False)
        pipe_ref_header.addWidget(self.pipe_btn_add_refs)
        pipe_ref_header.addWidget(self.pipe_lbl_ref_status, 1)
        pipe_ref_header.addWidget(self.pipe_btn_clear_refs)
        pipe_ref_widget = QWidget()
        pipe_ref_widget.setLayout(pipe_ref_header)
        form1.addRow(self._make_setting_label("Reference:"), pipe_ref_widget)
        layout.addLayout(form1)

        self.pipe_ref_items_container = QWidget()
        self.pipe_ref_items_layout = QVBoxLayout(self.pipe_ref_items_container)
        self.pipe_ref_items_layout.setContentsMargins(0, 0, 0, 0)
        self.pipe_ref_items_layout.setSpacing(6)
        self.pipe_ref_items_container.setVisible(False)
        layout.addWidget(self.pipe_ref_items_container)

        layout.addWidget(self._create_separator())
        step2_title = self._create_tab_section_title("Step 2: Video Generation")
        layout.addWidget(step2_title)

        form2 = QFormLayout()
        form2.setSpacing(12)
        form2.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
        form2.setFormAlignment(Qt.AlignTop)
        form2.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.pipe_cmb_vid_mode = self._create_setting_combo([
            ("Ingredients (Reference Style)", "ingredients"),
            ("Frames - Start Image", "frames_start"),
        ], current_data="ingredients")
        self.pipe_cmb_vid_mode.currentIndexChanged.connect(self._on_pipeline_video_mode_changed)

        self.pipe_cmb_vid_quality = self._create_setting_combo([], trigger_sync=False)
        self.pipe_cmb_vid_quality.currentIndexChanged.connect(lambda _=None: self._on_generation_settings_changed())
        self._update_pipeline_video_quality_options("ingredients")

        self.pipe_cmb_vid_ratio = self._create_setting_combo([
            ("Landscape (16:9)", "Landscape (16:9)"),
            ("Square (1:1)", "Square (1:1)"),
            ("Portrait (9:16)", "Portrait (9:16)"),
        ], current_data="Landscape (16:9)")

        self.pipe_txt_vid_prompt = QLineEdit()
        self.pipe_txt_vid_prompt.setObjectName("settingInput")
        self.pipe_txt_vid_prompt.setMinimumHeight(38)
        self.pipe_txt_vid_prompt.setPlaceholderText('Optional - default: "animate"')
        self.pipe_txt_vid_prompt.textChanged.connect(lambda _=None: self._on_generation_settings_changed())

        self.pipe_cmb_upscale = self._create_setting_combo([
            ("720p", "none"),
            ("1080p (Free)", "1080p"),
            ("4K (+50)", "4k"),
        ], current_data="none")
        self.pipe_cmb_parallel = self._create_parallel_combo(saved_slots)

        form2.addRow(self._make_setting_label("Video Mode:"), self.pipe_cmb_vid_mode)
        form2.addRow(self._make_setting_label("Video Quality:"), self.pipe_cmb_vid_quality)
        form2.addRow(self._make_setting_label("Video Ratio:"), self.pipe_cmb_vid_ratio)
        form2.addRow(self._make_setting_label("Video Prompt:"), self.pipe_txt_vid_prompt)
        form2.addRow(
            self._make_setting_label("Upscale:"),
            self._make_inline_row(
                self.pipe_cmb_upscale,
                self._make_setting_label("Parallel:"),
                self.pipe_cmb_parallel,
            ),
        )
        layout.addLayout(form2)

        layout.addWidget(self._create_separator())
        prompts_title = self._create_tab_section_title("Pipeline Prompts")
        layout.addWidget(prompts_title)

        prompts_split = QHBoxLayout()
        prompts_split.setSpacing(12)

        img_prompt_box = QVBoxLayout()
        img_prompt_box.setSpacing(6)
        img_prompt_label = QLabel("Image Prompts (one per line)")
        img_prompt_label.setStyleSheet("color: #F8FAFC; font-weight: 700;")
        self.pipe_txt_img_prompts = QPlainTextEdit()
        self.pipe_txt_img_prompts.setPlaceholderText("Enter image generation prompts here, one per line...")
        self.pipe_txt_img_prompts.setMinimumHeight(120)
        self.pipe_txt_img_prompts.verticalScrollBar().setSingleStep(6)
        self.pipe_txt_img_prompts.textChanged.connect(self._update_pipeline_count)
        img_prompt_box.addWidget(img_prompt_label)
        img_prompt_box.addWidget(self.pipe_txt_img_prompts)

        vid_prompt_box = QVBoxLayout()
        vid_prompt_box.setSpacing(6)
        vid_prompt_label = QLabel("Video Prompts (one per line)")
        vid_prompt_label.setStyleSheet("color: #F8FAFC; font-weight: 700;")
        self.pipe_txt_vid_prompts = QPlainTextEdit()
        self.pipe_txt_vid_prompts.setPlaceholderText('Video prompts - leave blank lines for "animate" default...')
        self.pipe_txt_vid_prompts.setMinimumHeight(120)
        self.pipe_txt_vid_prompts.verticalScrollBar().setSingleStep(6)
        self.pipe_txt_vid_prompts.textChanged.connect(self._update_pipeline_count)
        vid_prompt_box.addWidget(vid_prompt_label)
        vid_prompt_box.addWidget(self.pipe_txt_vid_prompts)

        prompts_split.addLayout(img_prompt_box, 1)
        prompts_split.addLayout(vid_prompt_box, 1)
        layout.addLayout(prompts_split)

        self.pipe_lbl_count = QLabel("")
        self.pipe_lbl_count.setObjectName("settingHint")
        layout.addWidget(self.pipe_lbl_count)

        pipe_actions = QHBoxLayout()
        pipe_actions.addStretch()
        self.pipe_btn_add_bulk = QPushButton("Add All to Queue")
        self.pipe_btn_add_bulk.setProperty("role", "primaryGradient")
        self.pipe_btn_add_bulk.clicked.connect(self._add_pipeline_to_queue)
        pipe_actions.addWidget(self.pipe_btn_add_bulk)
        layout.addLayout(pipe_actions)
        self._update_pipeline_count()
        layout.addStretch()

    def _create_bulk_panel(self, mode_key):
        wrapper = QWidget()
        wrapper_layout = QVBoxLayout(wrapper)
        wrapper_layout.setContentsMargins(0, 0, 0, 0)
        wrapper_layout.setSpacing(8)

        bulk_toggle = QPushButton("📦 Bulk Image Matching  ▶")
        bulk_toggle.setCheckable(True)
        bulk_toggle.setChecked(False)
        bulk_toggle.setProperty("role", "bulkToggle")
        bulk_toggle.clicked.connect(lambda checked=False, key=mode_key: self._toggle_bulk_section(key, checked))
        wrapper_layout.addWidget(bulk_toggle)

        content = QFrame()
        content.setObjectName("bulkPanel")
        content.setVisible(False)
        panel_layout = QVBoxLayout(content)
        panel_layout.setContentsMargins(10, 10, 10, 10)
        panel_layout.setSpacing(10)

        browse_style = """
            QPushButton {
                background: #1A2744;
                color: #3B82F6;
                border: 1px dashed #3B82F6;
                border-radius: 6px;
                padding: 8px 16px;
                font-size: 13px;
                font-weight: 600;
                min-width: 100px;
            }
            QPushButton:hover {
                background: #1E2D4A;
            }
        """
        clear_style = """
            QPushButton {
                background: #1F1A2A;
                color: #EF4444;
                border: 1px solid #EF4444;
                border-radius: 6px;
                padding: 8px 16px;
                font-size: 13px;
                font-weight: 600;
                min-width: 90px;
            }
            QPushButton:hover {
                background: #2A1D2C;
            }
        """
        add_style = """
            QPushButton {
                background: #3B82F6;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 10px 18px;
                font-size: 13px;
                font-weight: 700;
                min-width: 150px;
            }
            QPushButton:hover {
                background: #2563EB;
            }
        """

        header = QHBoxLayout()
        btn_folder = QPushButton()
        btn_folder.setText("Browse Folder")
        btn_folder.setProperty("role", "browse")
        btn_folder.setStyleSheet(browse_style)
        btn_folder.clicked.connect(lambda _=False, key=mode_key: self.select_bulk_image_folder(key))
        btn_files = QPushButton()
        btn_files.setText("Browse Images")
        btn_files.setProperty("role", "browse")
        btn_files.setStyleSheet(browse_style)
        btn_files.clicked.connect(lambda _=False, key=mode_key: self.select_bulk_image_files(key))
        lbl_loaded = QLabel("0 image(s)")
        lbl_loaded.setObjectName("settingHint")
        btn_clear = QPushButton()
        btn_clear.setText("Clear")
        btn_clear.setProperty("role", "danger")
        btn_clear.setStyleSheet(clear_style)
        btn_clear.clicked.connect(lambda _=False, key=mode_key: self.clear_bulk_panel(key))
        header.addWidget(btn_folder)
        header.addWidget(btn_files)
        header.addWidget(lbl_loaded)
        header.addStretch()
        header.addWidget(btn_clear)
        panel_layout.addLayout(header)

        sort_row = QHBoxLayout()
        sort_label = self._make_setting_label("Sort:")
        sort_row.addWidget(sort_label)
        cmb_sort = self._create_setting_combo([
            ("Name A-Z", "name_asc"),
            ("Name Z-A", "name_desc"),
            ("Time Old-New", "time_old"),
            ("Time New-Old", "time_new"),
        ], current_data="name_asc", trigger_sync=False)
        cmb_sort.currentIndexChanged.connect(lambda _=None, key=mode_key: self._refresh_bulk_pairing_preview(key))
        sort_row.addWidget(cmb_sort)
        missing_label = self._make_setting_label("Missing Prompt:")
        sort_row.addWidget(missing_label)
        cmb_missing = self._create_setting_combo([
            ("Use filename", "filename"),
            ("Skip", "skip"),
        ], current_data="filename", trigger_sync=False)
        cmb_missing.currentIndexChanged.connect(lambda _=None, key=mode_key: self._refresh_bulk_pairing_preview(key))
        sort_row.addWidget(cmb_missing)
        sort_row.addStretch()
        panel_layout.addLayout(sort_row)

        tbl_images = BulkImageDropTable(0, 3)
        tbl_images.setHorizontalHeaderLabels(["#", "Image", "Filename"])
        tbl_images.verticalHeader().setVisible(False)
        tbl_images.setAlternatingRowColors(True)
        tbl_images.setSelectionMode(QTableWidget.NoSelection)
        tbl_images.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        tbl_images.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        tbl_images.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        tbl_images.setMinimumHeight(110)
        self._configure_table_scrolling(tbl_images)
        tbl_images.setVisible(False)
        tbl_images.files_dropped.connect(lambda paths, key=mode_key: self._load_bulk_images_from_paths(key, paths))
        panel_layout.addWidget(tbl_images)

        prompts_input = QTextEdit()
        prompts_input.setPlaceholderText("Prompts, one per line. Line 1 pairs with image 1.")
        prompts_input.setMinimumHeight(84)
        prompts_input.verticalScrollBar().setSingleStep(6)
        prompts_input.textChanged.connect(lambda key=mode_key: self._refresh_bulk_pairing_preview(key))
        panel_layout.addWidget(prompts_input)

        pairs_table = QTableWidget(0, 3)
        pairs_table.setHorizontalHeaderLabels(["#", "Image", "Paired Prompt"])
        pairs_table.verticalHeader().setVisible(False)
        pairs_table.setAlternatingRowColors(True)
        pairs_table.setSelectionMode(QTableWidget.NoSelection)
        pairs_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        pairs_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        pairs_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        pairs_table.setMinimumHeight(130)
        self._configure_table_scrolling(pairs_table)
        pairs_table.setVisible(False)
        panel_layout.addWidget(pairs_table)

        lbl_hint = QLabel("Drop images or browse, then add prompts to preview pairings.")
        lbl_hint.setObjectName("settingHint")
        lbl_hint.setWordWrap(True)
        panel_layout.addWidget(lbl_hint)

        btn_add = QPushButton()
        btn_add.setText("Add All to Queue")
        btn_add.setProperty("role", "primaryGradient")
        btn_add.setStyleSheet(add_style)
        btn_add.clicked.connect(lambda _=False, key=mode_key: self.add_bulk_i2v_to_queue(key))
        panel_layout.addWidget(btn_add, alignment=Qt.AlignLeft)
        panel_layout.addStretch()
        wrapper_layout.addWidget(content)

        self.bulk_panels[mode_key] = {
            "group": wrapper,
            "wrapper": wrapper,
            "toggle": bulk_toggle,
            "content": content,
            "entries": [],
            "btn_folder": btn_folder,
            "btn_files": btn_files,
            "lbl_loaded": lbl_loaded,
            "btn_clear": btn_clear,
            "sort_selector": cmb_sort,
            "missing_selector": cmb_missing,
            "images_table": tbl_images,
            "prompts_input": prompts_input,
            "pairs_table": pairs_table,
            "hint_label": lbl_hint,
            "add_btn": btn_add,
        }
        return wrapper
        
    def setup_accounts(self):
        layout = QVBoxLayout(self.tab_accounts)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        overview = QFrame()
        overview.setObjectName("accountOverviewCard")
        overview_layout = QHBoxLayout(overview)
        overview_layout.setContentsMargins(14, 10, 14, 10)
        overview_layout.setSpacing(8)
        overview_title = QLabel("Account Health Monitor")
        overview_title.setObjectName("accountOverviewTitle")
        overview_layout.addWidget(overview_title)
        overview_layout.addStretch()

        self.lbl_acc_total = QLabel("Total: 0")
        self.lbl_acc_total.setObjectName("accountMetricChip")
        self.lbl_acc_logged_in = QLabel("Logged In: 0")
        self.lbl_acc_logged_in.setObjectName("accountMetricChip")
        self.lbl_acc_logged_out = QLabel("Logged Out: 0")
        self.lbl_acc_logged_out.setObjectName("accountMetricChip")
        self.lbl_acc_running = QLabel("Running: 0")
        self.lbl_acc_running.setObjectName("accountMetricChip")
        self.lbl_acc_cooldown = QLabel("Cooldown: 0")
        self.lbl_acc_cooldown.setObjectName("accountMetricChip")
        self.lbl_acc_ready = QLabel("Ready: 0")
        self.lbl_acc_ready.setObjectName("accountMetricChip")

        overview_layout.addWidget(self.lbl_acc_total)
        overview_layout.addWidget(self.lbl_acc_logged_in)
        overview_layout.addWidget(self.lbl_acc_logged_out)
        overview_layout.addWidget(self.lbl_acc_running)
        overview_layout.addWidget(self.lbl_acc_cooldown)
        overview_layout.addWidget(self.lbl_acc_ready)
        layout.addWidget(overview)
        
        add_group = QGroupBox("Add New Google Account Session")
        add_layout = QGridLayout()
        add_layout.setHorizontalSpacing(10)
        add_layout.setVerticalSpacing(8)
        self.acc_name_input = QLineEdit()
        self.acc_name_input.setPlaceholderText("Optional alias (leave blank for auto Gmail)")
        self.acc_name_input.setMinimumHeight(36)
        self.acc_proxy_input = QLineEdit()
        self.acc_proxy_input.setPlaceholderText("Optional proxy: socks5://user:pass@host:port")
        self.acc_proxy_input.setMinimumHeight(36)
        self.btn_login = QPushButton("Login to Google (New Browser)")
        self.btn_login.setProperty("role", "primary")
        self.btn_login.setMinimumHeight(38)
        self.btn_login.clicked.connect(self.start_login)
        
        add_layout.addWidget(QLabel("Account Name"), 0, 0)
        add_layout.addWidget(self.acc_name_input, 0, 1)
        add_layout.addWidget(QLabel("Proxy"), 1, 0)
        add_layout.addWidget(self.acc_proxy_input, 1, 1)
        add_layout.addWidget(self.btn_login, 0, 2, 2, 1)
        add_layout.setColumnStretch(1, 1)
        add_group.setLayout(add_layout)
        layout.addWidget(add_group)

        self.download_widget = QWidget()
        download_layout = QHBoxLayout(self.download_widget)
        download_layout.setContentsMargins(0, 4, 0, 4)
        download_layout.setSpacing(10)
        self.download_label = QLabel("Downloading CloakBrowser binary...")
        self.download_label.setStyleSheet("color: #60A5FA; font-weight: 600;")
        self.download_progress = QProgressBar()
        self.download_progress.setRange(0, 100)
        self.download_progress.setValue(0)
        self.download_progress.setFixedHeight(20)
        self.download_progress.setStyleSheet(
            """
            QProgressBar {
                border: 1px solid #334155;
                border-radius: 4px;
                background-color: #1E293B;
                text-align: center;
                color: white;
                font-weight: 600;
            }
            QProgressBar::chunk {
                background-color: #3B82F6;
                border-radius: 3px;
            }
            """
        )
        self.download_percent = QLabel("0%")
        self.download_percent.setStyleSheet("color: #60A5FA; font-weight: 600; min-width: 40px;")
        download_layout.addWidget(self.download_label)
        download_layout.addWidget(self.download_progress, 1)
        download_layout.addWidget(self.download_percent)
        self.download_widget.setVisible(False)
        layout.addWidget(self.download_widget)
        
        self.acc_table = QTableWidget(0, 10)
        self.acc_table.setHorizontalHeaderLabels([
            "ID",
            "Account Name",
            "Proxy",
            "Login Status",
            "Saved Status",
            "Runtime",
            "Cooldown Left",
            "Active Slots",
            "Detail",
            "Actions",
        ])
        self.acc_table.verticalHeader().setVisible(False)
        self.acc_table.verticalHeader().setDefaultSectionSize(40)
        self.acc_table.setAlternatingRowColors(False)
        self.acc_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.acc_table.setSelectionMode(QTableWidget.SingleSelection)
        self.acc_table.setStyleSheet(
            """
            QTableWidget {
                background: #1E293B;
                border: 1px solid #334155;
                border-radius: 6px;
                gridline-color: #334155;
                color: white;
                font-size: 12px;
            }
            QHeaderView::section {
                background: #0F172A;
                color: #94A3B8;
                font-size: 11px;
                font-weight: 600;
                border: none;
                border-bottom: 2px solid #334155;
                padding: 6px 8px;
            }
            QTableWidget::item {
                padding: 4px 8px;
                border-bottom: 1px solid #1E293B;
            }
            """
        )
        acc_header = self.acc_table.horizontalHeader()
        acc_header.setStretchLastSection(True)
        acc_header.setSectionResizeMode(0, QHeaderView.Fixed)
        acc_header.setSectionResizeMode(1, QHeaderView.Stretch)
        acc_header.setSectionResizeMode(2, QHeaderView.Fixed)
        acc_header.setSectionResizeMode(3, QHeaderView.Fixed)
        acc_header.setSectionResizeMode(4, QHeaderView.Fixed)
        acc_header.setSectionResizeMode(5, QHeaderView.Fixed)
        acc_header.setSectionResizeMode(6, QHeaderView.Fixed)
        acc_header.setSectionResizeMode(7, QHeaderView.Fixed)
        acc_header.setSectionResizeMode(8, QHeaderView.Stretch)
        acc_header.setSectionResizeMode(9, QHeaderView.Fixed)
        self.acc_table.setColumnWidth(0, 40)
        self.acc_table.setColumnWidth(2, 80)
        self.acc_table.setColumnWidth(3, 120)
        self.acc_table.setColumnWidth(4, 90)
        self.acc_table.setColumnWidth(5, 90)
        self.acc_table.setColumnWidth(6, 100)
        self.acc_table.setColumnWidth(7, 80)
        self.acc_table.setColumnWidth(9, 150)
        self._configure_table_scrolling(self.acc_table)
        layout.addWidget(self.acc_table)
        
        bottom_layout = QHBoxLayout()
        self.btn_refresh_accs = QPushButton("Refresh List")
        self.btn_refresh_accs.setProperty("role", "secondary")
        self.btn_refresh_accs.clicked.connect(self.refresh_accounts)

        self.btn_delete_acc = QPushButton("Delete Selected")
        self.btn_delete_acc.setProperty("role", "subtleDanger")
        self.btn_delete_acc.clicked.connect(self.delete_selected_account)
        
        bottom_layout.addWidget(self.btn_refresh_accs)
        bottom_layout.addStretch()
        bottom_layout.addWidget(self.btn_delete_acc)
        layout.addLayout(bottom_layout)
        
        self.load_accounts()
        
    def setup_failed_jobs(self):
        layout = QVBoxLayout(self.tab_failed_jobs)
        layout.setContentsMargins(10, 10, 10, 10)

        top_controls = QHBoxLayout()
        self.chk_select_all_failed = QCheckBox("Select All")
        self.chk_select_all_failed.clicked.connect(self._toggle_select_all_failed)
        top_controls.addWidget(self.chk_select_all_failed)
        top_controls.addStretch()
        layout.addLayout(top_controls)

        self.failed_table = QTableWidget(0, 7)
        self.failed_table.setHorizontalHeaderLabels(
            ["✓", "#", "Prompt", "Type", "Error Reason", "Original Prompt", "Status"]
        )
        self.failed_table.verticalHeader().setVisible(False)
        self.failed_table.setAlternatingRowColors(True)
        self.failed_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.failed_table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.failed_table.setEditTriggers(QAbstractItemView.DoubleClicked | QAbstractItemView.EditKeyPressed)
        self.failed_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.failed_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.failed_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.failed_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.failed_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.failed_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.failed_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.failed_table.itemSelectionChanged.connect(self._update_failed_jobs_actions)
        self.failed_table.itemChanged.connect(self._on_failed_table_item_changed)
        self._configure_table_scrolling(self.failed_table)
        layout.addWidget(self.failed_table)
        
        bottom_layout = QHBoxLayout()
        self.btn_refresh_failed = QPushButton("Refresh List")
        self.btn_refresh_failed.setProperty("role", "secondary")
        self.btn_refresh_failed.clicked.connect(self.load_failed_jobs)

        self.btn_requeue_selected = QPushButton("🔄 Retry Selected")
        self.btn_requeue_selected.setProperty("role", "warning")
        self.btn_requeue_selected.clicked.connect(self._retry_selected_failed)

        self.btn_retry_all_failed = QPushButton("🔄 Retry All Failed")
        self.btn_retry_all_failed.setProperty("role", "warning")
        self.btn_retry_all_failed.clicked.connect(self._retry_all_failed)

        self.btn_copy_failed = QPushButton("📋 Copy Failed Prompts")
        self.btn_copy_failed.setProperty("role", "secondary")
        self.btn_copy_failed.clicked.connect(self.copy_failed_prompts)

        self.btn_clear_failed = QPushButton("Clear Failed")
        self.btn_clear_failed.setProperty("role", "subtleDanger")
        self.btn_clear_failed.clicked.connect(self.clear_failed_jobs_list)
        
        bottom_layout.addWidget(self.btn_refresh_failed)
        bottom_layout.addStretch()
        bottom_layout.addWidget(self.btn_requeue_selected)
        bottom_layout.addWidget(self.btn_retry_all_failed)
        bottom_layout.addWidget(self.btn_copy_failed)
        bottom_layout.addWidget(self.btn_clear_failed)
        layout.addLayout(bottom_layout)
        
        self.load_failed_jobs()

    def setup_settings(self):
        layout = QVBoxLayout(self.tab_settings)
        layout.setContentsMargins(10, 10, 10, 10)

        perf_group = QGroupBox("Automation Engine")
        perf_layout = QVBoxLayout()

        slots_layout = QHBoxLayout()
        slots_layout.addWidget(QLabel("Worker Slots Per Account:"))
        self.spin_slots_per_account = QSpinBox()
        self.spin_slots_per_account.setRange(1, 6)
        self.spin_slots_per_account.setValue(max(1, min(6, get_int_setting("slots_per_account", 3))))
        slots_layout.addWidget(self.spin_slots_per_account)
        slots_layout.addStretch()
        perf_layout.addLayout(slots_layout)

        stagger_layout = QHBoxLayout()
        stagger_layout.addWidget(QLabel("Same-Account Stagger (sec):"))
        self.spin_same_account_stagger = QDoubleSpinBox()
        self.spin_same_account_stagger.setRange(0.0, 60.0)
        self.spin_same_account_stagger.setDecimals(1)
        self.spin_same_account_stagger.setSingleStep(0.1)
        self.spin_same_account_stagger.setValue(
            max(0.0, min(60.0, get_float_setting("same_account_stagger_seconds", 1.0)))
        )
        stagger_layout.addWidget(self.spin_same_account_stagger)
        stagger_layout.addStretch()
        perf_layout.addLayout(stagger_layout)

        global_stagger_layout = QHBoxLayout()
        global_stagger_layout.addWidget(QLabel("Global Stagger (sec):"))
        self.spin_global_stagger_min = QDoubleSpinBox()
        self.spin_global_stagger_min.setRange(0.0, 60.0)
        self.spin_global_stagger_min.setDecimals(1)
        self.spin_global_stagger_min.setSingleStep(0.1)
        self.spin_global_stagger_min.setValue(
            max(0.0, min(60.0, get_float_setting("global_stagger_min_seconds", 0.3)))
        )
        global_stagger_layout.addWidget(self.spin_global_stagger_min)
        global_stagger_layout.addWidget(QLabel("to"))
        self.spin_global_stagger_max = QDoubleSpinBox()
        self.spin_global_stagger_max.setRange(0.0, 120.0)
        self.spin_global_stagger_max.setDecimals(1)
        self.spin_global_stagger_max.setSingleStep(0.1)
        saved_gmax = get_float_setting("global_stagger_max_seconds", 0.6)
        self.spin_global_stagger_max.setValue(
            max(self.spin_global_stagger_min.value(), min(120.0, saved_gmax))
        )
        global_stagger_layout.addWidget(self.spin_global_stagger_max)
        global_stagger_layout.addStretch()
        perf_layout.addLayout(global_stagger_layout)

        speed_profile_layout = QHBoxLayout()
        speed_profile_layout.addWidget(QLabel("Speed Profile:"))
        self.cmb_speed_profile = QComboBox()
        self.cmb_speed_profile.addItem("Slow Stable", "stable")
        self.cmb_speed_profile.addItem("Fast", "fast")
        saved_speed_profile = str(get_setting("speed_profile", "fast") or "fast").strip().lower()
        speed_index = self.cmb_speed_profile.findData(saved_speed_profile if saved_speed_profile in ("stable", "fast") else "fast")
        if speed_index < 0:
            speed_index = 0
        self.cmb_speed_profile.setCurrentIndex(speed_index)
        speed_profile_layout.addWidget(self.cmb_speed_profile)
        speed_profile_layout.addStretch()
        perf_layout.addLayout(speed_profile_layout)

        warmup_layout = QHBoxLayout()
        warmup_layout.addWidget(QLabel("Warmup Delay (sec):"))
        self.spin_warmup_min = QDoubleSpinBox()
        self.spin_warmup_min.setRange(0.0, 10.0)
        self.spin_warmup_min.setDecimals(1)
        self.spin_warmup_min.setSingleStep(0.1)
        self.spin_warmup_min.setValue(
            max(0.0, min(10.0, get_float_setting("api_humanized_warmup_min_seconds", 0.2)))
        )
        warmup_layout.addWidget(self.spin_warmup_min)
        warmup_layout.addWidget(QLabel("to"))
        self.spin_warmup_max = QDoubleSpinBox()
        self.spin_warmup_max.setRange(0.0, 10.0)
        self.spin_warmup_max.setDecimals(1)
        self.spin_warmup_max.setSingleStep(0.1)
        saved_warmup_max = get_float_setting("api_humanized_warmup_max_seconds", 0.4)
        self.spin_warmup_max.setValue(
            max(self.spin_warmup_min.value(), min(10.0, saved_warmup_max))
        )
        warmup_layout.addWidget(self.spin_warmup_max)
        warmup_layout.addStretch()
        perf_layout.addLayout(warmup_layout)

        self.chk_profile_clone = QCheckBox("Enable profile cloning for extra slots (required for same-account parallel)")
        self.chk_profile_clone.setChecked(get_bool_setting("enable_profile_clones", True))
        perf_layout.addWidget(self.chk_profile_clone)

        recaptcha_layout = QHBoxLayout()
        recaptcha_layout.addWidget(QLabel("ReCAPTCHA Slot Cooldown (sec):"))
        self.spin_recaptcha_cooldown = QSpinBox()
        self.spin_recaptcha_cooldown.setRange(5, 600)
        self.spin_recaptcha_cooldown.setValue(
            max(5, min(600, get_int_setting("recaptcha_account_cooldown_seconds", 15)))
        )
        recaptcha_layout.addWidget(self.spin_recaptcha_cooldown)
        recaptcha_layout.addStretch()
        perf_layout.addLayout(recaptcha_layout)

        retry_layout = QHBoxLayout()
        retry_layout.addWidget(QLabel("Max Retries Per Job:"))
        self.spin_max_retries = QSpinBox()
        self.spin_max_retries.setRange(0, 5)
        self.spin_max_retries.setValue(
            max(0, min(5, get_int_setting("max_retries", get_int_setting("max_auto_retries_per_job", 3))))
        )
        retry_layout.addWidget(self.spin_max_retries)
        retry_layout.addStretch()
        perf_layout.addLayout(retry_layout)

        retry_delay_layout = QHBoxLayout()
        retry_delay_layout.addWidget(QLabel("Retry Base Delay (sec):"))
        self.spin_retry_base_delay = QSpinBox()
        self.spin_retry_base_delay.setRange(5, 300)
        self.spin_retry_base_delay.setValue(
            max(5, min(300, get_int_setting("retry_base_delay_seconds", get_int_setting("auto_retry_base_delay_seconds", 10))))
        )
        retry_delay_layout.addWidget(self.spin_retry_base_delay)
        retry_delay_layout.addStretch()
        perf_layout.addLayout(retry_delay_layout)

        auto_refresh_layout = QHBoxLayout()
        auto_refresh_layout.addWidget(QLabel("Auto-refresh After N Jobs:"))
        self.spin_auto_refresh_after_jobs = QSpinBox()
        self.spin_auto_refresh_after_jobs.setRange(0, 10000)
        self.spin_auto_refresh_after_jobs.setValue(
            max(0, min(10000, get_int_setting("auto_refresh_after_jobs", 150)))
        )
        self.spin_auto_refresh_after_jobs.setToolTip(
            "0 disables auto-refresh. Otherwise, refresh account slots after every N successful jobs."
        )
        auto_refresh_layout.addWidget(self.spin_auto_refresh_after_jobs)
        auto_refresh_layout.addStretch()
        perf_layout.addLayout(auto_refresh_layout)

        auto_restart_fail_layout = QHBoxLayout()
        auto_restart_fail_layout.addWidget(QLabel("Auto-restart After N reCAPTCHA Fails:"))
        self.spin_restart_threshold = QSpinBox()
        self.spin_restart_threshold.setRange(0, 20)
        self.spin_restart_threshold.setValue(
            max(0, min(20, get_int_setting("auto_restart_recap_fail_threshold", 3)))
        )
        auto_restart_fail_layout.addWidget(self.spin_restart_threshold)
        auto_restart_fail_layout.addWidget(QLabel("in last"))
        self.spin_restart_window = QSpinBox()
        self.spin_restart_window.setRange(5, 50)
        self.spin_restart_window.setValue(
            max(5, min(50, get_int_setting("auto_restart_recap_fail_window", 10)))
        )
        auto_restart_fail_layout.addWidget(self.spin_restart_window)
        auto_restart_fail_layout.addWidget(QLabel("attempts"))
        auto_restart_fail_layout.addStretch()
        perf_layout.addLayout(auto_restart_fail_layout)

        auto_restart_cooldown_layout = QHBoxLayout()
        auto_restart_cooldown_layout.addWidget(QLabel("Cooldown Before Auto-restart (sec):"))
        self.spin_restart_cooldown = QSpinBox()
        self.spin_restart_cooldown.setRange(10, 120)
        self.spin_restart_cooldown.setValue(
            max(10, min(120, get_int_setting("auto_restart_recap_cooldown_seconds", 30)))
        )
        auto_restart_cooldown_layout.addWidget(self.spin_restart_cooldown)
        auto_restart_cooldown_layout.addStretch()
        perf_layout.addLayout(auto_restart_cooldown_layout)

        self.chk_api_captcha_submit_lock = QCheckBox(
            "Legacy API submit lane (disabled; stagger controls now handle pacing)"
        )
        self.chk_api_captcha_submit_lock.setChecked(False)
        self.chk_api_captcha_submit_lock.setEnabled(False)
        self.chk_api_captcha_submit_lock.setToolTip(
            "Submit-lane throttling has been removed. Same-account and global stagger settings now control pacing."
        )
        perf_layout.addWidget(self.chk_api_captcha_submit_lock)

        mode_layout = QHBoxLayout()
        mode_layout.addWidget(QLabel("Image Execution Mode:"))
        self.cmb_image_execution_mode = QComboBox()
        self.cmb_image_execution_mode.addItem("API only (queue retries on failure)", "api_only")
        self.cmb_image_execution_mode.setCurrentIndex(0)
        self.cmb_image_execution_mode.setEnabled(False)
        self.cmb_image_execution_mode.setToolTip(
            "Generation uses API-only mode. Transient failures retry through the queue."
        )
        self.cmb_image_execution_mode.currentIndexChanged.connect(lambda _=None: self._update_runtime_badges())
        mode_layout.addWidget(self.cmb_image_execution_mode)
        mode_layout.addStretch()
        perf_layout.addLayout(mode_layout)

        browser_layout = QHBoxLayout()
        browser_layout.addWidget(QLabel("Browser Mode:"))
        self.cmb_browser_mode = QComboBox()
        self.cmb_browser_mode.addItem("Headless (Playwright)", "headless")
        self.cmb_browser_mode.addItem("Visible (Playwright)", "visible")
        self.cmb_browser_mode.addItem("Real Chrome (CDP)", "real_chrome")
        self.cmb_browser_mode.addItem("CloakBrowser (Best Stealth)", "cloakbrowser")
        self.cmb_browser_mode.currentIndexChanged.connect(self._on_browser_mode_changed)
        saved_browser_mode = str(get_setting("browser_mode", "cloakbrowser") or "cloakbrowser").strip().lower()
        if saved_browser_mode == "playwright":
            saved_browser_mode = "visible"
        browser_mode_index = self.cmb_browser_mode.findData(saved_browser_mode)
        if browser_mode_index < 0:
            browser_mode_index = self.cmb_browser_mode.findData("cloakbrowser")
        self.cmb_browser_mode.setCurrentIndex(browser_mode_index)
        browser_layout.addWidget(self.cmb_browser_mode)

        self.lbl_chrome_display = QLabel("Chrome Display:")
        browser_layout.addWidget(self.lbl_chrome_display)
        self.cmb_chrome_display = QComboBox()
        self.cmb_chrome_display.addItem("Visible (window dikhega)", "visible")
        self.cmb_chrome_display.addItem("Headless (background me chalega)", "headless")
        saved_chrome_display = str(get_setting("chrome_display", "headless") or "headless").strip().lower()
        chrome_display_index = self.cmb_chrome_display.findData(saved_chrome_display)
        if chrome_display_index < 0:
            chrome_display_index = self.cmb_chrome_display.findData("headless")
        self.cmb_chrome_display.setCurrentIndex(chrome_display_index)
        browser_layout.addWidget(self.cmb_chrome_display)

        self.lbl_cloak_display = QLabel("Cloak Display:")
        browser_layout.addWidget(self.lbl_cloak_display)
        self.cmb_cloak_display = QComboBox()
        self.cmb_cloak_display.addItem("Headless", "headless")
        self.cmb_cloak_display.addItem("Visible", "visible")
        saved_cloak_display = str(get_setting("cloak_display", "headless") or "headless").strip().lower()
        cloak_display_index = self.cmb_cloak_display.findData(saved_cloak_display)
        if cloak_display_index < 0:
            cloak_display_index = self.cmb_cloak_display.findData("headless")
        self.cmb_cloak_display.setCurrentIndex(cloak_display_index)
        browser_layout.addWidget(self.cmb_cloak_display)
        browser_layout.addStretch()
        perf_layout.addLayout(browser_layout)
        self._on_browser_mode_changed(self.cmb_browser_mode.currentIndex())

        self.chk_random_fingerprint = QCheckBox("Random fingerprint per session (like GoLogin)")
        self.chk_random_fingerprint.setChecked(get_bool_setting("random_fingerprint_per_session", False))
        self.chk_random_fingerprint.setStyleSheet("color: #94A3B8; font-size: 12px;")
        perf_layout.addWidget(self.chk_random_fingerprint)

        warmup_checkbox_style = (
            "QCheckBox { color: #94A3B8; font-size: 12px; padding: 2px 0; }"
            "QCheckBox::indicator { width: 16px; height: 16px; }"
            "QCheckBox::indicator:checked { background: #2563EB; border: 1px solid #3B82F6; border-radius: 3px; }"
            "QCheckBox::indicator:unchecked { background: #1E293B; border: 1px solid #475569; border-radius: 3px; }"
        )

        self.chk_cookie_warmup = QCheckBox("Cookie warm-up on first login (heavy — 2 searches + 3-4 site visits)")
        self.chk_cookie_warmup.setChecked(get_bool_setting("cookie_warmup", True))
        self.chk_cookie_warmup.setToolTip(
            "When enabled, performs a full cookie warm-up after first login:\n"
            "2 Google Searches + click results + 3-4 random site visits.\n"
            "Takes ~4-5 minutes. Improves reCAPTCHA score significantly.\n"
            "Only runs ONCE per account (won't repeat after first login)."
        )
        self.chk_cookie_warmup.setStyleSheet(warmup_checkbox_style)
        perf_layout.addWidget(self.chk_cookie_warmup)

        self.chk_light_warmup = QCheckBox("Quick warm-up before each run (light — 1 search + 0-1 site visit)")
        self.chk_light_warmup.setChecked(get_bool_setting("light_warmup", True))
        self.chk_light_warmup.setToolTip(
            "When enabled, performs a quick cookie refresh before each generation run:\n"
            "1 Google Search + click result + maybe 1 random site.\n"
            "Takes ~30-45 seconds. Keeps cookies fresh between runs."
        )
        self.chk_light_warmup.setStyleSheet(warmup_checkbox_style)
        perf_layout.addWidget(self.chk_light_warmup)

        self.cloak_update_widget = QWidget()
        cloak_update_layout = QHBoxLayout(self.cloak_update_widget)
        cloak_update_layout.setContentsMargins(0, 5, 0, 5)
        cloak_update_layout.setSpacing(10)
        self.lbl_cloak_version = QLabel("CloakBrowser: checking...")
        self.lbl_cloak_version.setStyleSheet("color: #94A3B8; font-size: 12px;")
        self.btn_cloak_update = QPushButton("🔄 Check for Updates")
        self.btn_cloak_update.setFixedWidth(200)
        self.btn_cloak_update.setFixedHeight(30)
        self.btn_cloak_update.setStyleSheet(
            """
            QPushButton {
                background-color: #334155;
                color: white;
                border: 1px solid #475569;
                border-radius: 4px;
                font-size: 12px;
                padding: 4px 12px;
            }
            QPushButton:hover {
                background-color: #475569;
            }
            QPushButton:disabled {
                background-color: #1E293B;
                color: #64748B;
            }
            """
        )
        self.btn_cloak_update.clicked.connect(self._on_cloak_update_clicked)
        cloak_update_layout.addWidget(self.lbl_cloak_version, 1)
        cloak_update_layout.addWidget(self.btn_cloak_update)
        perf_layout.addWidget(self.cloak_update_widget)

        self.lbl_cloak_update_status = QLabel("")
        self.lbl_cloak_update_status.setStyleSheet("color: #94A3B8; font-size: 11px;")
        self.lbl_cloak_update_status.setVisible(False)
        perf_layout.addWidget(self.lbl_cloak_update_status)

        output_dir_layout = QHBoxLayout()
        output_dir_layout.addWidget(QLabel("Output Folder:"))
        self.output_dir_input = QLineEdit()
        self.output_dir_input.setReadOnly(True)
        self.output_dir_input.setPlaceholderText("Choose where generated files should be saved")
        self.output_dir_input.setText(self._outputs_dir())
        output_dir_layout.addWidget(self.output_dir_input, 1)
        self.btn_browse_output_dir = QPushButton("Browse…")
        self.btn_browse_output_dir.setProperty("role", "browse")
        self.btn_browse_output_dir.clicked.connect(self._browse_output_directory)
        output_dir_layout.addWidget(self.btn_browse_output_dir)
        self.btn_reset_output_dir = QPushButton("Reset")
        self.btn_reset_output_dir.setProperty("role", "secondary")
        self.btn_reset_output_dir.clicked.connect(self._reset_output_directory)
        output_dir_layout.addWidget(self.btn_reset_output_dir)
        perf_layout.addLayout(output_dir_layout)

        note = QLabel(
            "Note: More slots increase RAM and CPU usage. "
            "If your system gets slow, reduce slots to 1-2."
        )
        note.setWordWrap(True)
        perf_layout.addWidget(note)

        self.btn_save_settings = QPushButton("Save Settings")
        self.btn_save_settings.setProperty("role", "primary")
        self.btn_save_settings.clicked.connect(self.save_settings)
        perf_layout.addWidget(self.btn_save_settings)

        self.btn_clean_profiles = QPushButton("Clean All Browser Profiles")
        self.btn_clean_profiles.setToolTip(
            "Removes accumulated cache, service workers, and tracking data.\n"
            "Preserves login cookies and session.\n"
            "Run this if reCAPTCHA errors are increasing.\n"
            "Same effect as reinstalling but without losing accounts."
        )
        self.btn_clean_profiles.setStyleSheet(
            "QPushButton { background: #DC2626; color: white; padding: 8px 16px; "
            "border-radius: 6px; font-weight: 600; } "
            "QPushButton:hover { background: #EF4444; }"
        )
        self.btn_clean_profiles.clicked.connect(self._on_clean_profiles)
        perf_layout.addWidget(self.btn_clean_profiles)

        perf_group.setLayout(perf_layout)
        layout.addWidget(perf_group)
        layout.addStretch()
        self._cloak_update_worker = None
        self._auto_check_cloak_on_startup()
        self._on_browser_mode_changed(self.cmb_browser_mode.currentIndex())

    def _apply_modern_theme(self):
        self.setStyleSheet(
            """
            QMainWindow, QWidget {
                background-color: #0F172A;
                color: #F8FAFC;
                font-family: "SF Pro Display", "Segoe UI", "Inter", "Helvetica Neue", "Arial";
                font-size: 13px;
            }
            QMainWindow#mainWindow {
                background: #0F172A;
            }
            QFrame#sidebarNav {
                background: #0B1120;
                border: none;
            }
            QLabel#sidebarTitle {
                color: #F8FAFC;
                font-size: 19px;
                font-weight: 800;
                padding: 4px 4px 14px 4px;
            }
            QFrame#sidebarDivider, QFrame#sidebarShellDivider {
                background: #1E293B;
                color: #1E293B;
                border: none;
            }
            QPushButton#sidebarNavButton {
                background: transparent;
                color: #94A3B8;
                text-align: left;
                font-size: 12px;
                font-weight: 600;
                padding: 9px 12px;
                border: none;
                border-radius: 8px;
            }
            QPushButton#sidebarNavButton:hover {
                background: #1E293B;
                color: #FFFFFF;
            }
            QPushButton#sidebarNavButton:checked {
                background: #2563EB;
                color: #FFFFFF;
            }
            QLabel#sidebarStat {
                color: #94A3B8;
                font-size: 11px;
                padding: 2px 4px;
            }
            QLabel#sidebarStatRunning {
                color: #60A5FA;
                font-size: 11px;
                padding: 2px 4px;
            }
            QLabel#sidebarStatDone {
                color: #22C55E;
                font-size: 11px;
                padding: 2px 4px;
            }
            QPushButton#sidebarFailedButton {
                background: transparent;
                color: #EF4444;
                text-align: left;
                font-size: 11px;
                font-weight: 600;
                padding: 4px;
                border: none;
                border-radius: 6px;
            }
            QPushButton#sidebarFailedButton:hover {
                background: #2D1B1B;
                color: #FCA5A5;
            }
            QPushButton#sidebarFailedButton[hasFailures="true"] {
                background: #2D1B1B;
                color: #FCA5A5;
            }
            QLabel#sidebarSession {
                color: #64748B;
                font-size: 10px;
                padding: 2px 4px;
            }
            QTabWidget#mainTabs {
                background: #0F172A;
            }
            QTabWidget#mainTabs::pane {
                border: none;
                background: #0F172A;
                margin-top: 0px;
            }
            QTabBar, QTabBar#mainAppTabBar {
                background: #0F172A;
            }
            QTabBar#mainAppTabBar {
                border: none;
            }
            QTabBar#mainAppTabBar::tab {
                background: #111827;
                color: #94A3B8;
                border: none;
                border-bottom: 2px solid transparent;
                padding: 10px 22px;
                margin-right: 4px;
                font-weight: 700;
                font-size: 13px;
            }
            QTabBar#mainAppTabBar::tab:selected {
                color: #3B82F6;
                background: #0F172A;
                border-bottom: 2px solid #3B82F6;
            }
            QTabBar#mainAppTabBar::tab:hover:!selected {
                color: #CBD5E1;
                background: #131C30;
            }
            QScrollArea, QScrollArea > QWidget > QWidget,
            QScrollArea#dashboardScroll, QWidget#dashboardContent {
                background: #0F172A;
                border: none;
            }
            QTabWidget::pane {
                border: 1px solid #334155;
                border-radius: 10px;
                background: #1E293B;
                padding: 0px;
                margin-top: -1px;
            }
            QTabBar::tab {
                padding: 10px 22px;
                font-weight: 700;
                font-size: 13px;
                color: #94A3B8;
                background: transparent;
                border: none;
                border-bottom: 2px solid transparent;
                margin-right: 4px;
            }
            QTabBar::tab:selected {
                color: #3B82F6;
                border-bottom: 2px solid #3B82F6;
            }
            QTabBar::tab:hover:!selected {
                color: #CBD5E1;
                background: #131C30;
            }
            QFrame#heroCard, QFrame#dashboardTopBar {
                background: #1E293B;
                border: 1px solid #334155;
                border-radius: 14px;
            }
            QLabel#heroTitle {
                color: #F8FAFC;
                font-size: 17px;
                font-weight: 800;
                letter-spacing: -0.4px;
            }
            QLabel#heroSubtitle {
                color: #94A3B8;
                font-size: 11px;
            }
            QLabel#metaBadge {
                background: #172033;
                color: #E2E8F0;
                border: 1px solid #334155;
                border-radius: 10px;
                padding: 7px 12px;
                font-weight: 700;
            }
            QFrame#accountOverviewCard, QFrame#statCard {
                background: #1E293B;
                border: 1px solid #334155;
                border-radius: 14px;
            }
            QFrame#statCard:hover {
                border: 1px solid #3B82F6;
                background: #243247;
            }
            QScrollArea#liveGridScroll, QWidget#liveGridContainer {
                background: transparent;
                border: none;
            }
            QFrame#liveJobCard {
                background: #1E293B;
                border: 1px solid #334155;
                border-radius: 12px;
            }
            QFrame#liveJobCard:hover {
                border-color: #3B82F6;
                background: #243247;
            }
            QLabel#liveJobNumber {
                color: #94A3B8;
                font-size: 11px;
                font-weight: 700;
            }
            QLabel#liveJobPrompt {
                color: #E2E8F0;
                font-size: 12px;
                font-weight: 600;
            }
            QLabel#liveJobMeta {
                color: #64748B;
                font-size: 11px;
            }
            QProgressBar#liveOverallProgress {
                background: #1E293B;
                border: 1px solid #334155;
                border-radius: 8px;
                text-align: center;
                color: #F8FAFC;
                font-weight: 700;
                padding: 1px;
            }
            QProgressBar#liveOverallProgress::chunk {
                background: #3B82F6;
                border-radius: 7px;
            }
            QLabel#accountOverviewTitle {
                color: #F8FAFC;
                font-size: 16px;
                font-weight: 700;
            }
            QLabel#accountMetricChip {
                background: #172033;
                border: 1px solid #334155;
                border-radius: 10px;
                color: #CBD5E1;
                font-weight: 700;
                padding: 7px 10px;
            }
            QLabel#statValue {
                background: transparent;
                border: none;
            }
            QLabel#statTitle {
                background: transparent;
                border: none;
            }
            QGroupBox, QGroupBox#dashboardPanel, QFrame#dashboardPanel {
                font-weight: 700;
                font-size: 14px;
                color: #F8FAFC;
                border: 1px solid #334155;
                border-radius: 14px;
                margin-top: 10px;
                padding: 18px 14px 14px 14px;
                background: #1E293B;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 16px;
                padding: 0 8px;
                color: #F8FAFC;
            }
            QLabel {
                color: #94A3B8;
                font-size: 13px;
                background: transparent;
            }
            QLabel#settingLabel {
                color: #94A3B8;
                font-size: 13px;
                font-weight: 600;
                padding-right: 6px;
            }
            QLabel#tabSectionTitle {
                color: #F8FAFC;
                font-size: 15px;
                font-weight: 700;
                padding-bottom: 6px;
                border-bottom: 1px solid #334155;
                margin-bottom: 8px;
            }
            QLabel#settingHint {
                color: #64748B;
                font-size: 12px;
                font-weight: 500;
            }
            QLineEdit, QTextEdit, QSpinBox, QTableWidget {
                background: #1E293B;
                color: #F8FAFC;
                border: 1px solid #334155;
                border-radius: 8px;
                selection-background-color: #1E2D4A;
                selection-color: #FFFFFF;
            }
            QLineEdit:focus, QTextEdit:focus, QSpinBox:focus {
                border-color: #3B82F6;
            }
            QComboBox, QComboBox#settingInput, QSpinBox#settingInput {
                padding: 8px 12px;
                border: 1px solid #334155;
                border-radius: 6px;
                background: #1E293B;
                color: #F8FAFC;
                font-size: 13px;
                min-height: 20px;
                min-width: 150px;
            }
            QComboBox:hover, QSpinBox#settingInput:hover {
                border-color: #3B82F6;
            }
            QComboBox:focus, QSpinBox#settingInput:focus {
                border-color: #3B82F6;
            }
            QComboBox::drop-down {
                border: none;
                width: 24px;
            }
            QComboBox::down-arrow {
                image: none;
                border-left: 4px solid transparent;
                border-right: 4px solid transparent;
                border-top: 5px solid #94A3B8;
                margin-right: 8px;
            }
            QComboBox QAbstractItemView {
                background: #1E293B;
                color: #F8FAFC;
                border: 1px solid #334155;
                border-radius: 6px;
                selection-background-color: #3B82F6;
                selection-color: white;
                padding: 4px;
            }
            QPushButton {
                padding: 8px 16px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 13px;
                border: 1px solid transparent;
                background: #1E293B;
                color: #F8FAFC;
            }
            QPushButton:disabled {
                color: #64748B;
                border-color: #334155;
                background: #253246;
            }
            QPushButton[role="primary"], QPushButton[role="primaryGradient"], QPushButton[role="startGradient"] {
                color: white;
                border: none;
                background: #3B82F6;
                padding: 12px 24px;
                font-size: 14px;
                font-weight: 700;
            }
            QPushButton[role="primary"]:hover, QPushButton[role="primaryGradient"]:hover, QPushButton[role="startGradient"]:hover {
                background: #2563EB;
            }
            QPushButton[role="outlineWarning"], QPushButton[role="warning"] {
                background: transparent;
                color: #F59E0B;
                border: 1px solid #F59E0B;
                padding: 12px 20px;
            }
            QPushButton[role="outlineWarning"]:hover, QPushButton[role="warning"]:hover {
                background: #1C1E2A;
            }
            QPushButton[role="outlineInfo"] {
                background: transparent;
                color: #3B82F6;
                border: 1px solid #3B82F6;
                padding: 12px 20px;
            }
            QPushButton[role="outlineInfo"]:hover {
                background: #172240;
            }
            QPushButton[role="outlineDanger"], QPushButton[role="subtleDanger"] {
                background: transparent;
                color: #EF4444;
                border: 1px solid #EF4444;
                padding: 12px 20px;
            }
            QPushButton[role="outlineDanger"]:hover, QPushButton[role="subtleDanger"]:hover {
                background: #1F1A2A;
            }
            QPushButton[role="ghost"], QPushButton[role="secondary"] {
                background: transparent;
                color: #94A3B8;
                border: 1px solid #475569;
            }
            QPushButton[role="ghost"]:hover, QPushButton[role="secondary"]:hover {
                background: #334155;
                color: #F8FAFC;
            }
            QPushButton[role="browse"] {
                background: #172240;
                color: #60A5FA;
                border: 1px dashed #3B82F6;
                border-radius: 8px;
                padding: 8px 16px;
                font-weight: 600;
            }
            QPushButton[role="browse"]:hover {
                background: #1A2744;
                border-style: solid;
            }
            QPushButton[role="danger"], QPushButton#refClearButton {
                background: #1F1A2A;
                color: #EF4444;
                border: 1px solid #EF4444;
                border-radius: 6px;
                font-weight: 700;
            }
            QPushButton[role="danger"]:hover, QPushButton#refClearButton:hover {
                background: #2A1D2C;
            }
            QPushButton#refClearButton {
                min-width: 34px;
                max-width: 34px;
                padding: 0px;
            }
            QLabel#refStatusLabel {
                background: #131C30;
                border: 1px solid #3B82F6;
                border-radius: 10px;
                padding: 7px 12px;
                color: #93C5FD;
                font-size: 13px;
                font-weight: 700;
                min-height: 24px;
            }
            QFrame#referenceRow, QFrame#bulkPanel {
                background: #172033;
                border: 1px solid #334155;
                border-radius: 12px;
            }
            QFrame#tabSeparator {
                background: #334155;
                border: none;
                max-height: 1px;
            }
            QTextEdit#logsOutput {
                background: #0B0E14;
                color: #22C55E;
                font-family: "SF Mono", "JetBrains Mono", "Fira Code", "Menlo", "Consolas", monospace;
                font-size: 12px;
                border: 1px solid #1E293B;
                border-radius: 10px;
                padding: 10px;
            }
            QTableWidget {
                background: #1E293B;
                color: #F8FAFC;
                border: 1px solid #334155;
                border-radius: 8px;
                gridline-color: #334155;
                selection-background-color: #1E2D4A;
                alternate-background-color: #243247;
            }
            QTableWidget::item {
                padding: 6px 8px;
                border-bottom: 1px solid #1E293B;
            }
            QHeaderView::section {
                background: #0F172A;
                color: #94A3B8;
                font-weight: 700;
                font-size: 11px;
                padding: 8px;
                border: none;
                border-bottom: 1px solid #334155;
            }
            QSplitter::handle {
                background: #1E293B;
                border-radius: 3px;
                border: 1px solid #334155;
            }
            QScrollBar:vertical {
                background: transparent;
                width: 8px;
                border-radius: 4px;
            }
            QScrollBar::handle:vertical {
                background: #475569;
                border-radius: 4px;
                min-height: 30px;
            }
            QScrollBar::handle:vertical:hover {
                background: #64748B;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
            QMenu {
                background: #1E293B;
                color: #F8FAFC;
                border: 1px solid #334155;
                border-radius: 8px;
                padding: 6px;
            }
            QMenu::item {
                padding: 8px 12px;
                border-radius: 6px;
            }
            QMenu::item:selected {
                background: #1A2744;
            }
            """
        )

    def _set_status_item_style(self, item, status_text):
        status = str(status_text or "").strip().lower()
        font = item.font()
        font.setBold(True)
        item.setFont(font)
        if status == "completed":
            item.setForeground(QColor("#22C55E"))
            item.setBackground(QColor(10, 31, 20))
        elif status == "running":
            item.setForeground(QColor("#3B82F6"))
            item.setBackground(QColor(11, 25, 47))
        elif status == "moderated":
            item.setForeground(QColor("#F59E0B"))
            item.setBackground(QColor(39, 24, 5))
        elif status == "failed":
            item.setForeground(QColor("#EF4444"))
            item.setBackground(QColor(44, 14, 14))
        elif status == "pending":
            item.setForeground(QColor("#94A3B8"))
            item.setBackground(QColor(22, 31, 47))
        else:
            item.setForeground(QColor("#94A3B8"))
            item.setBackground(QColor(30, 41, 59))

    def _set_account_runtime_item_style(self, item, runtime_text):
        runtime = str(runtime_text or "").strip().lower()
        if runtime == "running":
            item.setForeground(QColor("#22C55E"))
            item.setBackground(QColor(10, 31, 20))
        elif runtime == "cooldown":
            item.setForeground(QColor("#EF4444"))
            item.setBackground(QColor(44, 14, 14))
        elif runtime == "slot_cooldown":
            item.setForeground(QColor("#F59E0B"))
            item.setBackground(QColor(39, 24, 5))
        elif runtime == "ready":
            item.setForeground(QColor("#3B82F6"))
            item.setBackground(QColor(11, 25, 47))
        else:
            item.setForeground(QColor("#94A3B8"))
            item.setBackground(QColor(30, 41, 59))

    def _format_remaining(self, seconds):
        total = max(0, int(seconds))
        hours, rem = divmod(total, 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    def _get_or_create_table_item(self, table, row, col, text=None):
        item = table.item(row, col)
        if item is None:
            item = QTableWidgetItem("" if text is None else str(text))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            table.setItem(row, col, item)
        elif text is not None:
            item.setText(str(text))
        return item

    def _normalize_login_status(self, status_data):
        status = dict(status_data or {})
        state = str(status.get("state") or "").strip().lower()
        if not state:
            if status.get("logged_in"):
                state = "logged_in"
            elif status:
                state = "logged_out"
            else:
                state = "unknown"
        status["state"] = state
        status["logged_in"] = bool(status.get("logged_in")) if state != "logged_out" else False
        return status

    def _resolve_account_session_dir(self, account):
        account = dict(account or {})
        session_path = Path(str(account.get("session_path") or "").strip()).expanduser()
        if session_path.is_dir():
            return session_path

        account_name = str(account.get("name") or "").strip()
        if account_name:
            fallback = get_sessions_dir() / account_name
            if fallback.is_dir():
                return fallback
        return session_path

    def _get_login_status(self, account):
        import time as _time

        session_dir = self._resolve_account_session_dir(account)
        if not session_dir.exists():
            return False, "❌ Logged Out", ""

        # Check if runtime auth status override exists (from generation errors)
        acc_name = str(account.get("name") or "").strip()
        runtime_status = getattr(self, "_runtime_auth_status", {}).get(acc_name)
        if runtime_status == "expired":
            return False, "⚠ Session Expired", "Re-login required — generation auth failed"

        # Check exported_cookies.json for auth cookie validity
        cookies_json = session_dir / "exported_cookies.json"
        if cookies_json.exists():
            try:
                import json as _json
                with open(str(cookies_json), "r", encoding="utf-8") as f:
                    cookies = _json.load(f)
                auth_names = {"SID", "SSID", "HSID", "SAPISID", "__Secure-1PSID"}
                auth_cookies = [c for c in cookies if c.get("name") in auth_names]
                if auth_cookies:
                    now = _time.time()
                    expired = [
                        c for c in auth_cookies
                        if c.get("expires", 0) > 0 and c["expires"] < now
                    ]
                    if len(expired) >= 2:
                        return False, "⚠ Cookies Expired", f"{len(expired)} auth cookies expired"
                    return True, "✅ Logged In", f"{len(auth_cookies)} auth cookies"
            except Exception:
                pass

        # Fallback: check session files
        local_storage_dir = session_dir / "Default" / "Local Storage"
        if local_storage_dir.exists():
            try:
                if local_storage_dir.is_dir() and any(local_storage_dir.iterdir()):
                    return True, "✅ Logged In", str(local_storage_dir)
            except Exception:
                pass

        cookie_candidates = [
            session_dir / "Default" / "Network" / "Cookies",
            session_dir / "Default" / "Cookies",
        ]
        for candidate in cookie_candidates:
            if candidate.exists():
                return True, "✅ Logged In", str(candidate)

        return False, "❌ Logged Out", ""

    def _check_login_status(self, account):
        is_logged_in, _, _ = self._get_login_status(account)
        return "logged_in" if is_logged_in else "logged_out"

    def _warmup_progress_stylesheet(self, chunk_color="#F59E0B"):
        return (
            "QProgressBar {"
            " border: 1px solid #334155;"
            " border-radius: 3px;"
            " background-color: #1E293B;"
            " text-align: center;"
            " color: white;"
            " font-size: 11px;"
            "} "
            f"QProgressBar::chunk {{ background-color: {chunk_color}; border-radius: 2px; }}"
        )

    def _build_warmup_progress(self):
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(6)

        progress_bar = QProgressBar()
        progress_bar.setRange(0, 100)
        progress_bar.setValue(0)
        progress_bar.setFixedHeight(16)
        progress_bar.setFixedWidth(120)
        progress_bar.setStyleSheet(self._warmup_progress_stylesheet())

        status_label = QLabel("")
        status_label.setStyleSheet("color: #F59E0B; font-size: 11px;")

        layout.addWidget(progress_bar)
        layout.addWidget(status_label, 1)
        widget.setVisible(False)
        return widget, progress_bar, status_label

    def _set_account_detail_cell(self, row, account_name, detail_text):
        detail_label = QLabel(str(detail_text or ""))
        detail_label.setStyleSheet("color: #CBD5E1; padding: 2px 4px;")
        detail_label.setWordWrap(True)
        detail_label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)

        warmup_widget, warmup_bar, warmup_status = self._build_warmup_progress()
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(2, 0, 2, 0)
        layout.setSpacing(0)
        layout.addWidget(detail_label)
        layout.addWidget(warmup_widget)
        self.acc_table.setCellWidget(row, 8, container)
        self.warmup_widgets[account_name] = {
            "widget": warmup_widget,
            "progress_bar": warmup_bar,
            "status_label": warmup_status,
            "detail_label": detail_label,
            "row": row,
        }
        if account_name in self.active_warmup_progress:
            self._apply_warmup_state(account_name)

    def _set_account_detail_text(self, account_name, detail_text, row=None):
        warmup = self.warmup_widgets.get(account_name)
        if warmup:
            warmup["detail_label"].setText(str(detail_text or ""))
            if row is not None:
                warmup["row"] = row
            return
        if row is not None:
            self._get_or_create_table_item(self.acc_table, row, 8, detail_text)

    def _apply_warmup_state(self, account_name):
        warmup = self.warmup_widgets.get(account_name)
        state = self.active_warmup_progress.get(account_name)
        if not warmup or not state:
            return

        warmup["widget"].setVisible(True)
        warmup["detail_label"].setVisible(False)
        warmup["progress_bar"].setValue(int(state.get("percent", 0) or 0))
        warmup["status_label"].setText(str(state.get("status") or ""))
        success = state.get("success")
        if success is True:
            warmup["progress_bar"].setStyleSheet(self._warmup_progress_stylesheet("#22C55E"))
            warmup["status_label"].setStyleSheet("color: #22C55E; font-size: 11px;")
        elif success is False:
            warmup["progress_bar"].setStyleSheet(self._warmup_progress_stylesheet("#EF4444"))
            warmup["status_label"].setStyleSheet("color: #EF4444; font-size: 11px;")
        else:
            warmup["progress_bar"].setStyleSheet(self._warmup_progress_stylesheet("#F59E0B"))
            warmup["status_label"].setStyleSheet("color: #F59E0B; font-size: 11px;")

    def _on_warmup_progress(self, account_name, percent, status_text):
        account_key = str(account_name or "").strip()
        if not account_key:
            return
        self.active_warmup_progress[account_key] = {
            "percent": max(0, min(100, int(percent or 0))),
            "status": str(status_text or ""),
            "success": None,
        }
        self._apply_warmup_state(account_key)

    def _on_warmup_complete(self, account_name, success, message):
        account_key = str(account_name or "").strip()
        if not account_key:
            return
        self.active_warmup_progress[account_key] = {
            "percent": 100 if success else max(0, int(self.active_warmup_progress.get(account_key, {}).get("percent", 0))),
            "status": str(message or ""),
            "success": bool(success),
        }
        self._apply_warmup_state(account_key)
        QTimer.singleShot(3000, lambda name=account_key: self._restore_normal_detail(name))

    def _restore_normal_detail(self, account_name):
        warmup = self.warmup_widgets.get(account_name)
        if not warmup:
            self.active_warmup_progress.pop(account_name, None)
            return

        warmup["widget"].setVisible(False)
        warmup["detail_label"].setVisible(True)
        warmup["progress_bar"].setValue(0)
        warmup["progress_bar"].setStyleSheet(self._warmup_progress_stylesheet("#F59E0B"))
        warmup["status_label"].setText("")
        warmup["status_label"].setStyleSheet("color: #F59E0B; font-size: 11px;")
        self.active_warmup_progress.pop(account_name, None)

    def _clear_warmup_tracking(self, account_name):
        account_key = str(account_name or "").strip()
        if not account_key:
            return
        self.active_warmup_progress.pop(account_key, None)
        self.warmup_widgets.pop(account_key, None)

    def _set_account_saved_status_cell(self, row, account):
        if not hasattr(self, "acc_table"):
            return

        session_dir = self._resolve_account_session_dir(account)
        label = QLabel()
        label.setAlignment(Qt.AlignCenter)
        if session_dir.exists():
            label.setText("Saved")
            label.setStyleSheet("color: #3B82F6; font-weight: 600; padding: 4px 8px;")
            label.setToolTip(str(session_dir))
        else:
            label.setText("-")
            label.setStyleSheet("color: #64748B; padding: 4px 8px;")
        self.acc_table.setCellWidget(row, 4, label)

    def _set_account_login_status_cell(self, row, account_id, status_data=None):
        if not hasattr(self, "acc_table"):
            return

        status = self._normalize_login_status(status_data)
        state = status.get("state", "unknown")

        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(6)

        label = QLabel()
        detail_tip = ""
        if state == "logged_in":
            label.setText("✅ Logged In")
            label.setStyleSheet("color: #0f9d58; font-weight: 700;")
            email = str(status.get("email") or "").strip()
            expires = str(status.get("expires") or "").strip()
            detail_bits = [bit for bit in (email, expires) if bit]
            detail_tip = "\n".join(detail_bits)
        elif state == "checking":
            label.setText("⏳ Checking...")
            label.setStyleSheet("color: #64748b; font-style: italic;")
        elif state == "logging_in":
            label.setText("🔄 Logging in...")
            label.setStyleSheet("color: #1a73e8; font-style: italic;")
        elif state == "logged_out":
            label.setText("❌ Logged Out")
            label.setStyleSheet("color: #d93025; font-weight: 700;")
            detail_tip = str(status.get("error") or "").strip()
        else:
            label.setText("— Not Checked")
            label.setStyleSheet("color: #64748b;")

        if detail_tip:
            label.setToolTip(detail_tip)
            widget.setToolTip(detail_tip)
        layout.addWidget(label)

        if state == "logged_out":
            btn_relogin = QPushButton("Re-Login")
            btn_relogin.setFixedWidth(78)
            btn_relogin.setProperty("role", "warning")
            btn_relogin.setToolTip(detail_tip or "Delete the expired session and sign in again.")
            btn_relogin.clicked.connect(lambda _=False, account_id=account_id: self._relogin_account(account_id))
            layout.addWidget(btn_relogin)

        layout.addStretch()
        self.acc_table.setCellWidget(row, 3, widget)

    def _set_account_login_status_cell(self, row, account_id, status_data=None):
        if not hasattr(self, "acc_table"):
            return

        status = self._normalize_login_status(status_data)
        state = status.get("state", "unknown")
        account = self._account_record_by_id(account_id) or {}
        is_logged_in, status_text, detail_tip = self._get_login_status(account)

        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(4)

        label = QLabel()
        if state == "logging_in":
            label.setText("🔄 Logging in...")
            label.setStyleSheet("color: #3B82F6; font-weight: 600; padding: 4px 8px;")
        elif state == "checking":
            label.setText("⏳ Checking...")
            label.setStyleSheet("color: #94A3B8; font-weight: 600; padding: 4px 8px;")
        elif is_logged_in:
            label.setText(status_text)
            label.setStyleSheet("color: #22C55E; font-weight: 600; padding: 4px 8px;")
            email = str(status.get("email") or "").strip()
            expires = str(status.get("expires") or "").strip()
            detail_bits = [bit for bit in (detail_tip, email, expires) if bit]
            detail_tip = "\n".join(detail_bits)
        else:
            label.setText(status_text)
            label.setStyleSheet("color: #EF4444; font-weight: 600; padding: 4px 8px;")
            error_text = str(status.get("error") or "").strip()
            detail_tip = "\n".join(bit for bit in (detail_tip, error_text) if bit)

        if detail_tip:
            label.setToolTip(detail_tip)
            widget.setToolTip(detail_tip)
        layout.addWidget(label)
        layout.addStretch()
        self.acc_table.setCellWidget(row, 3, widget)

    def _refresh_login_statuses(self):
        if not hasattr(self, "acc_table"):
            return

        for row in range(self.acc_table.rowCount()):
            id_item = self.acc_table.item(row, 0)
            if not id_item:
                continue
            account_id = int(id_item.data(Qt.UserRole) or 0)
            self._set_account_login_status_cell(row, account_id, self.account_login_state.get(account_id))
            account = self._account_record_by_id(account_id) or {}
            self._set_account_saved_status_cell(row, account)

        self._refresh_account_overview()

    def _find_account_row(self, account_id):
        target_id = int(account_id or 0)
        for row in range(self.acc_table.rowCount()):
            id_item = self.acc_table.item(row, 0)
            if id_item and int(id_item.data(Qt.UserRole) or 0) == target_id:
                return row
        return -1

    def _account_record_by_id(self, account_id):
        target_id = int(account_id or 0)
        for account in list(self._latest_accounts or []):
            if int(account.get("id") or 0) == target_id:
                return account
        return None

    def _sanitize_account_clone_prefix(self, account_name):
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", str(account_name or "account")).strip("._-")
        return safe or "account"

    def _make_account_action_button(self, text, color, hover_color):
        btn = QPushButton(text)
        btn.setFixedSize(60, 26)
        btn.setStyleSheet(
            f"""
            QPushButton {{
                background: transparent;
                color: {color};
                font-size: 11px;
                font-weight: 600;
                border: 1px solid {color};
                border-radius: 4px;
            }}
            QPushButton:hover {{
                background: {hover_color};
                color: white;
            }}
            """
        )
        return btn

    def _add_account_action_buttons(self, row, account_id, account_name):
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(4)

        btn_reset = self._make_account_action_button("Reset", "#F59E0B", "#92400E")
        btn_reset.setToolTip("Delete saved session and open a fresh login browser.")
        btn_reset.clicked.connect(
            lambda _=False, target_id=account_id: self._reset_account_session(target_id)
        )
        layout.addWidget(btn_reset)

        btn_delete = self._make_account_action_button("Delete", "#EF4444", "#991B1B")
        btn_delete.setToolTip(f"Remove '{account_name}' from Account Manager.")
        btn_delete.clicked.connect(
            lambda _=False, target_id=account_id: self._delete_account_by_id(target_id)
        )
        layout.addWidget(btn_delete)

        layout.addStretch()
        self.acc_table.setCellWidget(row, 9, widget)

    def _proxy_status_text(self, proxy_value):
        proxy_text = str(proxy_value or "").strip()
        if not proxy_text:
            return "Direct"
        parsed = urlparse(proxy_text)
        if parsed.hostname and parsed.port:
            return f"{parsed.hostname}:{parsed.port}"
        return proxy_text

    def _set_account_proxy_cell(self, row, account_id, account_name, proxy_value):
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(8)

        label = QLabel()
        proxy_text = str(proxy_value or "").strip()
        if proxy_text:
            label.setText(f"\u25cf {self._proxy_status_text(proxy_text)}")
            label.setStyleSheet("color: #22C55E; font-weight: 600;")
            label.setToolTip(proxy_text)
        else:
            label.setText("Direct")
            label.setStyleSheet("color: #94A3B8; font-weight: 600;")

        btn_proxy = QPushButton("\u2699 Proxy")
        btn_proxy.setMinimumWidth(90)
        btn_proxy.setProperty("role", "secondary")
        btn_proxy.clicked.connect(
            lambda _=False, target_id=account_id: self._open_proxy_dialog(target_id)
        )

        layout.addWidget(label, 1)
        layout.addWidget(btn_proxy)
        self.acc_table.setCellWidget(row, 2, widget)

    def _open_proxy_dialog(self, account_id):
        account = self._account_record_by_id(account_id)
        if not account:
            QMessageBox.warning(self, "Account Missing", "Could not find that account in the database.")
            return

        account_name = str(account.get("name") or "").strip()
        current_proxy = str(account.get("proxy") or "").strip()
        dialog = ProxyConfigDialog(account_name, current_proxy, self)
        if dialog.exec() != QDialog.Accepted:
            return

        proxy_value = dialog.get_proxy_url()
        self._start_background_task(
            update_account_proxy_by_id,
            int(account_id),
            proxy_value,
            on_finished=lambda _result, account_id=account_id, proxy_value=proxy_value: self._on_account_proxy_saved(
                account_id,
                proxy_value,
            ),
        )

    def _delete_account_session_artifacts(self, account):
        account_name = str(account.get("name") or "").strip()
        session_path = Path(str(account.get("session_path") or "").strip()).expanduser()
        removed_paths = []

        if session_path.is_dir():
            shutil.rmtree(session_path, ignore_errors=True)
            removed_paths.append(str(session_path))

        clone_root = get_session_clones_dir()
        clone_prefixes = {
            self._sanitize_account_clone_prefix(account_name),
            self._sanitize_account_clone_prefix(session_path.name),
        }
        if clone_root.is_dir():
            for child in clone_root.iterdir():
                if not child.is_dir():
                    continue
                if any(prefix and child.name.startswith(prefix) for prefix in clone_prefixes):
                    shutil.rmtree(child, ignore_errors=True)
                    removed_paths.append(str(child))

        return removed_paths

    def _start_account_session_refresh(self, account_id, action_label="Re-login"):
        account = self._account_record_by_id(account_id)
        if not account:
            QMessageBox.warning(self, "Account Missing", "Could not find that account in the database.")
            return False

        account_name = str(account.get("name") or "").strip()
        runtime = self.account_runtime_state.get(account_name, {})
        active_slots = int(runtime.get("active_slots", 0) or 0)
        if active_slots > 0:
            QMessageBox.warning(
                self,
                "Account Busy",
                f"'{account_name}' currently has {active_slots} active slot(s). Stop the queue or wait until it is idle before {action_label.lower()}.",
            )
            return False

        if self.relogin_worker and self.relogin_worker.isRunning():
            QMessageBox.information(
                self,
                "Login Already Running",
                "Another account login browser is already open. Finish that first, then try again.",
            )
            return False

        removed_paths = self._delete_account_session_artifacts(account)
        self.account_login_state[int(account_id)] = {"state": "logging_in"}
        row = self._find_account_row(account_id)
        if row >= 0:
            self._set_account_login_status_cell(row, account_id, self.account_login_state[int(account_id)])

        if removed_paths:
            self.append_log(
                f"[ACCOUNTS] {action_label} requested for {account_name}. Cleared {len(removed_paths)} session item(s)."
            )
        else:
            self.append_log(
                f"[ACCOUNTS] {action_label} requested for {account_name}. No prior session files found."
            )
        self.append_log(f"[ACCOUNTS] Opening fresh login browser for {account_name}...")

        worker = LoginWorker(account_name)
        worker.log_msg.connect(self.append_log, Qt.QueuedConnection)
        worker.warmup_progress.connect(self.warmup_progress_signal.emit, Qt.QueuedConnection)
        worker.warmup_complete.connect(self.warmup_complete_signal.emit, Qt.QueuedConnection)
        worker.finished_login.connect(
            lambda name, session_path, detected_email, target_id=account_id: self.on_relogin_finished(
                target_id, name, session_path, detected_email
            )
        )
        self.relogin_worker = worker
        worker.start()
        return True

    def _reset_account_session(self, account_id):
        account = self._account_record_by_id(account_id)
        if not account:
            QMessageBox.warning(self, "Account Missing", "Could not find that account in the database.")
            return

        account_name = str(account.get("name") or "").strip()
        reply = QMessageBox.question(
            self,
            "Reset Session",
            f"Reset session for '{account_name}'?\n\nThis deletes saved cookies/session clones and opens a fresh login browser.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self._start_account_session_refresh(account_id, action_label="Session reset")

    def _delete_account_by_id(self, account_id):
        account = self._account_record_by_id(account_id)
        if not account:
            QMessageBox.warning(self, "Account Missing", "Could not find that account in the database.")
            return

        account_name = str(account.get("name") or "").strip()
        self._start_background_task(
            self._delete_account_record,
            account_id,
            account_name,
            on_finished=lambda _result, account_id=account_id, account_name=account_name: self._on_account_deleted(
                account_id,
                account_name,
            ),
        )

    @staticmethod
    def _clear_account_project_cache_artifacts(account_name):
        normalized_name = str(account_name or "").strip()
        if not normalized_name:
            return

        try:
            GoogleLabsBot.clear_account_project_cache(normalized_name)
        except Exception:
            pass

        cache_file = get_project_cache_path()
        if not cache_file.exists():
            return

        try:
            with cache_file.open("r", encoding="utf-8") as handle:
                cache_data = json.load(handle)
            if isinstance(cache_data, dict):
                cache_data.pop(normalized_name, None)
                with cache_file.open("w", encoding="utf-8") as handle:
                    json.dump(cache_data, handle, indent=2, sort_keys=True)
            else:
                cache_file.unlink(missing_ok=True)
        except Exception:
            try:
                cache_file.unlink(missing_ok=True)
            except Exception:
                pass

    @staticmethod
    def _delete_account_record(account_id, account_name):
        # Reassign any pending/running jobs from this account before deleting
        try:
            from src.db.db_manager import reassign_account_jobs
            reassigned = reassign_account_jobs(account_name)
            if reassigned > 0:
                pass  # Logged in _on_account_deleted
        except Exception:
            reassigned = 0
        MainWindow._clear_account_project_cache_artifacts(account_name)
        clear_account_flags(account_name)
        if int(account_id or 0) > 0:
            remove_account_by_id(int(account_id))
        else:
            remove_account(account_name)
        return reassigned

    def _on_account_deleted(self, account_id, account_name):
        self.account_login_state.pop(int(account_id or 0), None)
        self._runtime_auth_status.pop(account_name, None)
        self._clear_warmup_tracking(account_name)
        self.append_log(f"Deleted account '{account_name}'. Pending jobs reassigned to other accounts.")
        self.refresh_accounts()

    def _sanitize_account_clone_prefix(self, account_name):
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", str(account_name or "account")).strip("._-")
        return safe or "account"

    def _add_account_action_buttons(self, row, account_id, account_name):
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(4)

        btn_reset = self._make_account_action_button("Reset", "#F59E0B", "#92400E")
        btn_reset.setToolTip("Delete saved session and open a fresh login browser.")
        btn_reset.clicked.connect(
            lambda _=False, target_id=account_id: self._reset_account_session(target_id)
        )
        layout.addWidget(btn_reset)

        btn_delete = self._make_account_action_button("Delete", "#EF4444", "#991B1B")
        btn_delete.setToolTip(f"Remove '{account_name}' from Account Manager.")
        btn_delete.clicked.connect(
            lambda _=False, target_id=account_id: self._delete_account_by_id(target_id)
        )
        layout.addWidget(btn_delete)

        layout.addStretch()
        self.acc_table.setCellWidget(row, 9, widget)

    def _delete_account_session_artifacts(self, account):
        account_name = str(account.get("name") or "").strip()
        session_path = Path(str(account.get("session_path") or "").strip()).expanduser()
        removed_paths = []

        if account_name:
            set_account_flag(account_name, "warmup_done", "False")

        if session_path and session_path.is_dir():
            shutil.rmtree(session_path, ignore_errors=True)
            removed_paths.append(str(session_path))

        clone_root = get_session_clones_dir()
        clone_prefixes = {
            self._sanitize_account_clone_prefix(account_name),
            self._sanitize_account_clone_prefix(session_path.name if session_path else ""),
        }
        if clone_root.is_dir():
            for child in clone_root.iterdir():
                if not child.is_dir():
                    continue
                child_name = child.name
                if any(prefix and child_name.startswith(prefix) for prefix in clone_prefixes):
                    shutil.rmtree(child, ignore_errors=True)
                    removed_paths.append(str(child))

        return removed_paths

    def _start_account_session_refresh(self, account_id, action_label="Re-login"):
        account = self._account_record_by_id(account_id)
        if not account:
            QMessageBox.warning(self, "Account Missing", "Could not find that account in the database.")
            return False

        account_name = str(account.get("name") or "").strip()
        runtime = self.account_runtime_state.get(account_name, {})
        active_slots = int(runtime.get("active_slots", 0) or 0)
        if active_slots > 0:
            QMessageBox.warning(
                self,
                "Account Busy",
                f"'{account_name}' currently has {active_slots} active slot(s). Stop the queue or wait until it is idle before {action_label.lower()}.",
            )
            return False

        if self.relogin_worker and self.relogin_worker.isRunning():
            QMessageBox.information(
                self,
                "Login Already Running",
                "Another account login window is already open. Finish that first, then try again.",
            )
            return False

        removed_paths = self._delete_account_session_artifacts(account)
        self.account_login_state[int(account_id)] = {"state": "logging_in"}
        row = self._find_account_row(account_id)
        if row >= 0:
            self._set_account_login_status_cell(row, account_id, self.account_login_state[int(account_id)])

        if removed_paths:
            self.append_log(
                f"[ACCOUNTS] {action_label} requested for {account_name}. Cleared {len(removed_paths)} session item(s)."
            )
        else:
            self.append_log(
                f"[ACCOUNTS] {action_label} requested for {account_name}. No prior session files found."
            )
        self.append_log(f"[ACCOUNTS] Opening fresh login browser for {account_name}...")

        worker = LoginWorker(account_name, proxy=account.get("proxy"))
        worker.log_msg.connect(self.append_log, Qt.QueuedConnection)
        worker.warmup_progress.connect(self.warmup_progress_signal.emit, Qt.QueuedConnection)
        worker.warmup_complete.connect(self.warmup_complete_signal.emit, Qt.QueuedConnection)
        worker.finished_login.connect(
            lambda name, session_path, detected_email, target_id=account_id: self.on_relogin_finished(
                target_id, name, session_path, detected_email
            )
        )
        self.relogin_worker = worker
        worker.start()
        return True

    def _reset_account_session(self, account_id):
        account = self._account_record_by_id(account_id)
        if not account:
            QMessageBox.warning(self, "Account Missing", "Could not find that account in the database.")
            return

        account_name = str(account.get("name") or "").strip()
        reply = QMessageBox.question(
            self,
            "Reset Session",
            f"Reset session for '{account_name}'?\n\nThis deletes saved cookies/session clones and opens a fresh login browser.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self._start_account_session_refresh(account_id, action_label="Session reset")

    def _delete_account_by_id(self, account_id):
        account = self._account_record_by_id(account_id)
        if not account:
            QMessageBox.warning(self, "Account Missing", "Could not find that account in the database.")
            return

        account_name = str(account.get("name") or "").strip()
        self._start_background_task(
            self._delete_account_record,
            account_id,
            account_name,
            on_finished=lambda _result, account_id=account_id, account_name=account_name: self._on_account_deleted(
                account_id,
                account_name,
            ),
        )

    def _on_account_table_item_changed(self, item):
        return

    def _on_account_proxy_saved(self, account_id, proxy_value):
        for account in self._latest_accounts or []:
            if int(account.get("id") or 0) == int(account_id):
                account["proxy"] = proxy_value
                account_name = str(account.get("name") or "").strip()
                if proxy_value:
                    self.append_log(f"[ACCOUNTS] Proxy updated for {account_name}.")
                else:
                    self.append_log(f"[ACCOUNTS] Proxy cleared for {account_name}.")
                if self.queue_manager and self.queue_manager.isRunning():
                    self.append_log("[ACCOUNTS] Restart queue to apply updated proxy settings.")
                break
        self.refresh_accounts()

    def _refresh_account_overview(self):
        if not hasattr(self, "acc_table"):
            return
        total = self.acc_table.rowCount()
        logged_in = 0
        logged_out = 0
        running = 0
        cooldown = 0
        ready = 0
        for row in range(total):
            id_item = self.acc_table.item(row, 0)
            account_id = int(id_item.data(Qt.UserRole) or 0) if id_item else 0
            account = self._account_record_by_id(account_id) or {}
            if self._check_login_status(account) == "logged_in":
                logged_in += 1
            else:
                logged_out += 1

            runtime = str(self._get_or_create_table_item(self.acc_table, row, 5).text() or "").lower()
            if runtime == "running":
                running += 1
            elif runtime in ("cooldown", "slot cooldown", "slot_cooldown"):
                cooldown += 1
            elif runtime == "ready":
                ready += 1

        if hasattr(self, "lbl_acc_total"):
            self.lbl_acc_total.setText(f"Total: {total}")
        if hasattr(self, "lbl_acc_logged_in"):
            self.lbl_acc_logged_in.setText(f"Logged In: {logged_in}")
        if hasattr(self, "lbl_acc_logged_out"):
            self.lbl_acc_logged_out.setText(f"Logged Out: {logged_out}")
        if hasattr(self, "lbl_acc_running"):
            self.lbl_acc_running.setText(f"Running: {running}")
        if hasattr(self, "lbl_acc_cooldown"):
            self.lbl_acc_cooldown.setText(f"Cooldown: {cooldown}")
        if hasattr(self, "lbl_acc_ready"):
            self.lbl_acc_ready.setText(f"Ready: {ready}")

    def _refresh_account_runtime_cells(self):
        if not hasattr(self, "acc_table"):
            return

        now = time.time()
        for row in range(self.acc_table.rowCount()):
            name_item = self._get_or_create_table_item(self.acc_table, row, 1)
            account_name = str(name_item.data(Qt.UserRole) or name_item.text() or "").strip()
            runtime = self.account_runtime_state.get(account_name, {})

            runtime_status = str(runtime.get("status", "idle")).strip().lower()
            runtime_display = {
                "cooldown": "cooldown",
                "slot_cooldown": "slot cooldown",
                "running": "running",
                "ready": "ready",
                "idle": "idle",
            }.get(runtime_status, runtime_status or "idle")
            cooldown_until = float(runtime.get("cooldown_until", 0.0) or 0.0)
            active_slots = int(runtime.get("active_slots", 0) or 0)
            total_slots = int(runtime.get("total_slots", 1) or 1)
            detail = str(runtime.get("detail", "Queue stopped" if not self.queue_manager or not self.queue_manager.isRunning() else "Ready"))

            remaining = max(0, int(cooldown_until - now))
            cooldown_text = self._format_remaining(remaining) if remaining > 0 else "--"
            slots_text = f"{active_slots}/{max(1, total_slots)}"

            runtime_item = self._get_or_create_table_item(self.acc_table, row, 5, runtime_display)
            self._set_account_runtime_item_style(runtime_item, runtime_status)

            cooldown_item = self._get_or_create_table_item(self.acc_table, row, 6, cooldown_text)
            if remaining > 0:
                cooldown_item.setForeground(QColor("#b4233a"))
            else:
                cooldown_item.setForeground(QColor("#64748b"))
                if runtime_status == "cooldown":
                    runtime_item.setText("ready")
                    self._set_account_runtime_item_style(runtime_item, "ready")

            self._get_or_create_table_item(self.acc_table, row, 7, slots_text)
            self._set_account_detail_text(account_name, detail, row=row)

        self._refresh_account_overview()

    def _on_account_runtime_tick(self):
        self._refresh_account_runtime_cells()

    def _on_tab_changed(self, index):
        if self.tabs.widget(index) is self.tab_accounts and not self._account_status_auto_check_done:
            self._account_status_auto_check_done = True
            QTimer.singleShot(350, self.refresh_accounts)
        elif self.tabs.widget(index) is self.tab_accounts:
            QTimer.singleShot(150, self._refresh_login_statuses)
        if self.tabs.widget(index) is self.tab_live_generation:
            self._live_tab_dirty = False
            QTimer.singleShot(100, self._refresh_live_grid)
            self._request_queue_snapshot()
        if self.tabs.widget(index) is self.tab_failed_jobs:
            QTimer.singleShot(100, lambda: self._request_failed_jobs_refresh(force=True))
        self._sync_sidebar_selection()

    def _on_sidebar_page_selected(self, page_key):
        key = str(page_key or "dashboard").strip().lower()
        if key == "dashboard":
            self.tabs.setCurrentWidget(self.tab_dashboard)
            if hasattr(self, "mode_tabs"):
                self.mode_tabs.setCurrentIndex(0)
            return
        if key == "video":
            self.tabs.setCurrentWidget(self.tab_dashboard)
            if hasattr(self, "mode_tabs"):
                target_index = 1 if self.mode_tabs.count() > 1 else 0
                self.mode_tabs.setCurrentIndex(target_index)
            return
        if key == "accounts":
            self.tabs.setCurrentWidget(self.tab_accounts)
            return
        if key == "live":
            self.tabs.setCurrentWidget(self.tab_live_generation)
            return
        if key == "failed":
            self.tabs.setCurrentWidget(self.tab_failed_jobs)
            return
        if key == "settings":
            self.tabs.setCurrentWidget(self.tab_settings)

    def _current_sidebar_key(self):
        current_widget = self.tabs.currentWidget() if hasattr(self, "tabs") else None
        if current_widget is self.tab_dashboard:
            if hasattr(self, "mode_tabs") and self.mode_tabs.currentIndex() > 0:
                return "video"
            return "dashboard"
        if current_widget is self.tab_accounts:
            return "accounts"
        if current_widget is self.tab_live_generation:
            return "live"
        if current_widget is self.tab_failed_jobs:
            return "failed"
        if current_widget is self.tab_settings:
            return "settings"
        return "dashboard"

    def _sync_sidebar_selection(self):
        if hasattr(self, "sidebar"):
            self.sidebar.set_active(self._current_sidebar_key())

    def _on_browser_mode_changed(self, _index):
        mode = self.cmb_browser_mode.currentData() or "headless"
        is_real_chrome = mode == "real_chrome"
        is_cloak = mode == "cloakbrowser"
        if hasattr(self, "lbl_chrome_display"):
            self.lbl_chrome_display.setVisible(is_real_chrome)
        if hasattr(self, "cmb_chrome_display"):
            self.cmb_chrome_display.setVisible(is_real_chrome)
        if hasattr(self, "lbl_cloak_display"):
            self.lbl_cloak_display.setVisible(is_cloak)
        if hasattr(self, "cmb_cloak_display"):
            self.cmb_cloak_display.setVisible(is_cloak)
        if hasattr(self, "cloak_update_widget"):
            self.cloak_update_widget.setVisible(is_cloak)
        if hasattr(self, "lbl_cloak_update_status"):
            self.lbl_cloak_update_status.setVisible(False)
        if is_cloak:
            self._refresh_cloak_version_display()
        if hasattr(self, "chk_random_fingerprint"):
            if is_cloak:
                self.chk_random_fingerprint.setChecked(False)
                self.chk_random_fingerprint.setEnabled(False)
                self.chk_random_fingerprint.setToolTip("CloakBrowser has built-in C++ fingerprinting")
            else:
                self.chk_random_fingerprint.setEnabled(True)
                self.chk_random_fingerprint.setToolTip("")

    def _refresh_cloak_version_display(self, reset_button=True):
        if not hasattr(self, "lbl_cloak_version") or not hasattr(self, "btn_cloak_update"):
            return
        try:
            import importlib

            importlib.invalidate_caches()
            cloakbrowser = importlib.import_module("cloakbrowser")
            binary_info = getattr(cloakbrowser, "binary_info", None)
            pkg_version = str(getattr(cloakbrowser, "__version__", "unknown"))

            try:
                info = binary_info() if callable(binary_info) else {}
                bin_version = str((info or {}).get("version") or "unknown")
                installed = bool((info or {}).get("installed"))
            except Exception:
                bin_version = "unknown"
                installed = False

            if installed:
                self.lbl_cloak_version.setText(f"CloakBrowser v{pkg_version} | Binary: {bin_version}")
                self.lbl_cloak_version.setStyleSheet("color: #22C55E; font-size: 12px;")
            else:
                self.lbl_cloak_version.setText(f"CloakBrowser v{pkg_version} | Binary: not downloaded")
                self.lbl_cloak_version.setStyleSheet("color: #F59E0B; font-size: 12px;")
            if reset_button:
                self.btn_cloak_update.setText("Check for Updates")
            return
        except ImportError:
            self.lbl_cloak_version.setText("CloakBrowser: not installed")
            self.lbl_cloak_version.setStyleSheet("color: #EF4444; font-size: 12px;")
            if reset_button:
                self.btn_cloak_update.setText("Install CloakBrowser")
            return
        except Exception as exc:
            self.lbl_cloak_version.setText(f"CloakBrowser: error ({str(exc)[:40]})")
            self.lbl_cloak_version.setStyleSheet("color: #EF4444; font-size: 12px;")
            if reset_button:
                self.btn_cloak_update.setText("Check for Updates")
            return
        try:
            import importlib

            importlib.invalidate_caches()
            cloakbrowser = importlib.import_module("cloakbrowser")
            binary_info = getattr(cloakbrowser, "binary_info", None)
            pkg_version = str(getattr(cloakbrowser, "__version__", "unknown"))

            try:
                info = binary_info() if callable(binary_info) else {}
                bin_version = str((info or {}).get("version") or "unknown")
                installed = bool((info or {}).get("installed"))
            except Exception:
                bin_version = "unknown"
                installed = False

            if installed:
                self.lbl_cloak_version.setText(f"🟢 CloakBrowser v{pkg_version} | Binary: {bin_version}")
                self.lbl_cloak_version.setStyleSheet("color: #22C55E; font-size: 12px;")
                self.lbl_cloak_version.setText(f"CloakBrowser v{pkg_version} | Binary: {bin_version}")
            else:
                self.lbl_cloak_version.setText(f"🟡 CloakBrowser v{pkg_version} | Binary: not downloaded")
                self.lbl_cloak_version.setStyleSheet("color: #F59E0B; font-size: 12px;")
                self.lbl_cloak_version.setText(f"CloakBrowser v{pkg_version} | Binary: not downloaded")
            if not reset_button:
                return
            self.btn_cloak_update.setText("🔄 Check for Updates")
        except ImportError:
            self.lbl_cloak_version.setText("🔴 CloakBrowser: not installed")
            self.lbl_cloak_version.setStyleSheet("color: #EF4444; font-size: 12px;")
            self.btn_cloak_update.setText("📦 Install CloakBrowser")
        except Exception as exc:
            self.lbl_cloak_version.setText(f"CloakBrowser: error ({str(exc)[:40]})")
            self.lbl_cloak_version.setStyleSheet("color: #EF4444; font-size: 12px;")

    def _auto_check_cloak_on_startup(self):
        self._refresh_cloak_version_display()

    def _on_cloak_update_clicked(self):
        if getattr(self, "_cloak_update_worker", None) is not None and self._cloak_update_worker.isRunning():
            return

        install_mode = False
        try:
            import importlib

            importlib.import_module("cloakbrowser")
            self.btn_cloak_update.setText("Check for Updates")
        except ImportError:
            install_mode = True

        self.btn_cloak_update.setEnabled(False)
        self.btn_cloak_update.setText("⏳ Updating...")
        self.lbl_cloak_update_status.setVisible(True)
        self.btn_cloak_update.setText("Updating...")
        self.lbl_cloak_update_status.setText("Starting update...")
        self.lbl_cloak_update_status.setStyleSheet("color: #60A5FA; font-size: 11px;")

        self._cloak_update_worker = CloakUpdateWorker(install_mode=install_mode, parent=self)
        self._cloak_update_worker.status_changed.connect(self._on_cloak_update_status)
        self._cloak_update_worker.finished.connect(self._on_cloak_update_finished)
        self._cloak_update_worker.start()

    def _on_cloak_update_status(self, message, color):
        message = str(message or "").encode("ascii", "ignore").decode().strip() or str(message or "")
        self.lbl_cloak_update_status.setText(str(message or ""))
        self.lbl_cloak_update_status.setStyleSheet(f"color: {color}; font-size: 11px;")
        self.lbl_cloak_update_status.setVisible(True)

    def _on_cloak_update_finished(self, success, message):
        message = str(message or "").encode("ascii", "ignore").decode().strip() or str(message or "")
        self.btn_cloak_update.setEnabled(True)
        self._refresh_cloak_version_display()
        if success:
            self.btn_cloak_update.setText("✅ Up to Date")
            self.lbl_cloak_update_status.setText(f"✅ {message}")
            self.lbl_cloak_update_status.setStyleSheet("color: #22C55E; font-size: 11px;")
            self.btn_cloak_update.setText("Up to Date")
            self.lbl_cloak_update_status.setText(f"Success: {message}")
        else:
            self.btn_cloak_update.setText("❌ Update Failed")
            self.lbl_cloak_update_status.setText(f"❌ {message}")
            self.lbl_cloak_update_status.setStyleSheet("color: #EF4444; font-size: 11px;")
            self.btn_cloak_update.setText("Update Failed")
            self.lbl_cloak_update_status.setText(f"Failed: {message}")

        QTimer.singleShot(5000, self._reset_cloak_update_button)
        QTimer.singleShot(10000, lambda: self.lbl_cloak_update_status.setVisible(False))
        self._cloak_update_worker = None

    def _reset_cloak_update_button(self):
        if not hasattr(self, "btn_cloak_update") or not self.btn_cloak_update.isEnabled():
            return
        try:
            import importlib

            importlib.import_module("cloakbrowser")
            self.btn_cloak_update.setText("Check for Updates")
        except ImportError:
            self.btn_cloak_update.setText("Install CloakBrowser")
        return
        try:
            import importlib

            importlib.import_module("cloakbrowser")
            self.btn_cloak_update.setText("🔄 Check for Updates")
        except ImportError:
            self.btn_cloak_update.setText("📦 Install CloakBrowser")

    def _on_mode_tab_changed(self, _index):
        self._adjust_mode_tabs_height()
        self._sync_sidebar_selection()
        self._scroll_active_mode_tab_to_top()
        if not getattr(self, "_pending_settings_sync_ready", False):
            return
        self._sync_generation_mode_ui()

    def _scroll_active_mode_tab_to_top(self):
        if not hasattr(self, "mode_tabs"):
            return
        scroll = self._mode_tab_scrolls.get(self.mode_tabs.currentWidget())
        if scroll is None:
            return
        QTimer.singleShot(0, lambda sb=scroll.verticalScrollBar(): sb.setValue(0))

    def _remove_stray_mode_tab_buttons(self):
        if not hasattr(self, "mode_tabs"):
            return
        for tab_widget in (
            getattr(self, "mode_tab_image", None),
            getattr(self, "mode_tab_t2v", None),
            getattr(self, "mode_tab_ref", None),
            getattr(self, "mode_tab_frames", None),
            getattr(self, "mode_tab_pipeline", None),
        ):
            if tab_widget is None:
                continue
            for button in tab_widget.findChildren(QPushButton):
                text = " ".join(str(button.text() or "").strip().split()).lower()
                if text not in {"start", "start automation"}:
                    continue
                if hasattr(self, "btn_start") and button is self.btn_start:
                    continue
                parent = button.parentWidget()
                if parent is not None and parent.layout() is not None:
                    parent.layout().removeWidget(button)
                button.hide()
                button.setParent(None)
                button.deleteLater()

    def _failed_stat_button_style(self, has_failures):
        if has_failures:
            return (
                "QPushButton { background-color: #2D1B1B; color: #EF4444; font-size: 36px; font-weight: 700; "
                "border: 2px solid #EF4444; border-radius: 8px; text-align: left; padding: 16px; } "
                "QPushButton:hover { background-color: #3D2020; }"
            )
        return (
            "QPushButton { background-color: #1E293B; color: #EF4444; font-size: 36px; font-weight: 700; "
            "border: 2px solid #1E293B; border-radius: 8px; text-align: left; padding: 16px; } "
            "QPushButton:hover { border: 2px solid #EF4444; background-color: #2D1B1B; }"
        )

    def _go_to_failed_tab(self):
        if not hasattr(self, "tabs"):
            return
        for index in range(self.tabs.count()):
            if self.tabs.tabText(index) == "Failed Jobs":
                self.tabs.setCurrentIndex(index)
                break

    def _update_session_stats_label(self, generated_count=None):
        if not hasattr(self, "lbl_session_stats"):
            return
        try:
            count = max(0, int(generated_count if generated_count is not None else 0))
        except Exception:
            count = 0
        self.lbl_session_stats.setText(f"Session: {count} images generated")
        if hasattr(self, "sidebar"):
            pending = int(self.stat_pending.text()) if hasattr(self, "stat_pending") else 0
            running = int(self.stat_running.text()) if hasattr(self, "stat_running") else 0
            done = int(self.stat_completed.text()) if hasattr(self, "stat_completed") else count
            failed = int(self.stat_failed.text()) if hasattr(self, "stat_failed") else 0
            self.sidebar.update_stats(pending, running, done, failed, count)

    def _note_terminal_job(self, job_id, status):
        job_key = str(job_id or "").strip()
        status_text = str(status or "").strip().lower()
        if not job_key:
            return
        if status_text in ("completed", "failed"):
            previous = self._terminal_job_states.get(job_key)
            if previous != status_text:
                self._completion_times.append(time.time())
                if len(self._completion_times) > 30:
                    self._completion_times = self._completion_times[-30:]
            self._terminal_job_states[job_key] = status_text
        else:
            self._terminal_job_states.pop(job_key, None)

    def _on_generation_started(self):
        self._generation_start_time = time.time()
        self._completion_times = []
        self._terminal_job_states = {}
        self._update_progress_display()

    def _on_queue_stopped(self):
        if hasattr(self, "lbl_speed"):
            self.lbl_speed.setText("Speed: --")
        if hasattr(self, "lbl_eta"):
            self.lbl_eta.setText("ETA: --")
        self._completion_times = []
        self._generation_start_time = None
        self._terminal_job_states = {}

    def _update_progress_display(self, jobs=None):
        if not hasattr(self, "overall_progress"):
            return

        jobs = list(jobs if jobs is not None else (self._latest_queue_jobs or []))
        total = len(jobs)
        done = 0
        failed = 0
        current_terminal = {}

        for job in jobs:
            status = str(job.get("status") or "").strip().lower()
            job_id = str(job.get("id") or job.get("job_id") or "").strip()
            if status == "completed":
                done += 1
            elif status == "failed":
                failed += 1
            if job_id:
                current_terminal[job_id] = status
                self._note_terminal_job(job_id, status)

        stale_ids = [job_id for job_id in self._terminal_job_states if job_id not in current_terminal]
        for job_id in stale_ids:
            self._terminal_job_states.pop(job_id, None)

        completed = done + failed
        self._update_session_stats_label(done)

        if total <= 0:
            self.overall_progress.setValue(0)
            self.lbl_progress_text.setText("0/0 (0%)")
            self.lbl_speed.setText("Speed: --")
            self.lbl_eta.setText("ETA: --")
            return

        percent = int((completed / total) * 100) if total else 0
        self.overall_progress.setValue(percent)
        self.lbl_progress_text.setText(f"{done}/{total} ({percent}%)")

        if len(self._completion_times) >= 2:
            time_span = max(0.0, self._completion_times[-1] - self._completion_times[0])
            if time_span > 0:
                images_per_sec = (len(self._completion_times) - 1) / time_span
                images_per_min = images_per_sec * 60.0
                self.lbl_speed.setText(f"Speed: ~{images_per_min:.1f} img/min")
                remaining = max(0, total - completed)
                if images_per_sec > 0 and remaining > 0:
                    eta_seconds = remaining / images_per_sec
                    if eta_seconds < 60:
                        self.lbl_eta.setText(f"ETA: ~{int(eta_seconds)}s")
                    elif eta_seconds < 3600:
                        self.lbl_eta.setText(f"ETA: ~{int(eta_seconds / 60)}m")
                    else:
                        hours = int(eta_seconds / 3600)
                        mins = int((eta_seconds % 3600) / 60)
                        self.lbl_eta.setText(f"ETA: ~{hours}h {mins}m")
                elif remaining <= 0:
                    self.lbl_eta.setText("ETA: complete")
                else:
                    self.lbl_eta.setText("ETA: calculating...")
            else:
                self.lbl_speed.setText("Speed: calculating...")
                self.lbl_eta.setText("ETA: calculating...")
        elif self._generation_start_time and done > 0:
            elapsed = max(0.0, time.time() - self._generation_start_time)
            if elapsed > 0:
                images_per_min = (done / elapsed) * 60.0
                self.lbl_speed.setText(f"Speed: ~{images_per_min:.1f} img/min")
                remaining = max(0, total - completed)
                if remaining <= 0:
                    self.lbl_eta.setText("ETA: complete")
                else:
                    eta_seconds = (remaining / max(1, done)) * elapsed
                    if eta_seconds < 60:
                        self.lbl_eta.setText(f"ETA: ~{int(eta_seconds)}s")
                    elif eta_seconds < 3600:
                        self.lbl_eta.setText(f"ETA: ~{int(eta_seconds / 60)}m")
                    else:
                        hours = int(eta_seconds / 3600)
                        mins = int((eta_seconds % 3600) / 60)
                        self.lbl_eta.setText(f"ETA: ~{hours}h {mins}m")
            else:
                self.lbl_speed.setText("Speed: --")
                self.lbl_eta.setText("ETA: --")
        else:
            self.lbl_speed.setText("Speed: --")
            self.lbl_eta.setText("ETA: --")

    def _refresh_dashboard_stats(self, jobs=None):
        if jobs is None:
            jobs = self._latest_queue_jobs
        counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
        for job in jobs:
            status = str(job.get("status") or "").strip().lower()
            if status in counts:
                counts[status] += 1

        if hasattr(self, "stat_pending"):
            self.stat_pending.setText(str(counts["pending"]))
        if hasattr(self, "stat_running"):
            self.stat_running.setText(str(counts["running"]))
        if hasattr(self, "stat_completed"):
            self.stat_completed.setText(str(counts["completed"]))
        if hasattr(self, "stat_failed"):
            self.stat_failed.setText(str(counts["failed"]))
        if hasattr(self, "btn_failed_count"):
            self.btn_failed_count.setStyleSheet(self._failed_stat_button_style(counts["failed"] > 0))
        self._update_session_stats_label(counts["completed"])
        self._update_progress_display(jobs)
        if hasattr(self, "sidebar"):
            self.sidebar.update_stats(
                counts["pending"],
                counts["running"],
                counts["completed"],
                counts["failed"],
                counts["completed"],
            )

    def _adjust_mode_tabs_height(self):
        if not hasattr(self, "mode_tabs"):
            return
        compact_screen = self.height() < 800
        preferred_top = 180 if compact_screen else 220
        self.mode_tabs.setMinimumHeight(preferred_top)
        self.mode_tabs.setMaximumHeight(16777215)
        if hasattr(self, "dashboard_body_splitter"):
            sizes = self.dashboard_body_splitter.sizes()
            if sizes and sizes[0] < preferred_top:
                total = max(sum(sizes), preferred_top + 260)
                self.dashboard_body_splitter.setSizes([preferred_top, max(260, total - preferred_top)])

    def _update_runtime_badges(self):
        if hasattr(self, "lbl_runtime_mode"):
            active_label = self.mode_tabs.tabText(self.mode_tabs.currentIndex()) if hasattr(self, "mode_tabs") else "Image"
            self.lbl_runtime_mode.setText(f"Mode: {active_label}")

        if hasattr(self, "lbl_runtime_parallel"):
            selected = self._current_parallel_slots()
            self.lbl_runtime_parallel.setText(f"Parallel: {int(selected or 1)}/account")

        self._update_session_stats_label(self.stat_completed.text() if hasattr(self, "stat_completed") else 0)

    def _update_queue_status_label(self):
        if not hasattr(self, "lbl_queue_status"):
            return

        if self.queue_stopping:
            text = "Queue: STOPPING"
            color = "#EF4444"
            background = "#1F1A2A"
            border = "#EF4444"
        elif self.queue_paused:
            text = "Queue: PAUSED"
            color = "#F59E0B"
            background = "#1C1E2A"
            border = "#F59E0B"
        elif self.queue_running:
            text = "Queue: RUNNING"
            color = "#22C55E"
            background = "#111F2E"
            border = "#22C55E"
        else:
            text = "Queue: STOPPED"
            color = "#64748B"
            background = "#1D2535"
            border = "#475569"

        self.lbl_queue_status.setText(text)
        self.lbl_queue_status.setStyleSheet(
            f"color: {color}; font-weight: 700; font-size: 12px; "
            f"padding: 6px 12px; background: {background}; border: 1px solid {border}; border-radius: 8px;"
        )
        self._sync_sidebar_selection()

    def _set_queue_controls_state(self, state):
        state = str(state or "stopped").strip().lower()
        is_running = state in ("running", "paused", "stopping")
        is_paused = state == "paused"
        is_stopping = state == "stopping"

        self.btn_start.setEnabled(not is_running)
        self.btn_pause.setVisible(not is_paused)
        self.btn_pause.setEnabled(state == "running")
        self.btn_resume.setVisible(is_paused)
        self.btn_resume.setEnabled(is_paused)
        self.btn_stop.setEnabled(state in ("running", "paused"))
        if hasattr(self, "btn_force_stop_clear"):
            self.btn_force_stop_clear.setEnabled(not is_stopping)

        self.queue_running = is_running
        self.queue_paused = is_paused
        self.queue_stopping = is_stopping
        self._update_queue_status_label()

    def _estimate_video_credits(self, prompt_count=1, output_count=1, upscale="none", video_model=""):
        model_text = str(video_model or "").strip().lower()
        if "lower pri" in model_text or "lower priority" in model_text or "relaxed" in model_text:
            base_credits = 0
        elif "quality" in model_text:
            base_credits = 100
        else:
            base_credits = 10

        per_video = base_credits + (50 if str(upscale or "none").strip().lower() == "4k" else 0)
        return max(1, int(prompt_count or 1)) * max(1, int(output_count or 1)) * per_video

    def _build_credits_estimate_text(self):
        current_settings = self._current_generation_settings()
        job_type = str(current_settings.get("job_type") or "image").strip().lower()
        if job_type == "image":
            return "Images: ~0 credits"

        output_count = max(1, int(current_settings.get("video_output_count") or current_settings.get("output_count") or 1))
        upscale = str(current_settings.get("video_upscale") or "none").strip().lower()
        video_model = str(current_settings.get("video_model") or current_settings.get("model") or "").strip()
        total = self._estimate_video_credits(
            prompt_count=1,
            output_count=output_count,
            upscale=upscale,
            video_model=video_model,
        )

        if job_type == "pipeline":
            return f"~{total} credits per prompt (image free + video {total})"

        if upscale == "1080p":
            suffix = " (1080p upscale free)"
        elif upscale == "4k":
            suffix = f" (incl. {50 * output_count} for 4K upscale)"
        else:
            suffix = ""
        return f"~{total} credits per prompt{suffix}"

    def _sync_pending_queue_jobs_to_current_settings(self):
        if not getattr(self, "_pending_settings_sync_ready", False):
            return 0

        current_settings = self._current_generation_settings()
        self._start_background_task(
            update_pending_jobs_generation_settings,
            current_settings["model"],
            current_settings["aspect_ratio"],
            current_settings["output_count"],
            current_settings["ref_path"],
            ref_paths=current_settings.get("ref_paths"),
            job_type=current_settings["job_type"],
            video_model=current_settings["video_model"],
            video_sub_mode=current_settings["video_sub_mode"],
            video_ratio=current_settings.get("video_ratio", ""),
            video_prompt=current_settings.get("video_prompt", ""),
            video_upscale=current_settings["video_upscale"],
            video_output_count=current_settings["video_output_count"],
            start_image_path=current_settings["start_image_path"],
            end_image_path=current_settings["end_image_path"],
            filter_job_type=current_settings["job_type"],
            filter_video_sub_mode=(
                current_settings["video_sub_mode"]
                if current_settings["job_type"] in ("video", "pipeline")
                else None
            ),
            on_finished=self._on_pending_settings_sync_finished,
        )
        return 0

    def _on_pending_settings_sync_finished(self, updated_count):
        try:
            updated_count = int(updated_count or 0)
        except Exception:
            updated_count = 0
        if updated_count > 0:
            self.load_queue_table()
        return updated_count

    def _on_generation_settings_changed(self):
        self._update_runtime_badges()
        self._sync_pending_queue_jobs_to_current_settings()

    def _set_remaining_credits(self, credits):
        _ = credits
        return

    def append_log(self, msg):
        text = str(msg or "")
        marker = "[CREDITS] Remaining:"
        if marker in text:
            try:
                value_text = text.split(marker, 1)[1].strip().split()[0].replace(",", "")
                self._set_remaining_credits(value_text)
            except Exception:
                pass
        if hasattr(self, "log_buffer"):
            self.log_buffer.append(text)
            return
        self.logs_output.append(text)

    def _on_clean_profiles(self):
        """Clean all account browser profiles — remove junk, keep cookies."""
        from src.core.profile_cleaner import clean_profile
        from src.core.app_paths import get_sessions_dir

        sessions_dir = str(get_sessions_dir())
        total_deleted = 0
        total_freed = 0

        if os.path.isdir(sessions_dir):
            for name in os.listdir(sessions_dir):
                path = os.path.join(sessions_dir, name)
                if os.path.isdir(path):
                    d, f = clean_profile(path)
                    total_deleted += d
                    total_freed += f

        freed_mb = total_freed / (1024 * 1024)
        self._append_log(f"[CLEAN] All profiles cleaned: {total_deleted} items, {freed_mb:.1f}MB freed.")

        from PySide6.QtWidgets import QMessageBox
        QMessageBox.information(
            self, "Profiles Cleaned",
            f"Cleaned {total_deleted} items\nFreed {freed_mb:.1f} MB\n\nLogin sessions preserved.",
        )

    def save_settings(self):
        slots = int(self.spin_slots_per_account.value())
        stagger = round(float(self.spin_same_account_stagger.value()), 1)
        global_stagger_min = round(float(self.spin_global_stagger_min.value()), 1)
        global_stagger_max = round(float(self.spin_global_stagger_max.value()), 1)
        if global_stagger_max < global_stagger_min:
            global_stagger_max = global_stagger_min
            self.spin_global_stagger_max.setValue(global_stagger_max)
        recaptcha_cooldown = int(self.spin_recaptcha_cooldown.value())
        max_retries = int(self.spin_max_retries.value())
        retry_base_delay = int(self.spin_retry_base_delay.value())
        auto_refresh_after_jobs = int(self.spin_auto_refresh_after_jobs.value())
        auto_restart_fail_threshold = int(self.spin_restart_threshold.value())
        auto_restart_fail_window = int(self.spin_restart_window.value())
        auto_restart_cooldown = int(self.spin_restart_cooldown.value())
        profile_clone_enabled = self.chk_profile_clone.isChecked()
        image_execution_mode = "api_only"
        browser_mode = self.cmb_browser_mode.currentData() or "headless"
        chrome_display = self.cmb_chrome_display.currentData() or "visible"
        cloak_display = self.cmb_cloak_display.currentData() or "headless"
        random_fingerprint_enabled = self.chk_random_fingerprint.isChecked()
        cookie_warmup_enabled = self.chk_cookie_warmup.isChecked()
        light_warmup_enabled = self.chk_light_warmup.isChecked()
        speed_profile = self.cmb_speed_profile.currentData() or "fast"
        warmup_min = round(float(self.spin_warmup_min.value()), 1)
        warmup_max = round(float(self.spin_warmup_max.value()), 1)
        if warmup_max < warmup_min:
            warmup_max = warmup_min
            self.spin_warmup_max.setValue(warmup_max)

        if str(speed_profile).lower() == "fast":
            ref_wait_min, ref_wait_max = 0.3, 0.8
            no_ref_wait_min, no_ref_wait_max = warmup_min, warmup_max
        else:
            speed_profile = "stable"
            ref_wait_min, ref_wait_max = 0.4, 0.9
            no_ref_wait_min, no_ref_wait_max = max(0.3, warmup_min), max(0.6, warmup_max)

        default_output_dir = self._default_outputs_dir()
        selected_output_dir = os.path.abspath(os.path.expanduser(str(self.output_dir_input.text() or "").strip()))
        stored_output_dir = "" if selected_output_dir == default_output_dir else selected_output_dir
        self.output_dir_input.setText(selected_output_dir)
        self._update_runtime_badges()
        target_slots = max(1, min(5, slots))
        for selector_name in ("img_cmb_parallel", "t2v_cmb_parallel", "ref_cmb_parallel", "frm_cmb_parallel", "pipe_cmb_parallel"):
            selector = getattr(self, selector_name, None)
            if selector is None:
                continue
            idx = selector.findData(target_slots)
            if idx >= 0:
                selector.setCurrentIndex(idx)
        settings_payload = {
            "slots_per_account": str(slots),
            "same_account_stagger_seconds": str(stagger),
            "global_stagger_min_seconds": str(global_stagger_min),
            "global_stagger_max_seconds": str(global_stagger_max),
            "max_retries": str(max_retries),
            "max_auto_retries_per_job": str(max_retries),
            "retry_base_delay_seconds": str(retry_base_delay),
            "auto_retry_base_delay_seconds": str(retry_base_delay),
            "recaptcha_account_cooldown_seconds": str(recaptcha_cooldown),
            "auto_refresh_after_jobs": str(auto_refresh_after_jobs),
            "auto_restart_recap_fail_threshold": str(auto_restart_fail_threshold),
            "auto_restart_recap_fail_window": str(auto_restart_fail_window),
            "auto_restart_recap_cooldown_seconds": str(auto_restart_cooldown),
            "enable_profile_clones": "1" if profile_clone_enabled else "0",
            "api_captcha_submit_lock": "0",
            "image_execution_mode": str(image_execution_mode),
            "browser_mode": str(browser_mode),
            "chrome_display": str(chrome_display),
            "cloak_display": str(cloak_display),
            "random_fingerprint_per_session": "1" if random_fingerprint_enabled else "0",
            "cookie_warmup": "1" if cookie_warmup_enabled else "0",
            "light_warmup": "1" if light_warmup_enabled else "0",
            "speed_profile": str(speed_profile),
            "api_min_submit_gap_seconds": "0",
            "api_humanized_warmup_min_seconds": str(warmup_min),
            "api_humanized_warmup_max_seconds": str(warmup_max),
            "api_humanized_wait_ref_min_seconds": str(ref_wait_min),
            "api_humanized_wait_ref_max_seconds": str(ref_wait_max),
            "api_humanized_wait_no_ref_min_seconds": str(no_ref_wait_min),
            "api_humanized_wait_no_ref_max_seconds": str(no_ref_wait_max),
            "output_directory": stored_output_dir,
        }
        self._start_background_task(
            self._persist_settings_payload,
            settings_payload,
            on_finished=lambda _result: self._on_settings_saved(
                slots,
                stagger,
                global_stagger_min,
                global_stagger_max,
                max_retries,
                retry_base_delay,
                recaptcha_cooldown,
                auto_refresh_after_jobs,
                auto_restart_fail_threshold,
                auto_restart_fail_window,
                auto_restart_cooldown,
                profile_clone_enabled,
                browser_mode,
                chrome_display,
                cloak_display,
                random_fingerprint_enabled,
                cookie_warmup_enabled,
                light_warmup_enabled,
                speed_profile,
                selected_output_dir,
                warmup_min,
                warmup_max,
                ref_wait_min,
                ref_wait_max,
                no_ref_wait_min,
                no_ref_wait_max,
            ),
        )

    @staticmethod
    def _persist_settings_payload(settings_payload):
        for key, value in dict(settings_payload or {}).items():
            set_setting(str(key), str(value))
        return True

    def _on_settings_saved(
        self,
        slots,
        stagger,
        global_stagger_min,
        global_stagger_max,
        max_retries,
        retry_base_delay,
        recaptcha_cooldown,
        auto_refresh_after_jobs,
        auto_restart_fail_threshold,
        auto_restart_fail_window,
        auto_restart_cooldown,
        profile_clone_enabled,
        browser_mode,
        chrome_display,
        cloak_display,
        random_fingerprint_enabled,
        cookie_warmup_enabled,
        light_warmup_enabled,
        speed_profile,
        selected_output_dir,
        warmup_min,
        warmup_max,
        ref_wait_min,
        ref_wait_max,
        no_ref_wait_min,
        no_ref_wait_max,
    ):
        self.append_log(
            f"[SETTINGS] Saved: slots/account={slots}, stagger={stagger:.1f}s, "
            f"global stagger={global_stagger_min:.1f}s-{global_stagger_max:.1f}s, "
            f"retries={max_retries}, retry base delay={retry_base_delay}s, "
            f"reCAPTCHA cooldown={recaptcha_cooldown}s, "
            f"auto-refresh every {auto_refresh_after_jobs} job(s), "
            f"auto-restart after {auto_restart_fail_threshold} reCAPTCHA fail(s) in {auto_restart_fail_window}, "
            f"restart cooldown={auto_restart_cooldown}s, "
            f"profile cloning={'on' if profile_clone_enabled else 'off'}, "
            f"image mode=api_only, browser mode={browser_mode}, chrome display={chrome_display}, "
            f"cloak display={cloak_display}, random fingerprint={'on' if random_fingerprint_enabled else 'off'}, "
            f"cookie warm-up={'on' if cookie_warmup_enabled else 'off'}, "
            f"light warm-up={'on' if light_warmup_enabled else 'off'}, speed profile={speed_profile}, "
            f"output dir={selected_output_dir}."
        )
        self.append_log(
            f"[SETTINGS] Applied speed preset: warmup={warmup_min:.1f}-{warmup_max:.1f}s, "
            f"pre-submit(ref)={ref_wait_min:.1f}-{ref_wait_max:.1f}s, "
            f"pre-submit(no-ref)={no_ref_wait_min:.1f}-{no_ref_wait_max:.1f}s."
        )
        if self.queue_manager and self.queue_manager.isRunning():
            self.append_log("[SETTINGS] Restart Queue Manager to apply new slot settings.")
        QMessageBox.information(self, "Settings Saved", "Automation settings saved successfully.")
        
    def start_login(self):
        acc_name = self.acc_name_input.text().strip()
        proxy_value = self.acc_proxy_input.text().strip()
        log_target = acc_name if acc_name else "AUTO-GMAIL"
        self.append_log(f"Starting login flow for {log_target}. A browser will open...")
        self._reset_download_progress_widget()
        self._pending_login_add = None
        self.btn_login.setEnabled(False)
        self.acc_name_input.setEnabled(False)
        self.acc_proxy_input.setEnabled(False)
        
        self.login_worker = LoginWorker(acc_name, proxy=proxy_value)
        self.login_worker.log_msg.connect(self.append_log, Qt.QueuedConnection)
        self.login_worker.download_progress.connect(self._on_download_progress, Qt.QueuedConnection)
        self.login_worker.download_complete.connect(self._on_download_complete, Qt.QueuedConnection)
        self.login_worker.session_saved.connect(self.on_login_session_saved, Qt.QueuedConnection)
        self.login_worker.warmup_progress.connect(self.warmup_progress_signal.emit, Qt.QueuedConnection)
        self.login_worker.warmup_complete.connect(self.warmup_complete_signal.emit, Qt.QueuedConnection)
        self.login_worker.finished_login.connect(self.on_login_finished)
        self.login_worker.start()

    def on_login_session_saved(self, name, session_path, detected_email):
        proxy_value = str(getattr(self.login_worker, "proxy", "") or "").strip() if self.login_worker else ""
        self._pending_login_add = (str(name), str(session_path))
        self._start_background_task(
            add_account,
            name,
            session_path,
            proxy_value,
            on_finished=lambda added, name=name, detected_email=detected_email: self._on_account_added(
                name,
                detected_email,
                added,
            ),
        )
        
    def on_login_finished(self, name, session_path, detected_email):
        proxy_value = str(getattr(self.login_worker, "proxy", "") or "").strip() if self.login_worker else ""
        self.login_worker = None
        self.acc_name_input.clear()
        self.acc_proxy_input.clear()
        self.btn_login.setEnabled(True)
        self.acc_name_input.setEnabled(True)
        self.acc_proxy_input.setEnabled(True)
        if self._pending_login_add == (str(name), str(session_path)):
            self._pending_login_add = None
            return
        self._start_background_task(
            add_account,
            name,
            session_path,
            proxy_value,
            on_finished=lambda added, name=name, detected_email=detected_email: self._on_account_added(
                name,
                detected_email,
                added,
            ),
        )

    def refresh_accounts(self):
        self.load_accounts(after_load=lambda: self.start_login_status_check())

    def _on_account_added(self, name, detected_email, added):
        if added:
            if detected_email:
                self.append_log(f"Account '{name}' auto-detected from Google login and added successfully.")
            else:
                self.append_log(f"Account '{name}' added successfully to database.")
        else:
            self.append_log(f"Account '{name}' already exists. Session was not added as duplicate.")
            QMessageBox.warning(
                self,
                "Duplicate Account",
                f"Account '{name}' already exists in Account Manager."
            )
        self.refresh_accounts()

    def _reset_download_progress_widget(self):
        if not hasattr(self, "download_widget"):
            return
        self.download_widget.setVisible(False)
        self.download_progress.setValue(0)
        self.download_percent.setText("0%")
        self.download_label.setText("Downloading CloakBrowser binary...")
        self.download_label.setStyleSheet("color: #60A5FA; font-weight: 600;")
        self.download_progress.setStyleSheet(
            """
            QProgressBar {
                border: 1px solid #334155;
                border-radius: 4px;
                background-color: #1E293B;
                text-align: center;
                color: white;
                font-weight: 600;
            }
            QProgressBar::chunk {
                background-color: #3B82F6;
                border-radius: 3px;
            }
            """
        )

    def _on_download_progress(self, percent, status_text):
        self.download_widget.setVisible(True)
        safe_percent = max(0, min(100, int(percent or 0)))
        self.download_progress.setValue(safe_percent)
        self.download_percent.setText(f"{safe_percent}%")
        self.download_label.setText(str(status_text or "Downloading CloakBrowser..."))
        self.btn_login.setEnabled(False)
        self.btn_login.setText("Downloading CloakBrowser...")

    def _on_download_complete(self, success, message):
        self.download_widget.setVisible(True)
        if success:
            self.download_label.setText(f"OK {message}")
            self.download_label.setStyleSheet("color: #22C55E; font-weight: 600;")
            self.download_progress.setValue(100)
            self.download_percent.setText("100%")
            self.download_progress.setStyleSheet(
                """
                QProgressBar {
                    border: 1px solid #334155;
                    border-radius: 4px;
                    background-color: #1E293B;
                    text-align: center;
                    color: white;
                    font-weight: 600;
                }
                QProgressBar::chunk {
                    background-color: #22C55E;
                    border-radius: 3px;
                }
                """
            )
            self.btn_login.setText("Waiting for Login Browser...")
            QTimer.singleShot(5000, lambda: self.download_widget.setVisible(False))
        else:
            self.download_label.setText(f"Failed {message}")
            self.download_label.setStyleSheet("color: #EF4444; font-weight: 600;")
            self.btn_login.setText("Login to Google (New Browser)")

    def load_accounts(self, after_load=None):
        self.acc_table.setRowCount(0)
        self._start_background_task(
            self._load_accounts_payload,
            on_finished=lambda payload, after_load=after_load: self._apply_accounts_payload(payload, after_load=after_load),
        )

    @staticmethod
    def _load_accounts_payload():
        accs = get_accounts()
        rename_count = 0
        display_map = {}
        for acc in accs:
            current_name = str(acc.get("name") or "").strip()
            if current_name and "@" in current_name:
                display_map[acc.get("id")] = current_name
                continue
            detected = AccountManager.detect_email_from_session_dir(acc.get("session_path"))
            if detected and detected != current_name:
                if update_account_name_by_id(acc.get("id"), detected):
                    acc["name"] = detected
                    display_map[acc.get("id")] = detected
                    rename_count += 1
                else:
                    alias = current_name or "unnamed"
                    display_map[acc.get("id")] = f"{detected} (alias: {alias})"
            else:
                display_map[acc.get("id")] = current_name

        if rename_count > 0:
            accs = get_accounts()
            for acc in accs:
                name = str(acc.get("name") or "").strip()
                if acc.get("id") not in display_map:
                    display_map[acc.get("id")] = name
        return {"accounts": accs, "display_map": display_map, "rename_count": rename_count}

    def _apply_accounts_payload(self, payload, after_load=None):
        self._loading_accounts_table = True
        self.acc_table.setRowCount(0)
        self.warmup_widgets = {}
        payload = dict(payload or {})
        accs = list(payload.get("accounts") or [])
        display_map = dict(payload.get("display_map") or {})
        rename_count = int(payload.get("rename_count") or 0)
        self._latest_accounts = accs
        if rename_count > 0:
            self.append_log(f"[ACCOUNTS] Auto-updated {rename_count} account name(s) from session Gmail.")

        live_ids = {int(acc.get("id") or 0) for acc in accs}
        self.account_login_state = {
            int(account_id): value
            for account_id, value in self.account_login_state.items()
            if int(account_id) in live_ids
        }

        try:
            for i, acc in enumerate(accs):
                self.acc_table.insertRow(i)
                db_id = int(acc.get("id") or 0)
                id_item = QTableWidgetItem(str(i + 1))
                id_item.setData(Qt.UserRole, db_id)
                real_name = str(acc.get('name') or "")
                display_name = str(display_map.get(acc.get("id"), real_name) or "")
                name_item = QTableWidgetItem(display_name)
                name_item.setData(Qt.UserRole, real_name)
                name_item.setData(Qt.UserRole + 1, str(acc.get("session_path") or ""))
                saved_status_item = QTableWidgetItem("")

                for item in (id_item, name_item, saved_status_item):
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)

                self.acc_table.setItem(i, 0, id_item)
                self.acc_table.setItem(i, 1, name_item)
                self.acc_table.setItem(i, 4, saved_status_item)
                self._set_account_proxy_cell(i, db_id, real_name or display_name, acc.get("proxy"))
                self._set_account_login_status_cell(i, db_id, self.account_login_state.get(db_id))
                self._set_account_saved_status_cell(i, acc)
                self._get_or_create_table_item(self.acc_table, i, 5, "idle")
                self._get_or_create_table_item(self.acc_table, i, 6, "--")
                self._get_or_create_table_item(self.acc_table, i, 7, "0/1")
                self._set_account_detail_cell(i, real_name, "Queue stopped")
                self._add_account_action_buttons(i, db_id, real_name or display_name)
        finally:
            self._loading_accounts_table = False
        self._refresh_account_runtime_cells()
        self._refresh_login_statuses()
        if callable(after_load):
            after_load()

    def start_login_status_check(self, account_ids=None):
        accounts = list(self._latest_accounts or [])
        if account_ids is not None:
            wanted = {int(account_id) for account_id in account_ids}
            accounts = [acc for acc in accounts if int(acc.get("id") or 0) in wanted]

        if not accounts:
            self.btn_refresh_accs.setEnabled(True)
            self.btn_refresh_accs.setText("Refresh List")
            self._refresh_account_overview()
            return

        if self.login_check_worker and self.login_check_worker.isRunning():
            self.append_log("[ACCOUNTS] Login status check already running.")
            return

        self.btn_refresh_accs.setEnabled(False)
        self.btn_refresh_accs.setText("Checking...")

        for account in accounts:
            account_id = int(account.get("id") or 0)
            self.account_login_state[account_id] = {"state": "checking"}
            row = self._find_account_row(account_id)
            if row >= 0:
                self._set_account_login_status_cell(row, account_id, self.account_login_state[account_id])

        self._refresh_account_overview()
        self.login_check_worker = LoginCheckWorker(accounts)
        self.login_check_worker.single_result.connect(self._on_single_account_checked)
        self.login_check_worker.result_ready.connect(self._on_login_check_complete)
        self.login_check_worker.start()

    def _on_single_account_checked(self, account_id, status):
        normalized = self._normalize_login_status(status)
        self.account_login_state[int(account_id)] = normalized
        row = self._find_account_row(account_id)
        if row >= 0:
            self._set_account_login_status_cell(row, account_id, normalized)
        self._refresh_account_overview()

    def _on_login_check_complete(self, all_results):
        self.btn_refresh_accs.setEnabled(True)
        self.btn_refresh_accs.setText("Refresh List")
        self.login_check_worker = None

        logged_in = 0
        total = 0
        for account_id, status in dict(all_results or {}).items():
            total += 1
            normalized = self._normalize_login_status(status)
            self.account_login_state[int(account_id)] = normalized
            if normalized.get("logged_in"):
                logged_in += 1

        logged_out = max(0, total - logged_in)
        if total > 0:
            if logged_out > 0:
                self.append_log(
                    f"[ACCOUNTS] Login check: {logged_in}/{total} logged in, {logged_out} need re-login."
                )
            else:
                self.append_log(f"[ACCOUNTS] All {total} accounts logged in.")
        self._refresh_account_overview()

    def _relogin_account(self, account_id):
        self._start_account_session_refresh(account_id, action_label="Re-login")

    def on_relogin_finished(self, account_id, name, session_path, detected_email):
        self.relogin_worker = None
        self._start_background_task(
            self._persist_relogin_result,
            account_id,
            session_path,
            name,
            on_finished=lambda updated, account_id=account_id, name=name, detected_email=detected_email: self._on_relogin_saved(
                account_id,
                name,
                detected_email,
                updated,
            ),
        )

    @staticmethod
    def _persist_relogin_result(account_id, session_path, name):
        updated = update_account_session_by_id(account_id, session_path, name)
        if not updated:
            updated = update_account_session_by_id(account_id, session_path)
        return bool(updated)

    def _on_relogin_saved(self, account_id, name, detected_email, updated):
        if updated:
            if detected_email:
                self.append_log(f"[ACCOUNTS] Re-login complete for '{name}'. Session refreshed.")
            else:
                self.append_log(f"[ACCOUNTS] Re-login complete for '{name}'.")
        else:
            self.append_log(f"[ACCOUNTS] Session refreshed for '{name}', but the account name could not be updated.")
        self.load_accounts(after_load=lambda: self.start_login_status_check(account_ids=[account_id]))

    def delete_selected_account(self):
        selected = self.acc_table.selectedItems()
        if not selected:
            return
            
        row = selected[0].row()
        id_item = self.acc_table.item(row, 0)
        db_id = int(id_item.data(Qt.UserRole) or 0)
        self._delete_account_by_id(db_id)

    def _is_moderated_failed_error(self, error_text):
        text = str(error_text or "").strip()
        if not text:
            return False
        if text.startswith("[moderated]") or text.startswith("MODERATION:"):
            return True

        text_upper = text.upper()
        moderation_tokens = (
            "PROMINENT_PERSON",
            "SAFETY_FILTER",
            "CONTENT_POLICY",
            "MODERATION",
            "FILTER_FAILED",
            "BLOCKED",
            "SEXUALLY_EXPLICIT",
            "VIOLENCE",
            "HATE_SPEECH",
            "CHILD_SAFETY",
            "HARMFUL",
            "DANGEROUS",
            "TOXIC",
        )
        return any(token in text_upper for token in moderation_tokens)

    def _update_failed_jobs_actions(self):
        row_count = self.failed_table.rowCount()
        has_rows = row_count > 0
        checked_rows = self._checked_failed_rows()
        self.btn_requeue_selected.setEnabled(bool(checked_rows))
        self.btn_requeue_selected.setToolTip(
            "Retry checked failed jobs. Moderated jobs require an edited prompt."
        )
        self.btn_retry_all_failed.setEnabled(has_rows)
        self.btn_retry_all_failed.setToolTip(
            "Retry every failed job. Moderated rows will only retry if the prompt was edited."
        )
        self.btn_copy_failed.setEnabled(has_rows)
        self.btn_clear_failed.setEnabled(has_rows)
        self.chk_select_all_failed.setEnabled(has_rows)

    def _checked_failed_rows(self):
        rows = []
        for row in range(self.failed_table.rowCount()):
            item = self.failed_table.item(row, 0)
            if item and item.checkState() == Qt.CheckState.Checked:
                rows.append(row)
        return rows

    def _toggle_select_all_failed(self, checked):
        if getattr(self, "_loading_failed_table", False):
            return
        self._loading_failed_table = True
        target_state = Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked
        try:
            for row in range(self.failed_table.rowCount()):
                item = self.failed_table.item(row, 0)
                if item:
                    item.setCheckState(target_state)
        finally:
            self._loading_failed_table = False
        self._update_failed_jobs_actions()

    def _sync_failed_select_all_checkbox(self):
        if not hasattr(self, "chk_select_all_failed"):
            return
        row_count = self.failed_table.rowCount()
        self.chk_select_all_failed.blockSignals(True)
        self.chk_select_all_failed.setChecked(row_count > 0 and len(self._checked_failed_rows()) == row_count)
        self.chk_select_all_failed.blockSignals(False)

    def _failed_row_job_id(self, row):
        item = self.failed_table.item(row, 0)
        return item.data(Qt.UserRole) if item else None

    def _failed_row_is_moderated(self, row):
        item = self.failed_table.item(row, 0)
        return bool(item.data(Qt.UserRole + 1)) if item else False

    def _failed_original_prompt(self, row):
        item = self.failed_table.item(row, 0)
        return str(item.data(Qt.UserRole + 2) or "") if item else ""

    def _failed_row_edited_prompt(self, row):
        prompt_item = self.failed_table.item(row, 2)
        return str(prompt_item.text() or "").strip() if prompt_item else ""

    def _apply_failed_prompt_edit_style(self, row):
        prompt_item = self.failed_table.item(row, 2)
        if not prompt_item:
            return
        edited_prompt = str(prompt_item.text() or "").strip()
        original_prompt = self._failed_original_prompt(row)
        if edited_prompt and edited_prompt != original_prompt:
            prompt_item.setBackground(QColor("#1A2744"))
        else:
            prompt_item.setBackground(QColor())

    def _on_failed_table_item_changed(self, item):
        if item is None or getattr(self, "_loading_failed_table", False):
            return
        if item.column() == 0:
            self._sync_failed_select_all_checkbox()
            self._update_failed_jobs_actions()
            return
        if item.column() != 2:
            return

        row = item.row()
        job_id = self._failed_row_job_id(row)
        original_prompt = self._failed_original_prompt(row)
        edited_prompt = str(item.text() or "").strip()
        if job_id:
            if edited_prompt and edited_prompt != original_prompt:
                self.failed_prompt_edits[job_id] = edited_prompt
            else:
                self.failed_prompt_edits.pop(job_id, None)
        self._apply_failed_prompt_edit_style(row)

    def load_failed_jobs(self, jobs=None):
        if jobs is not None:
            self._populate_failed_jobs_table(jobs)
            return
        self._request_failed_jobs_refresh(force=self.tabs.currentWidget() is self.tab_failed_jobs)

    def _populate_failed_jobs_table(self, jobs):
        self._loading_failed_table = True
        self.failed_table.setRowCount(0)
        jobs = list(jobs or [])
        live_ids = {str(job.get("id") or "") for job in jobs}
        self.failed_prompt_edits = {
            job_id: prompt
            for job_id, prompt in self.failed_prompt_edits.items()
            if job_id in live_ids
        }
        self._failed_jobs_dirty = False

        try:
            for i, j in enumerate(jobs):
                self.failed_table.insertRow(i)
                is_moderated = self._is_moderated_failed_error(j.get("error"))
                job_id = str(j.get("id") or "")
                original_prompt = str(j.get("prompt") or "")
                edited_prompt = self.failed_prompt_edits.get(job_id, original_prompt)

                check_item = QTableWidgetItem("")
                check_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsUserCheckable)
                check_item.setCheckState(Qt.CheckState.Unchecked)
                check_item.setData(Qt.UserRole, job_id)
                check_item.setData(Qt.UserRole + 1, is_moderated)
                check_item.setData(Qt.UserRole + 2, original_prompt)
                self.failed_table.setItem(i, 0, check_item)

                number_item = QTableWidgetItem(str(j.get("queue_no") or i + 1))
                number_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                self.failed_table.setItem(i, 1, number_item)

                prompt_item = QTableWidgetItem(edited_prompt)
                prompt_item.setToolTip("Double-click to edit prompt before retrying.")
                prompt_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEditable)
                self.failed_table.setItem(i, 2, prompt_item)
                self._apply_failed_prompt_edit_style(i)

                type_item = QTableWidgetItem(str(j.get("job_type") or "image").title())
                type_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                self.failed_table.setItem(i, 3, type_item)

                error_item = QTableWidgetItem(str(j.get("error") or ""))
                error_item.setToolTip(str(j.get("error") or ""))
                error_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                self.failed_table.setItem(i, 4, error_item)

                original_item = QTableWidgetItem(original_prompt)
                original_item.setToolTip(original_prompt)
                original_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                self.failed_table.setItem(i, 5, original_item)

                badge_text = "⚠️ Moderated" if is_moderated else "❌ Failed"
                badge_item = QTableWidgetItem(badge_text)
                badge_item.setToolTip(
                    "Blocked by Google content filter. Edit the prompt before retrying."
                    if is_moderated
                    else f"{str(j.get('job_type') or 'image').title()} job failed after queue retries."
                )
                badge_item.setForeground(QColor("#2B1600") if is_moderated else QColor("#5A0F12"))
                badge_item.setBackground(QColor("#F8D98B") if is_moderated else QColor("#F4B7BD"))
                badge_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                self.failed_table.setItem(i, 6, badge_item)
        finally:
            self._loading_failed_table = False

        self._sync_failed_select_all_checkbox()
        self._update_failed_jobs_actions()

    def _retry_failed_rows(self, rows):
        if not rows:
            QMessageBox.information(self, "No Failed Jobs Selected", "Select at least one failed job to retry.")
            return

        retried = 0
        skipped = 0
        retry_updates = []
        for row in rows:
            job_id = self._failed_row_job_id(row)
            if not job_id:
                continue

            edited_prompt = self._failed_row_edited_prompt(row)
            original_prompt = self._failed_original_prompt(row)
            is_moderated = self._failed_row_is_moderated(row)

            if not edited_prompt:
                skipped += 1
                self.append_log(f"[RETRY] Skipping job {str(job_id)[:8]} — prompt is empty.")
                continue

            if is_moderated and edited_prompt == original_prompt:
                skipped += 1
                preview = edited_prompt[:40] + ("..." if len(edited_prompt) > 40 else "")
                self.append_log(f"[RETRY] Skipping moderated job — prompt not changed: '{preview}'")
                continue

            self.failed_prompt_edits.pop(job_id, None)
            retry_updates.append(
                {
                    "job_id": job_id,
                    "prompt": edited_prompt,
                    "retry_source": "failed_tab",
                }
            )

        if retry_updates:
            self._start_background_task(
                retry_failed_jobs_to_top,
                retry_updates,
                retry_source="failed_tab",
                on_finished=lambda retried_count, skipped=skipped: self._on_failed_retry_finished(retried_count, skipped),
            )
            return

        if retried <= 0:
            QMessageBox.information(
                self,
                "Nothing Re-queued",
                "No failed jobs were re-queued. Moderated jobs need an edited prompt before retrying.",
            )
        else:
            extra = f" Skipped {skipped} job(s)." if skipped else ""
            self.append_log(
                f"[RETRY] {retried} job(s) re-queued at top with original filenames preserved.{extra}"
            )

        self.load_failed_jobs()
        self.load_queue_table()

    def _on_failed_retry_finished(self, retried, skipped):
        retried = int(retried or 0)
        if retried <= 0:
            QMessageBox.information(
                self,
                "Nothing Re-queued",
                "No failed jobs were re-queued. Moderated jobs need an edited prompt before retrying.",
            )
        else:
            extra = f" Skipped {skipped} job(s)." if skipped else ""
            self.append_log(
                f"[RETRY] {retried} job(s) re-queued at top with original filenames preserved.{extra}"
            )
        self.load_failed_jobs()
        self.load_queue_table()

    def _retry_selected_failed(self):
        self._retry_failed_rows(self._checked_failed_rows())

    def _retry_all_failed(self):
        self._retry_failed_rows(list(range(self.failed_table.rowCount())))

    def requeue_failed_job(self):
        self._retry_selected_failed()

    def copy_failed_prompts(self):
        prompts = []
        for row in range(self.failed_table.rowCount()):
            prompt_item = self.failed_table.item(row, 2)
            prompt_text = str(prompt_item.text() or "").strip() if prompt_item else ""
            if prompt_text:
                prompts.append(prompt_text)
        if not prompts:
            QMessageBox.information(self, "No Failed Prompts", "There are no failed prompts to copy.")
            return

        QApplication.clipboard().setText("\n".join(prompts))
        self.append_log(f"Copied {len(prompts)} failed prompt(s) to clipboard.")

    def clear_failed_jobs_list(self):
        if self.failed_table.rowCount() <= 0:
            return
        confirm = QMessageBox.question(
            self,
            "Clear Failed Jobs",
            "Remove all failed jobs from the list?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return

        self._start_background_task(
            clear_failed_jobs,
            on_finished=self._on_failed_jobs_cleared,
        )

    def _on_failed_jobs_cleared(self, deleted_count):
        self.append_log(f"Cleared {int(deleted_count or 0)} failed job(s) from the dashboard.")
        self.load_failed_jobs()
        self.load_queue_table()

    def _is_supported_bulk_image_file(self, file_path):
        suffix = Path(str(file_path or "")).suffix.lower()
        return suffix in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}

    def _bulk_panel(self, mode_key):
        return self.bulk_panels.get(str(mode_key or "").strip())

    def _load_bulk_images_from_paths(self, mode_key, paths):
        panel = self._bulk_panel(mode_key)
        if not panel:
            return

        entries = []
        seen_paths = set()
        for raw_path in paths or []:
            path = Path(str(raw_path or "")).expanduser()
            if not path.exists():
                continue
            if path.is_dir():
                candidates = [item for item in path.iterdir() if item.is_file() and self._is_supported_bulk_image_file(item)]
            elif path.is_file() and self._is_supported_bulk_image_file(path):
                candidates = [path]
            else:
                candidates = []

            for item in candidates:
                resolved = str(item.resolve())
                if resolved in seen_paths:
                    continue
                seen_paths.add(resolved)
                try:
                    modified_time = float(item.stat().st_mtime)
                except Exception:
                    modified_time = 0.0
                entries.append({"path": resolved, "filename": item.name, "modified_time": modified_time})

        panel["entries"] = entries
        self._refresh_bulk_pairing_preview(mode_key)

    def clear_bulk_panel(self, mode_key):
        panel = self._bulk_panel(mode_key)
        if not panel:
            return
        panel["entries"] = []
        panel["prompts_input"].clear()
        self._refresh_bulk_pairing_preview(mode_key)

    def select_bulk_image_folder(self, mode_key):
        folder_path = QFileDialog.getExistingDirectory(self, "Select Folder With Images", "")
        if folder_path:
            self._load_bulk_images_from_paths(mode_key, [folder_path])

    def select_bulk_image_files(self, mode_key):
        file_paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select Images",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.bmp *.gif)",
        )
        if file_paths:
            self._load_bulk_images_from_paths(mode_key, file_paths)

    def _get_sorted_bulk_images(self, mode_key):
        panel = self._bulk_panel(mode_key)
        if not panel:
            return []
        images = list(panel.get("entries") or [])
        sort_mode = str(panel["sort_selector"].currentData() or "name_asc")
        if sort_mode == "name_desc":
            images.sort(key=lambda item: str(item.get("filename") or "").lower(), reverse=True)
        elif sort_mode == "time_old":
            images.sort(key=lambda item: float(item.get("modified_time") or 0.0))
        elif sort_mode == "time_new":
            images.sort(key=lambda item: float(item.get("modified_time") or 0.0), reverse=True)
        else:
            images.sort(key=lambda item: str(item.get("filename") or "").lower())
        return images

    def _build_thumbnail_icon(self, image_path, size=56):
        pixmap = QPixmap(str(image_path or ""))
        if pixmap.isNull():
            return QIcon(), QSize(size, size)
        scaled = pixmap.scaled(size, size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        return QIcon(scaled), scaled.size()

    def _filename_to_prompt(self, filename):
        name_no_ext = Path(str(filename or "")).stem
        return name_no_ext.replace("_", " ").replace("-", " ").strip()

    def _build_bulk_pairs(self, mode_key):
        panel = self._bulk_panel(mode_key)
        if not panel:
            return [], [], []
        images = self._get_sorted_bulk_images(mode_key)
        raw_lines = panel["prompts_input"].toPlainText().splitlines()
        prompts = [line.strip() for line in raw_lines if line.strip()]
        missing_action = str(panel["missing_selector"].currentData() or "filename")

        pairs = []
        for idx, image in enumerate(images):
            if idx < len(prompts):
                prompt = prompts[idx]
                paired = True
            elif missing_action == "filename":
                prompt = self._filename_to_prompt(image.get("filename"))
                paired = False
            else:
                continue

            pairs.append(
                {
                    "index": idx + 1,
                    "image_path": image.get("path"),
                    "filename": image.get("filename"),
                    "prompt": prompt,
                    "paired": paired,
                }
            )
        return images, prompts, pairs

    def _refresh_bulk_pairing_preview(self, mode_key):
        panel = self._bulk_panel(mode_key)
        if not panel:
            return
        images, prompts, pairs = self._build_bulk_pairs(mode_key)

        images_table = panel["images_table"]
        pairs_table = panel["pairs_table"]
        images_table.setRowCount(len(images))
        images_table.setVisible(bool(images))
        for row_idx, image in enumerate(images):
            images_table.setRowHeight(row_idx, 64)

            index_item = QTableWidgetItem(str(row_idx + 1))
            index_item.setForeground(QColor("#E2E8F0"))
            images_table.setItem(row_idx, 0, index_item)

            thumb_item = QTableWidgetItem("")
            icon, thumb_size = self._build_thumbnail_icon(image.get("path"))
            if not icon.isNull():
                thumb_item.setIcon(icon)
                images_table.setIconSize(thumb_size)
            thumb_item.setToolTip(str(image.get("path") or ""))
            images_table.setItem(row_idx, 1, thumb_item)

            file_item = QTableWidgetItem(str(image.get("filename") or ""))
            file_item.setToolTip(str(image.get("path") or ""))
            file_item.setForeground(QColor("#E2E8F0"))
            images_table.setItem(row_idx, 2, file_item)

        pairs_table.setRowCount(len(pairs))
        pairs_table.setVisible(bool(pairs))
        for row_idx, pair in enumerate(pairs):
            pairs_table.setRowHeight(row_idx, 64)

            index_item = QTableWidgetItem(str(pair.get("index") or row_idx + 1))
            index_item.setForeground(QColor("#E2E8F0"))
            pairs_table.setItem(row_idx, 0, index_item)

            image_item = QTableWidgetItem(str(pair.get("filename") or ""))
            icon, thumb_size = self._build_thumbnail_icon(pair.get("image_path"))
            if not icon.isNull():
                image_item.setIcon(icon)
                pairs_table.setIconSize(thumb_size)
            image_item.setToolTip(str(pair.get("image_path") or ""))
            image_item.setForeground(QColor("#E2E8F0"))
            pairs_table.setItem(row_idx, 1, image_item)

            prompt_item = QTableWidgetItem(str(pair.get("prompt") or ""))
            if not pair.get("paired", True):
                prompt_item.setForeground(QColor("#F59E0B"))
                prompt_item.setToolTip("Auto-generated from filename")
            else:
                prompt_item.setForeground(QColor("#E2E8F0"))
            pairs_table.setItem(row_idx, 2, prompt_item)

        panel["lbl_loaded"].setText(f"{len(images)} image(s)")
        missing_count = max(0, len(images) - len(prompts))
        if not images:
            hint = "Drop images or choose a folder, then add prompts to preview pairings."
        elif missing_count > 0:
            action_label = (
                "will use filename prompts"
                if str(panel["missing_selector"].currentData() or "filename") == "filename"
                else "will be skipped"
            )
            hint = f"{len(images)} images, {len(prompts)} prompts — {missing_count} images {action_label}."
        elif len(prompts) > len(images):
            hint = f"{len(prompts) - len(images)} extra prompt(s) have no matching image."
        else:
            hint = f"Ready: {len(pairs)} image/prompt pair(s)."
        panel["hint_label"].setText(hint)
        panel["add_btn"].setEnabled(bool(pairs))

    def add_bulk_i2v_to_queue(self, mode_key):
        images, prompts, pairs = self._build_bulk_pairs(mode_key)
        if not images:
            QMessageBox.warning(self, "No Images", "Load images first for bulk image-to-video.")
            return
        if not pairs:
            QMessageBox.warning(self, "No Pairs", "No image/prompt pairs are ready to add to the queue.")
            return

        bulk_sub_mode = str(mode_key or "")
        if bulk_sub_mode not in ("ingredients", "frames_start"):
            QMessageBox.information(
                self,
                "Bulk Mode Unavailable",
                "Bulk image matching is available only for Ingredients and Frames - Start Image modes.",
            )
            return

        current_settings = self._current_generation_settings()

        auto_generated_count = 0
        job_specs = []
        for pair in pairs:
            if not pair.get("paired", True):
                auto_generated_count += 1
            ref_path = pair["image_path"] if bulk_sub_mode == "ingredients" else None
            start_image_path = pair["image_path"] if bulk_sub_mode == "frames_start" else None
            job_specs.append({
                "job_id": str(uuid.uuid4()),
                "prompt": pair["prompt"],
                "model": current_settings["model"],
                "aspect_ratio": current_settings["aspect_ratio"],
                "output_count": current_settings["output_count"],
                "ref_path": ref_path,
                "ref_paths": [ref_path] if ref_path else [],
                "job_type": "video",
                "video_model": current_settings["video_model"],
                "video_sub_mode": current_settings["video_sub_mode"],
                "video_ratio": current_settings.get("video_ratio", current_settings["aspect_ratio"]),
                "video_prompt": current_settings.get("video_prompt", ""),
                "video_upscale": current_settings["video_upscale"],
                "video_output_count": current_settings["video_output_count"],
                "start_image_path": start_image_path,
                "end_image_path": None,
            })

        extra_note = f" ({auto_generated_count} filename prompt(s))" if auto_generated_count else ""
        estimate = self._estimate_video_credits(
            prompt_count=len(pairs),
            output_count=current_settings["video_output_count"],
            upscale=current_settings["video_upscale"],
            video_model=current_settings["video_model"],
        )
        self._start_bulk_queue_add(
            job_specs,
            success_logs=[
                f"[CREDITS] Estimated cost: ~{estimate} credits for {len(pairs)} prompt(s) x {current_settings['video_output_count']} output(s)",
                f"Added {len(pairs)} bulk image-to-video task(s) in {bulk_sub_mode.replace('_', ' ')} mode{extra_note}.",
            ],
            progress_title=f"Adding {len(job_specs)} bulk video job(s)...",
        )

    def _sync_primary_reference_path(self):
        return self.current_ref_paths[0] if getattr(self, "current_ref_paths", None) else None

    def _rebuild_reference_image_rows(self):
        if not hasattr(self, "ref_items_layout"):
            return

        while self.ref_items_layout.count():
            item = self.ref_items_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

        for path in list(getattr(self, "current_ref_paths", []) or []):
            row_widget = QWidget()
            row_layout = QHBoxLayout(row_widget)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(8)

            label = QLabel(f"📎 {os.path.basename(path)}")
            label.setObjectName("refStatusLabel")
            label.setToolTip(path)

            btn_remove = QPushButton("X")
            btn_remove.setObjectName("refClearButton")
            btn_remove.setProperty("role", "danger")
            btn_remove.setFixedWidth(28)
            btn_remove.setMinimumHeight(28)
            btn_remove.setToolTip(f"Remove {os.path.basename(path)}")
            btn_remove.clicked.connect(lambda _=False, target_path=path: self._remove_reference_image(target_path))

            row_layout.addWidget(label, 1)
            row_layout.addWidget(btn_remove)
            self.ref_items_layout.addWidget(row_widget)

        self.ref_items_container.setVisible(bool(getattr(self, "current_ref_paths", [])))

    def _update_reference_image_ui(self):
        ref_count = len(getattr(self, "current_ref_paths", []) or [])

        if hasattr(self, "lbl_ref_status"):
            if ref_count <= 0:
                self.lbl_ref_status.setText("None")
            elif ref_count == 1:
                self.lbl_ref_status.setText(f"1 image selected: {os.path.basename(self.current_ref_paths[0])}")
            else:
                self.lbl_ref_status.setText(f"{ref_count} images selected")

        self._rebuild_reference_image_rows()
        self.btn_clear_ref.setVisible(ref_count > 0)
        self._on_generation_settings_changed()

    def _add_reference_image(self, path):
        normalized = str(path or "").strip()
        if not normalized:
            return False
        if not os.path.exists(normalized):
            return False
        if normalized in self.current_ref_paths:
            return False
        self.current_ref_paths.append(normalized)
        self._update_reference_image_ui()
        return True

    def _remove_reference_image(self, path):
        normalized = str(path or "").strip()
        self.current_ref_paths = [item for item in self.current_ref_paths if str(item or "").strip() != normalized]
        self._update_reference_image_ui()

    def clear_reference_image(self):
        self.current_ref_paths = []
        self._update_reference_image_ui()

    def select_reference_image(self):
        file_paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select Reference Image(s)",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.bmp *.gif)",
        )
        if file_paths:
            added_any = False
            for file_path in file_paths:
                added_any = self._add_reference_image(file_path) or added_any
            if not added_any:
                self._update_reference_image_ui()
        elif not self.current_ref_paths:
            self._update_reference_image_ui()

    def _rebuild_pipeline_reference_rows(self):
        if not hasattr(self, "pipe_ref_items_layout"):
            return

        while self.pipe_ref_items_layout.count():
            item = self.pipe_ref_items_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

        for path in list(getattr(self, "current_pipe_ref_paths", []) or []):
            row_widget = QWidget()
            row_layout = QHBoxLayout(row_widget)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(8)

            label = QLabel(f"📎 {os.path.basename(path)}")
            label.setObjectName("refStatusLabel")
            label.setToolTip(path)

            btn_remove = QPushButton("X")
            btn_remove.setObjectName("refClearButton")
            btn_remove.setProperty("role", "danger")
            btn_remove.setFixedWidth(28)
            btn_remove.setMinimumHeight(28)
            btn_remove.setToolTip(f"Remove {os.path.basename(path)}")
            btn_remove.clicked.connect(lambda _=False, target_path=path: self._remove_pipeline_reference_image(target_path))

            row_layout.addWidget(label, 1)
            row_layout.addWidget(btn_remove)
            self.pipe_ref_items_layout.addWidget(row_widget)

        self.pipe_ref_items_container.setVisible(bool(getattr(self, "current_pipe_ref_paths", [])))

    def _update_pipeline_reference_ui(self):
        ref_count = len(getattr(self, "current_pipe_ref_paths", []) or [])

        if hasattr(self, "pipe_lbl_ref_status"):
            if ref_count <= 0:
                self.pipe_lbl_ref_status.setText("None")
            elif ref_count == 1:
                self.pipe_lbl_ref_status.setText(f"1 image selected: {os.path.basename(self.current_pipe_ref_paths[0])}")
            else:
                self.pipe_lbl_ref_status.setText(f"{ref_count} images selected")

        self._rebuild_pipeline_reference_rows()
        if hasattr(self, "pipe_btn_clear_refs"):
            self.pipe_btn_clear_refs.setVisible(ref_count > 0)
        self._on_generation_settings_changed()

    def _add_pipeline_reference_image(self, path):
        normalized = str(path or "").strip()
        if not normalized or not os.path.exists(normalized):
            return False
        if normalized in self.current_pipe_ref_paths:
            return False
        self.current_pipe_ref_paths.append(normalized)
        self._update_pipeline_reference_ui()
        return True

    def _remove_pipeline_reference_image(self, path):
        normalized = str(path or "").strip()
        self.current_pipe_ref_paths = [
            item for item in self.current_pipe_ref_paths if str(item or "").strip() != normalized
        ]
        self._update_pipeline_reference_ui()

    def clear_pipeline_reference_images(self):
        self.current_pipe_ref_paths = []
        self._update_pipeline_reference_ui()

    def select_pipeline_reference_images(self):
        file_paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select Pipeline Reference Image(s)",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.bmp *.gif)",
        )
        if file_paths:
            added_any = False
            for file_path in file_paths:
                added_any = self._add_pipeline_reference_image(file_path) or added_any
            if not added_any:
                self._update_pipeline_reference_ui()
        elif not self.current_pipe_ref_paths:
            self._update_pipeline_reference_ui()

    def _update_single_reference_image_ui(self):
        has_ref = bool(self.current_ref_path)
        if hasattr(self, "lbl_ref_single"):
            self.lbl_ref_single.setText(os.path.basename(self.current_ref_path) if has_ref else "None")
        if hasattr(self, "btn_ref_single_clear"):
            self.btn_ref_single_clear.setVisible(has_ref)
        self._on_generation_settings_changed()

    def clear_single_reference_image(self):
        self.current_ref_path = None
        self._update_single_reference_image_ui()

    def select_single_reference_image(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Reference Image",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.bmp *.gif)",
        )
        if file_path:
            self.current_ref_path = file_path
        self._update_single_reference_image_ui()

    def clear_start_image(self):
        self.current_start_image_path = None
        if hasattr(self, "lbl_start_image"):
            self.lbl_start_image.setText("None")
        if hasattr(self, "btn_clear_start_image"):
            self.btn_clear_start_image.setVisible(False)
        self._on_generation_settings_changed()

    def select_start_image(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Start Image",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.bmp *.gif)",
        )
        if file_path:
            self.current_start_image_path = file_path
            self.lbl_start_image.setText(os.path.basename(file_path))
            self.btn_clear_start_image.setVisible(True)
            self._on_generation_settings_changed()
        elif not self.current_start_image_path and hasattr(self, "lbl_start_image"):
            self.lbl_start_image.setText("None")

    def clear_end_image(self):
        self.current_end_image_path = None
        if hasattr(self, "lbl_end_image"):
            self.lbl_end_image.setText("None")
        if hasattr(self, "btn_clear_end_image"):
            self.btn_clear_end_image.setVisible(False)
        self._on_generation_settings_changed()

    def select_end_image(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select End Image",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.bmp *.gif)",
        )
        if file_path:
            self.current_end_image_path = file_path
            self.lbl_end_image.setText(os.path.basename(file_path))
            self.btn_clear_end_image.setVisible(True)
            self._on_generation_settings_changed()
        elif not self.current_end_image_path and hasattr(self, "lbl_end_image"):
            self.lbl_end_image.setText("None")

    def _add_pipeline_to_queue(self):
        current_settings = self._current_generation_settings()
        if current_settings.get("job_type") != "pipeline":
            return

        text = self.pipe_txt_img_prompts.toPlainText().strip() if hasattr(self, "pipe_txt_img_prompts") else ""
        if not text:
            QMessageBox.warning(self, "No Prompts", "Enter at least one image prompt.")
            return

        image_prompts = [line.strip() for line in text.splitlines() if line.strip()]
        raw_video_lines = []
        if hasattr(self, "pipe_txt_vid_prompts"):
            raw_video_lines = [line.strip() for line in self.pipe_txt_vid_prompts.toPlainText().splitlines()]

        custom_video_prompt_count = 0
        job_specs = []
        for idx, image_prompt in enumerate(image_prompts):
            target_video_prompt = (
                raw_video_lines[idx]
                if idx < len(raw_video_lines) and raw_video_lines[idx]
                else current_settings.get("video_prompt", "animate")
            )
            if idx < len(raw_video_lines) and raw_video_lines[idx]:
                custom_video_prompt_count += 1
            job_specs.append({
                "job_id": str(uuid.uuid4()),
                "prompt": image_prompt,
                "model": current_settings["model"],
                "aspect_ratio": current_settings["aspect_ratio"],
                "output_count": current_settings["output_count"],
                "ref_path": current_settings["ref_path"],
                "ref_paths": current_settings.get("ref_paths"),
                "job_type": "pipeline",
                "video_model": current_settings["video_model"],
                "video_sub_mode": current_settings["video_sub_mode"],
                "video_ratio": current_settings.get("video_ratio", current_settings["aspect_ratio"]),
                "video_prompt": target_video_prompt,
                "video_upscale": current_settings["video_upscale"],
                "video_output_count": current_settings["video_output_count"],
                "start_image_path": None,
                "end_image_path": None,
            })

        animate_count = max(0, len(image_prompts) - custom_video_prompt_count)
        estimate = self._estimate_video_credits(
            prompt_count=len(image_prompts),
            output_count=1,
            upscale=current_settings["video_upscale"],
            video_model=current_settings["video_model"],
        )
        msg = f"[PIPELINE] Added {len(image_prompts)} pipeline jobs. {custom_video_prompt_count} with custom video prompts"
        if animate_count > 0:
            msg += f", {animate_count} with 'animate' default"
        self._start_bulk_queue_add(
            job_specs,
            success_logs=[
                f"[CREDITS] Estimated cost: ~{estimate} credits for {len(image_prompts)} pipeline prompt(s)",
                msg,
            ],
            after_success=lambda: self.pipe_txt_img_prompts.clear() if hasattr(self, "pipe_txt_img_prompts") else None,
            progress_title=f"Adding {len(job_specs)} pipeline job(s)...",
        )

    def _current_video_sub_mode(self):
        if not hasattr(self, "mode_tabs"):
            return "text_to_video"
        idx = self.mode_tabs.currentIndex()
        if idx == 1:
            return "text_to_video"
        if idx == 2:
            return "ingredients"
        if idx == 3 and hasattr(self, "frm_cmb_mode"):
            return str(self.frm_cmb_mode.currentData() or "frames_start")
        if idx == 4 and hasattr(self, "pipe_cmb_vid_mode"):
            return str(self.pipe_cmb_vid_mode.currentData() or "ingredients")
        return "text_to_video"

    def _validate_video_job_inputs(self, settings, *, bulk_mode_override=None):
        sub_mode = str(bulk_mode_override or settings.get("video_sub_mode") or "text_to_video")
        ref_path = str(settings.get("ref_path") or "").strip()
        start_image_path = str(settings.get("start_image_path") or "").strip()
        end_image_path = str(settings.get("end_image_path") or "").strip()

        if sub_mode == "ingredients" and not ref_path:
            QMessageBox.warning(self, "Missing Reference Image", "Ingredients mode needs a reference image.")
            return False
        if sub_mode == "frames_start" and not start_image_path:
            QMessageBox.warning(self, "Missing Start Image", "Frames - Start Image mode needs a start image.")
            return False
        if sub_mode == "frames_start_end":
            if not start_image_path:
                QMessageBox.warning(self, "Missing Start Image", "Frames - Start + End mode needs a start image.")
                return False
            if not end_image_path:
                QMessageBox.warning(self, "Missing End Image", "Frames - Start + End mode needs an end image.")
                return False
        return True

    def _current_job_type(self):
        if hasattr(self, "mode_tabs") and self.mode_tabs.currentIndex() == 4:
            return "pipeline"
        if hasattr(self, "mode_tabs") and self.mode_tabs.currentIndex() > 0:
            return "video"
        return "image"

    def _current_parallel_slots(self):
        if not hasattr(self, "mode_tabs"):
            return 1
        idx = self.mode_tabs.currentIndex()
        selector = {
            0: getattr(self, "img_cmb_parallel", None),
            1: getattr(self, "t2v_cmb_parallel", None),
            2: getattr(self, "ref_cmb_parallel", None),
            3: getattr(self, "frm_cmb_parallel", None),
            4: getattr(self, "pipe_cmb_parallel", None),
        }.get(idx)
        if selector is None:
            return 1
        return int(selector.currentData() or 1)

    def _current_generation_settings(self):
        required_controls = (
            "img_cmb_outputs",
            "t2v_cmb_outputs",
            "ref_cmb_outputs",
            "frm_cmb_outputs",
            "pipe_cmb_vid_ratio",
        )
        if not hasattr(self, "mode_tabs") or not all(hasattr(self, name) for name in required_controls):
            return {
                "job_type": "image",
                "model": "Imagen 4",
                "aspect_ratio": "Landscape (16:9)",
                "output_count": 1,
                "ref_path": None,
                "ref_paths": [],
                "video_model": "",
                "video_sub_mode": "",
                "video_ratio": "",
                "video_prompt": "",
                "video_upscale": "none",
                "video_output_count": 1,
                "start_image_path": None,
                "end_image_path": None,
            }

        idx = self.mode_tabs.currentIndex()
        if idx == 0:
            output_count = int(self.img_cmb_outputs.currentData() or 1)
            ref_paths = list(self.current_ref_paths)
            return {
                "job_type": "image",
                "model": str(self.img_cmb_model.currentText() or "Imagen 4"),
                "aspect_ratio": str(self.img_cmb_ratio.currentData() or self.img_cmb_ratio.currentText() or "Landscape (16:9)"),
                "output_count": output_count,
                "ref_path": ref_paths[0] if ref_paths else None,
                "ref_paths": ref_paths,
                "video_model": "",
                "video_sub_mode": "",
                "video_ratio": "",
                "video_prompt": "",
                "video_upscale": "none",
                "video_output_count": 1,
                "start_image_path": None,
                "end_image_path": None,
            }

        if idx == 1:
            output_count = int(self.t2v_cmb_outputs.currentData() or 1)
            model = str(self.t2v_cmb_quality.currentText() or "Veo 3.1 - Fast")
            return {
                "job_type": "video",
                "model": model,
                "aspect_ratio": str(self.t2v_cmb_ratio.currentData() or self.t2v_cmb_ratio.currentText() or "Landscape (16:9)"),
                "output_count": output_count,
                "ref_path": None,
                "ref_paths": [],
                "video_model": model,
                "video_sub_mode": "text_to_video",
                "video_ratio": str(self.t2v_cmb_ratio.currentData() or self.t2v_cmb_ratio.currentText() or "Landscape (16:9)"),
                "video_prompt": "",
                "video_upscale": str(self.t2v_cmb_upscale.currentData() or "none"),
                "video_output_count": output_count,
                "start_image_path": None,
                "end_image_path": None,
            }

        if idx == 2:
            output_count = int(self.ref_cmb_outputs.currentData() or 1)
            model = str(self.ref_cmb_quality.currentText() or "Veo 3.1 - Fast")
            ref_path = str(self.current_ref_path or "").strip() or None
            return {
                "job_type": "video",
                "model": model,
                "aspect_ratio": str(self.ref_cmb_ratio.currentData() or self.ref_cmb_ratio.currentText() or "Landscape (16:9)"),
                "output_count": output_count,
                "ref_path": ref_path,
                "ref_paths": [ref_path] if ref_path else [],
                "video_model": model,
                "video_sub_mode": "ingredients",
                "video_ratio": str(self.ref_cmb_ratio.currentData() or self.ref_cmb_ratio.currentText() or "Landscape (16:9)"),
                "video_prompt": "",
                "video_upscale": str(self.ref_cmb_upscale.currentData() or "none"),
                "video_output_count": output_count,
                "start_image_path": None,
                "end_image_path": None,
            }

        if idx == 3:
            frame_mode = self._current_video_sub_mode()
            output_count = int(self.frm_cmb_outputs.currentData() or 1)
            model = str(self.frm_cmb_quality.currentText() or "Veo 3.1 - Fast")
            return {
                "job_type": "video",
                "model": model,
                "aspect_ratio": str(self.frm_cmb_ratio.currentData() or self.frm_cmb_ratio.currentText() or "Landscape (16:9)"),
                "output_count": output_count,
                "ref_path": None,
                "ref_paths": [],
                "video_model": model,
                "video_sub_mode": frame_mode,
                "video_ratio": str(self.frm_cmb_ratio.currentData() or self.frm_cmb_ratio.currentText() or "Landscape (16:9)"),
                "video_prompt": "",
                "video_upscale": str(self.frm_cmb_upscale.currentData() or "none"),
                "video_output_count": output_count,
                "start_image_path": self.current_start_image_path,
                "end_image_path": self.current_end_image_path if frame_mode == "frames_start_end" else None,
            }

        ref_paths = list(getattr(self, "current_pipe_ref_paths", []) or [])
        image_model = str(self.pipe_cmb_img_model.currentText() or "Imagen 4")
        video_model = str(self.pipe_cmb_vid_quality.currentText() or "Veo 3.1 - Fast")
        return {
            "job_type": "pipeline",
            "model": image_model,
            "aspect_ratio": str(self.pipe_cmb_img_ratio.currentData() or self.pipe_cmb_img_ratio.currentText() or "Landscape (16:9)"),
            "output_count": 1,
            "ref_path": ref_paths[0] if ref_paths else None,
            "ref_paths": ref_paths,
            "video_model": video_model,
            "video_sub_mode": str(self.pipe_cmb_vid_mode.currentData() or "ingredients"),
            "video_ratio": str(self.pipe_cmb_vid_ratio.currentData() or self.pipe_cmb_vid_ratio.currentText() or "Landscape (16:9)"),
            "video_prompt": str(self.pipe_txt_vid_prompt.text() or "").strip() or "animate",
            "video_upscale": str(self.pipe_cmb_upscale.currentData() or "none"),
            "video_output_count": 1,
            "start_image_path": None,
            "end_image_path": None,
        }

    def _sync_generation_mode_ui(self):
        frame_mode = self._current_video_sub_mode()
        pipeline_active = hasattr(self, "mode_tabs") and self.mode_tabs.currentIndex() == 4
        if hasattr(self, "end_row"):
            self.end_row.setVisible(frame_mode == "frames_start_end")
        if hasattr(self, "frm_bulk_group"):
            self.frm_bulk_group.setVisible(frame_mode == "frames_start")
        if hasattr(self, "frm_bulk_separator"):
            self.frm_bulk_separator.setVisible(frame_mode == "frames_start")
        if hasattr(self, "btn_ref_single_clear"):
            self.btn_ref_single_clear.setVisible(bool(self.current_ref_path))
        if hasattr(self, "btn_clear_start_image"):
            self.btn_clear_start_image.setVisible(bool(self.current_start_image_path))
        if hasattr(self, "btn_clear_end_image"):
            self.btn_clear_end_image.setVisible(frame_mode == "frames_start_end" and bool(self.current_end_image_path))
        if hasattr(self, "prompts_group"):
            self.prompts_group.setEnabled(not pipeline_active)
            self.prompts_group.setTitle("Prompts Input" if not pipeline_active else "Prompts Input (use Pipeline tab prompts)")
        if hasattr(self, "prompts_input"):
            self.prompts_input.setPlaceholderText(
                "Paste your prompts here, one per line..."
                if not pipeline_active
                else "Pipeline tab uses its own Image Prompts + Video Prompts editors."
            )
        if hasattr(self, "btn_add_to_queue"):
            self.btn_add_to_queue.setEnabled(not pipeline_active)
        self._update_runtime_badges()
        self._adjust_mode_tabs_height()

    def _import_prompts_txt(self):
        if not hasattr(self, "prompts_input"):
            return
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Import Prompts TXT",
            "",
            "Text Files (*.txt);;All Files (*.*)",
        )
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                content = handle.read()
        except UnicodeDecodeError:
            with open(file_path, "r", encoding="latin-1") as handle:
                content = handle.read()
        except Exception as exc:
            QMessageBox.warning(self, "Import Failed", f"Unable to read file:\n{exc}")
            return

        self.prompts_input.setPlainText(content)

    def add_prompts_to_queue(self):
        if hasattr(self, "mode_tabs") and self.mode_tabs.currentIndex() == 4:
            QMessageBox.information(self, "Use Pipeline Add Button", "Use the Pipeline tab's 'Add All to Queue' button.")
            return
        text = self.prompts_input.toPlainText().strip()
        if not text:
            QMessageBox.warning(self, "Warning", "Please enter at least one prompt.")
            return

        current_settings = self._current_generation_settings()
        if current_settings["job_type"] == "video" and not self._validate_video_job_inputs(current_settings):
            return
        
        prompts = [p.strip() for p in text.split('\n') if p.strip()]
        job_specs = []
        for prompt_text in prompts:
            job_specs.append({
                "job_id": str(uuid.uuid4()),
                "prompt": prompt_text,
                "model": current_settings["model"],
                "aspect_ratio": current_settings["aspect_ratio"],
                "output_count": current_settings["output_count"],
                "ref_path": current_settings["ref_path"],
                "ref_paths": current_settings.get("ref_paths"),
                "job_type": current_settings["job_type"],
                "video_model": current_settings["video_model"],
                "video_sub_mode": current_settings["video_sub_mode"],
                "video_ratio": current_settings.get("video_ratio", current_settings["aspect_ratio"]),
                "video_prompt": current_settings.get("video_prompt", ""),
                "video_upscale": current_settings["video_upscale"],
                "video_output_count": current_settings["video_output_count"],
                "start_image_path": current_settings["start_image_path"],
                "end_image_path": current_settings["end_image_path"],
            })

        success_logs = []
        if current_settings["job_type"] in ("video", "pipeline"):
            estimate = self._estimate_video_credits(
                prompt_count=len(prompts),
                output_count=current_settings["video_output_count"],
                upscale=current_settings["video_upscale"],
                video_model=current_settings["video_model"],
            )
            success_logs.append(
                f"[CREDITS] Estimated cost: ~{estimate} credits for {len(prompts)} prompt(s) x {current_settings['video_output_count']} output(s)"
            )
        success_logs.append(f"Added {len(prompts)} prompts to queue.")
        self._start_bulk_queue_add(
            job_specs,
            success_logs=success_logs,
            after_success=self.prompts_input.clear,
            progress_title=f"Adding {len(job_specs)} prompt(s) to queue...",
        )

    def clear_queue(self):
        reply = QMessageBox.question(
            self,
            'Confirm Clear',
            'Clear all task queue history (pending, running, completed, failed) in one click?',
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            if self.queue_manager and self.queue_manager.isRunning():
                self.pending_clear_all = True
                self.queue_manager.stop()
                self._set_queue_controls_state("stopping")
                self.append_log("Stop requested. Running jobs are being cancelled and the queue will auto-clear.")
                return

            from src.db.db_manager import clear_all_jobs
            self._start_background_task(
                clear_all_jobs,
                on_finished=self._on_clear_queue_finished,
            )

    def clear_completed_jobs_from_queue(self):
        self._start_background_task(
            clear_completed_jobs,
            on_finished=self._on_clear_completed_finished,
        )

    def load_queue_table(self, jobs=None):
        if jobs is None:
            self._request_queue_snapshot()
            return

        self._apply_queue_snapshot(self._slim_jobs_for_ui(jobs))

    def show_queue_context_menu(self, position):
        from PySide6.QtWidgets import QMenu
        menu = QMenu()
        remove_action = menu.addAction("Remove Selected Task")
        action = menu.exec(self.queue_table.viewport().mapToGlobal(position))
        
        if action == remove_action:
            index = self.queue_table.indexAt(position)
            if not index.isValid():
                selected_rows = self.queue_table.selectionModel().selectedRows()
                index = selected_rows[0] if selected_rows else None
            if index is not None and index.isValid():
                row = index.row()
                job_id = self.queue_model.job_id_at(row)
                from src.db.db_manager import delete_job
                self._start_background_task(
                    delete_job,
                    job_id,
                    on_finished=lambda _result, job_id=job_id: self._on_manual_job_removed(job_id),
                )

    def _on_manual_job_removed(self, job_id):
        self.append_log("Removed job manually.")
        self.load_queue_table()

    def _on_clear_queue_finished(self, deleted_count):
        self.append_log(f"Queue cleared manually. Removed {int(deleted_count or 0)} task(s).")
        self.load_queue_table()
        self.load_failed_jobs()

    def _on_clear_completed_finished(self, deleted_count):
        self.append_log(f"Cleared {int(deleted_count or 0)} completed job(s) from queue history.")
        self.load_queue_table()
        self.load_failed_jobs()

    def _on_force_clear_finished(self, deleted_count):
        self.append_log(f"[SYSTEM] Instant queue clear complete. Removed {int(deleted_count or 0)} task(s).")
        self.load_queue_table()
        self.load_failed_jobs()

    def _on_auto_clear_after_stop_finished(self, deleted_count):
        self.append_log(f"Queue auto-cleared after stop. Removed {int(deleted_count or 0)} task(s).")
        self.load_queue_table()
        self.load_failed_jobs()

    def start_queue_manager(self):
        if self.queue_manager and self.queue_manager.isRunning():
            return

        if self.queue_manager and not self.queue_manager.isRunning():
            try:
                self.queue_manager.deleteLater()
            except Exception:
                pass
            self.queue_manager = None

        self.pending_clear_all = False
        self.queue_paused = False
        self.queue_stopping = False

        self.account_runtime_state = {}
        self._refresh_account_runtime_cells()

        current_settings = self._current_generation_settings()
        pending_count = self._cached_pending_count()
        self.append_log(
            f"[SETTINGS] Pending queue keeps per-job settings for {pending_count} task(s). "
            f"Current panel default: {current_settings['job_type']}, "
            f"{current_settings['model']}, {current_settings['aspect_ratio']}, "
            f"x{current_settings['output_count']}, "
            f"references={len(current_settings.get('ref_paths') or [])}, "
            f"upscale={current_settings['video_upscale']}."
        )

        selected_slots = max(1, min(5, self._current_parallel_slots()))
        set_setting("slots_per_account", str(selected_slots))
        self.spin_slots_per_account.setValue(selected_slots)

        if selected_slots > 1:
            current_stagger = max(0.0, min(60.0, float(self.spin_same_account_stagger.value())))
            minimum_stagger = 1.0
            if current_stagger < minimum_stagger:
                current_stagger = minimum_stagger
                set_setting("same_account_stagger_seconds", str(current_stagger))
                self.spin_same_account_stagger.setValue(current_stagger)
                self.append_log(
                    f"[SETTINGS] Auto-applied minimum {minimum_stagger:.1f}s same-account stagger for parallel slots."
                )

        if selected_slots > 1 and not self.chk_profile_clone.isChecked():
            set_setting("enable_profile_clones", "1")
            self.chk_profile_clone.setChecked(True)
            self.append_log("[SETTINGS] Auto-enabled profile cloning for parallel slots.")

        self.append_log(f"[SETTINGS] Parallel tasks selected: {selected_slots} slot(s)/account.")
        self._update_runtime_badges()
             
        self.queue_manager = AsyncQueueManager()
        self.queue_manager.signals.log_msg.connect(self.append_log, Qt.QueuedConnection)
        self.queue_manager.signals.job_updated.connect(self.on_job_updated, Qt.QueuedConnection)
        self.queue_manager.signals.account_runtime.connect(self.on_account_runtime, Qt.QueuedConnection)
        self.queue_manager.signals.account_auth_status.connect(self._on_account_auth_status, Qt.QueuedConnection)
        self.queue_manager.signals.show_warning.connect(self._show_session_warning, Qt.QueuedConnection)
        self.queue_manager.signals.warmup_progress.connect(self.warmup_progress_signal.emit, Qt.QueuedConnection)
        self.queue_manager.signals.warmup_complete.connect(self.warmup_complete_signal.emit, Qt.QueuedConnection)
        self.queue_manager.finished.connect(self.on_queue_manager_finished, Qt.QueuedConnection)
        self._on_generation_started()
        if hasattr(self, "progress_timer"):
            self.progress_timer.start()
        self.queue_manager.start()

        self._set_queue_controls_state("running")
        self._show_session_warning("")
        self.append_log("Queue Manager Started. Dispatching bots...")

    def pause_queue_manager(self):
        if not self.queue_manager or not self.queue_manager.isRunning():
            return
        if self.queue_paused or self.queue_stopping:
            return

        self.queue_manager.pause_dispatch()
        self._set_queue_controls_state("paused")
        running = sum(1 for runtime in self.account_runtime_state.values() if int(runtime.get("active_slots", 0) or 0) > 0)
        self.append_log(
            f"[SYSTEM] Queue paused. {running} account(s) still running will finish current jobs. No new jobs will dispatch until resumed."
        )

    def resume_queue_manager(self):
        if not self.queue_manager or not self.queue_manager.isRunning():
            return
        if not self.queue_paused or self.queue_stopping:
            return

        self.queue_manager.resume_dispatch()
        self._set_queue_controls_state("running")
        pending = self._cached_pending_count()
        self.append_log(f"[SYSTEM] Queue resumed. {pending} pending job(s) ready for dispatch.")

    def stop_queue_manager(self):
        if self.queue_manager and self.queue_manager.isRunning():
            self.queue_manager.stop()
            self._set_queue_controls_state("stopping")
            self.append_log("[SYSTEM] Stop requested. Active jobs are being cancelled and queue state will reset.")

    def force_stop_and_clear_queue(self):
        reply = QMessageBox.warning(
            self,
            "Force Stop + Instant Clear",
            "This will cancel active jobs immediately and clear full queue now.\n"
            "Running tasks may be lost. Continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        from src.db.db_manager import clear_all_jobs

        if self.queue_manager and self.queue_manager.isRunning():
            self.pending_clear_all = False
            self.queue_manager.force_stop()
            self._set_queue_controls_state("stopping")
            self.append_log("[SYSTEM] Force stop sent. Active jobs cancelled immediately.")

        self._start_background_task(
            clear_all_jobs,
            on_finished=self._on_force_clear_finished,
        )

    def on_queue_manager_finished(self):
        finished_manager = self.sender()
        if finished_manager is not None and self.queue_manager is not None and finished_manager is not self.queue_manager:
            try:
                finished_manager.deleteLater()
            except Exception:
                pass
            return

        if finished_manager is None:
            finished_manager = self.queue_manager

        self.queue_manager = None
        self._set_queue_controls_state("stopped")
        if hasattr(self, "progress_timer"):
            self.progress_timer.stop()
        self._on_queue_stopped()
        self.append_log("Queue Manager safely stopped and contexts closed.")
        self.load_queue_table()
        self.load_failed_jobs()
        self.account_runtime_state = {
            str(self.acc_table.item(row, 1).data(Qt.UserRole) or self.acc_table.item(row, 1).text() or ""): {
                "status": "idle",
                "cooldown_until": 0.0,
                "active_slots": 0,
                "total_slots": 1,
                "detail": "Queue stopped",
            }
            for row in range(self.acc_table.rowCount())
            if self.acc_table.item(row, 1) is not None
        }
        self._refresh_account_runtime_cells()
        if self.pending_clear_all:
            from src.db.db_manager import clear_all_jobs
            self.pending_clear_all = False
            self._start_background_task(
                clear_all_jobs,
                on_finished=self._on_auto_clear_after_stop_finished,
            )
        if finished_manager is not None:
            try:
                finished_manager.deleteLater()
            except Exception:
                pass

    def on_job_updated(self, job_id, status, account, error_msg):
        self._request_queue_snapshot()
        if str(status or "").strip().lower() in ("failed", "pending", "completed"):
            self._schedule_failed_jobs_refresh()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        width = self.width()
        height = self.height()

        if hasattr(self, "sidebar"):
            self.sidebar.setFixedWidth(140 if width < 1400 else 180)
        if hasattr(self, "progress_widget"):
            self.progress_widget.setFixedHeight(24 if height < 800 else 28)
        if hasattr(self, "dashboard_body_splitter") and hasattr(self, "mode_tabs"):
            self._adjust_mode_tabs_height()
        if hasattr(self, "tabs") and self.tabs.currentWidget() is getattr(self, "tab_live_generation", None):
            self.ui_throttler.schedule("live_grid_resize", self._refresh_live_grid)

    def _on_account_auth_status(self, account_name, status, message):
        """Handle auth status changes from queue manager (logged_in / expired)."""
        if not hasattr(self, "_runtime_auth_status"):
            self._runtime_auth_status = {}

        if status == "expired":
            self._runtime_auth_status[account_name] = "expired"
            self._append_log(f"[{account_name}] Session expired: {message}")
        elif status == "logged_in":
            # Clear expired status on successful generation
            self._runtime_auth_status.pop(account_name, None)

        # Trigger immediate status refresh
        self._refresh_login_statuses()

    def on_account_runtime(self, account_name, runtime_status, cooldown_until_ts, active_slots, total_slots, detail):
        self.account_runtime_state[str(account_name)] = {
            "status": str(runtime_status or "idle"),
            "cooldown_until": float(cooldown_until_ts or 0.0),
            "active_slots": int(active_slots or 0),
            "total_slots": int(total_slots or 1),
            "detail": str(detail or ""),
        }
        self._refresh_account_runtime_cells()

    def _shutdown_worker_thread(self, worker, timeout_ms=4000):
        if not worker:
            return
        try:
            if hasattr(worker, "stop"):
                worker.stop()
        except Exception:
            pass
        try:
            worker.requestInterruption()
        except Exception:
            pass
        try:
            worker.wait(max(0, int(timeout_ms)))
        except Exception:
            pass

    def closeEvent(self, event):
        """Immediate exit — kill browsers, force terminate. No crash dialog."""
        event.accept()
        try:
            process_tracker.kill_all()
        except Exception:
            pass
        os._exit(0)

    def _kill_zombie_browsers(self, startup=False):
        try:
            killed = process_tracker.load_and_kill_stale() if startup else process_tracker.kill_all()
            if killed:
                phase = "STARTUP" if startup else "CLEANUP"
                print(f"[{phase}] Killed {killed} app browser process(es).")
        except Exception:
            pass

    def _cleanup_stale_locks(self):
        for base_dir in (get_sessions_dir(), get_session_clones_dir()):
            try:
                if not base_dir.exists():
                    continue
            except Exception:
                continue

            for pattern in ("Singleton*", "lockfile"):
                try:
                    lock_paths = list(base_dir.rglob(pattern))
                except Exception:
                    continue

                for lock_path in lock_paths:
                    try:
                        if lock_path.is_file():
                            lock_path.unlink()
                    except Exception:
                        pass



