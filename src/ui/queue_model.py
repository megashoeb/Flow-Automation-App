from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor


class QueueTableModel(QAbstractTableModel):
    HEADERS = ["#", "Prompt", "Type", "Status"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._jobs = []

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._jobs)

    def columnCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self.HEADERS)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]
        return None

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._jobs)):
            return None

        job = self._jobs[index.row()]
        col = index.column()
        status = str(job.get("status") or "").strip().lower()

        if role == Qt.DisplayRole:
            if col == 0:  # #
                if job.get("is_retry"):
                    display_no = job.get("output_index") or job.get("queue_no")
                else:
                    display_no = job.get("queue_no")
                return str(display_no or index.row() + 1)
            if col == 1:  # Prompt
                prompt = str(job.get("prompt") or "")
                return (prompt[:60] + "...") if len(prompt) > 60 else prompt
            if col == 2:  # Type
                return str(job.get("job_type_display") or "")
            if col == 3:  # Status — short labels
                return {
                    "pending": "PENDING",
                    "running": "RUN",
                    "completed": "DONE",
                    "failed": "FAIL",
                    "moderated": "MOD",
                }.get(status, status.upper())

        if role == Qt.ToolTipRole:
            if col == 1:
                # Full prompt + model + account in tooltip
                parts = [str(job.get("prompt") or "")]
                model = str(job.get("model_display") or "")
                account = str(job.get("account") or "")
                progress = str(job.get("progress") or "")
                if model:
                    parts.append(f"Model: {model}")
                if account:
                    parts.append(f"Account: {account}")
                if progress:
                    parts.append(f"Progress: {progress}")
                return "\n".join(parts)
            if col == 3:
                progress = str(job.get("progress") or "")
                return f"{status.upper()} {progress}" if progress else status.upper()

        if role == Qt.TextAlignmentRole and col in (0, 2, 3):
            return int(Qt.AlignCenter)

        if role == Qt.FontRole:
            if col == 3:
                from PySide6.QtGui import QFont
                f = QFont()
                f.setBold(True)
                f.setPointSize(8)
                return f

        if role == Qt.ForegroundRole:
            if col == 3:
                return {
                    "pending": QColor("#F59E0B"),
                    "running": QColor("#3B82F6"),
                    "completed": QColor("#22C55E"),
                    "failed": QColor("#EF4444"),
                    "moderated": QColor("#F59E0B"),
                }.get(status, QColor("#F8FAFC"))
            if col == 2:
                return QColor("#94A3B8")
            return QColor("#CBD5E1")

        if role == Qt.BackgroundRole:
            if col == 3:
                return {
                    "pending": QColor(245, 158, 11, 25),
                    "running": QColor(59, 130, 246, 30),
                    "completed": QColor(34, 197, 94, 25),
                    "failed": QColor(239, 68, 68, 30),
                    "moderated": QColor(245, 158, 11, 25),
                }.get(status)
            if job.get("is_retry"):
                return QColor("#1A2744")

        if role == Qt.UserRole:
            return job.get("job_id")

        return None

    def set_jobs(self, jobs):
        self.beginResetModel()
        self._jobs = list(jobs or [])
        self.endResetModel()

    def bulk_update(self, updates):
        if not updates:
            return

        rows = sorted(int(row) for row in updates.keys())
        for row in rows:
            if 0 <= row < len(self._jobs):
                self._jobs[row] = dict(updates[row])

        start = rows[0]
        end = rows[-1]
        self.dataChanged.emit(self.index(start, 0), self.index(end, self.columnCount() - 1))

    def job_id_at(self, row):
        if 0 <= int(row) < len(self._jobs):
            return self._jobs[int(row)].get("job_id")
        return None
