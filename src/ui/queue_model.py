from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor


class QueueTableModel(QAbstractTableModel):
    HEADERS = ["No.", "Prompt", "Type", "Model", "Account", "Status", "Progress"]

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
            if col == 0:
                return str(job.get("queue_no") or index.row() + 1)
            if col == 1:
                prompt = str(job.get("prompt") or "")
                return (prompt[:80] + "...") if len(prompt) > 80 else prompt
            if col == 2:
                return str(job.get("job_type_display") or "")
            if col == 3:
                return str(job.get("model_display") or "")
            if col == 4:
                return str(job.get("account") or "")
            if col == 5:
                return status.upper()
            if col == 6:
                return str(job.get("progress") or "")

        if role == Qt.ToolTipRole:
            if col == 1:
                return str(job.get("prompt") or "")
            if col == 3:
                return str(job.get("model_display") or "")
            if col == 4:
                return str(job.get("account") or "")
            if col == 5:
                return status.upper()
            if col == 6:
                return str(job.get("progress") or "")

        if role == Qt.TextAlignmentRole and col in (0, 2, 5, 6):
            return int(Qt.AlignCenter)

        if role == Qt.ForegroundRole:
            if col == 5:
                return {
                    "pending": QColor("#94A3B8"),
                    "running": QColor("#3B82F6"),
                    "completed": QColor("#22C55E"),
                    "failed": QColor("#EF4444"),
                    "moderated": QColor("#F59E0B"),
                }.get(status, QColor("#F8FAFC"))
            return QColor("#F8FAFC")

        if role == Qt.BackgroundRole:
            if col == 5:
                return {
                    "pending": QColor("#1D2535"),
                    "running": QColor("#1A2744"),
                    "completed": QColor("#132432"),
                    "failed": QColor("#1F1A2A"),
                    "moderated": QColor("#1C1E2A"),
                }.get(status, QColor("#1D2535"))
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
