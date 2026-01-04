from PyQt6.QtCore import QThread, pyqtSignal

from services.market import update_market_data


class DownloadThread(QThread):
    finished = pyqtSignal(bool, str)

    def __init__(self, data_dir: str):
        super().__init__()
        self.data_dir = data_dir

    def run(self):
        try:
            success, message = update_market_data(self.data_dir)
            self.finished.emit(success, message)
        except Exception as e:
            self.finished.emit(False, f"Error: {str(e)}")
