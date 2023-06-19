import time
from messenger import Messenger
from lakeClient import LakeClient
from datetime import datetime
from threading import Thread


class NewDataHandler:
    lakeClient = None
    msgr = None
    threads = []
    cleanup = False
    period = 10

    def __init__(self) -> None:
        self.lakeClient = LakeClient("fiip3kdatalake")
        self.msgr = Messenger()

    def periodicCleanUp(self, period: float = 10):
        def cleanThreads():
            toRemove = []
            for thread in self.threads:
                if not thread.is_alive():
                    toRemove.append(thread)
            for thread in toRemove:
                self.threads.remove(thread)

            if self.cleanup:
                time.sleep(self.period)
                cleanThreads()

        self.period = period
        self.cleanup = True
        Thread(target=cleanThreads).start()

    def stopPeriodicCleanUp(self):
        self.cleanup = False

    def startMessageProcessor(self, msg: dict):
        def processMessage():
            if msg["message"] == "upload":
                result = self.lakeClient.uploadFile(
                    msg["filename"], msg["path"], overwrite=msg["overwrite"]
                )
            elif msg["message"] == "download":
                result = self.lakeClient.downloadFile(
                    msg["path"], msg["filename"], msg["saveas"]
                )
            else:
                print(msg)

        thread = Thread(target=processMessage, daemon=False)
        thread.start()
        self.threads.append(thread)

    def sendUploadMessage(
        self, topic: str, filename: str, path: str = "", overwrite: bool = True
    ):
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%H:%M:%S %d.%m.%Y")
        self.msgr.sendMessage(
            topic,
            {
                "message": "upload",
                "path": path,
                "filename": filename,
                "overwrite": overwrite,
                "datetime": formatted_datetime,
            },
        )

    def sendDownloadMessage(
        self, topic: str, path: str, filename: str, saveas: str = None
    ):
        if not saveas:
            saveas = filename
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%H:%M:%S %d.%m.%Y")
        self.msgr.sendMessage(
            topic,
            {
                "message": "download",
                "path": path,
                "filename": filename,
                "saveas": saveas,
                "datetime": formatted_datetime,
            },
        )

    def join(self):
        for thread in self.threads:
            thread.join()
