#!source venv/bin/activate

import time
from messenger import Messenger
from lakeClient import LakeClient
from datetime import datetime
from threading import Thread


class NewDataHandler:

    lakeClient = None

    def __init__(self) -> None:
        self.lakeClient = LakeClient("fiip3kdatalake")

    def messageReceived(self, msg: dict):
        def processMessage():
            if(msg["message"]=="upload"):
                result = self.lakeClient.uploadFile(msg['filename'], msg["path"], overwrite=msg["overwrite"])
            elif(msg["message"]=="download"):
                result = self.lakeClient.downloadFile(msg['path'], msg['filename'], msg['saveas'])
            else:
                print(msg)
                time.sleep(10)

        thread = Thread(target=processMessage, daemon=False)
        thread.start()

def main():
    ndh = NewDataHandler()
    msgr = Messenger()
    thread, exitEvent = msgr.startListening("wgcpspem-test", handler=ndh.messageReceived, daemon = None)
    print("Listener started...")
    
    time.sleep(5)
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%H:%M:%S %d.%m.%Y")

    filename = "test.csv"

    msgr.sendMessage("wgcpspem-test", {"message":"upload", "path":"newdata/test", "filename":filename, "overwrite":True, "datetime":formatted_datetime})
    time.sleep(5)
    msgr.sendMessage("wgcpspem-test", {"message":"download", "path":"newdata/test", "filename":filename, "saveas":"downloadedTest.csv", "datetime":formatted_datetime})
    print("Messages sent...")

    input()
    exitEvent.set()
    thread.join()
    print("Listener finished.")

if __name__ == "__main__":
    main()