from messenger import Messenger
from lakeClient import LakeClient
from datetime import datetime
import time

class NewDataHandler:

    lakeClient = None

    def __init__(self) -> None:
        self.lakeClient = LakeClient("fiip3kdatalake")



    def messageReceived(self, msg: str):
        def processMessage():
            pass
        # TODO

        print(msg)

def main():
    ndh = NewDataHandler()
    lakeClient = LakeClient("fiip3kdatalake")
    #lakeClient.uploadFile("test.csv", "newdata/test")
    #file = lakeClient.downloadFile("newdata/test/", "test.csv", "downloadedTest.csv")
    msgr = Messenger()
    thread, exitEvent = msgr.startListening("wgcpspem-test", handler=ndh.messageReceived, daemon = None)
    print("Listener started...")
    
    time.sleep(5)
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%H:%M:%S %d.%m.%Y")
    msgr.sendMessage("wgcpspem-test", "MESSAGEU " + formatted_datetime)
    msgr.sendMessage("wgcpspem-test", "MESSAGEU " + formatted_datetime)
    msgr.sendMessage("wgcpspem-test", "MESSAGEU " + formatted_datetime)
    print("Messages sent...")

    while input() != "EXIT":
        pass
    exitEvent.set()
    thread.join()
    print("Listener finished.")

if __name__ == "__main__":
    main()