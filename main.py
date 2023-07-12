#!source venv/bin/activate
#!call venv/bin/activate

import time
from newDataHandler import NewDataHandler


def main():
    ndh = NewDataHandler()
    thread, exitEvent = ndh.msgr.startListening(
        "wgcpspem-test", handler=ndh.startMessageProcessor
    )
    print("Listener started...")
    ndh.periodicCleanUp(10)

    time.sleep(3)

    topic = "wgcpspem-test"

    filename = "test.csv"
    ndh.sendUploadMessage(topic, filename, "newdata/test", overwrite=False)

    time.sleep(3)

    filename = "tran_hv_pstra.tsv"
    ndh.sendUploadMessage(topic, filename, "newdata/test", overwrite=False)

    time.sleep(3)

    filename = "urb_percep.tsv"
    ndh.sendUploadMessage(topic, filename, "newdata/test", overwrite=False)

    time.sleep(3)

    ndh.sendDownloadMessage(topic, "newdata/test", filename, "downloadedTest.csv")
    print("Messages sent...")

    # input()
    exitEvent.set()

    thread.join()
    ndh.join()
    ndh.stopPeriodicCleanUp()
    print("Listener finished.")


if __name__ == "__main__":
    main()
