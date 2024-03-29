import time
from newDataHandler import NewDataHandler


def main():
    ndh = NewDataHandler()

    topic = "wgcpspem-upload"

    thread, exitEvent = ndh.msgr.startListening(
        topic, handler=ndh.startMessageProcessor
    )
    print("Listener started...")
    ndh.periodicCleanUp(10)

    time.sleep(3)

    topic = "wgcpspem-test"

    filename = "test.csv"
    ndh.sendUploadMessage(topic, filename, "newdata/test", overwrite=True)

    time.sleep(3)

    filename = "tran_hv_pstra.tsv"
    ndh.sendUploadMessage(topic, filename, "newdata/test", overwrite=False)

    time.sleep(3)

    filename = "urb_percep.tsv"
    ndh.sendUploadMessage(topic, filename, "newdata/test", overwrite=False)

    time.sleep(3)

    ndh.sendDownloadMessage(topic, "newdata/test", filename, "downloadedTest.csv")
    print("Download message sent...")

    # Input() for standard cases
    # input()

    # Sleep instead of input() for testing purposes
    time.sleep(3)

    exitEvent.set()
    thread.join()
    ndh.join()
    ndh.stopPeriodicCleanUp()
    print("Listener finished.")


if __name__ == "__main__":
    main()
