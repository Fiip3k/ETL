import threading
import json
from confluent_kafka import Producer, Consumer, KafkaException
from config import ProducerConfig, ConsumerConfig
from threading import Thread
from typing import Callable

class Messenger:

    producer = None
    consumer = None

    def __init__(self):
        self.producer = Messenger.loadProducer()
        self.consumer = Messenger.loadConsumer()
        

    def sendMessage(self, topic: str, message: dict) -> None:
        jsonData = json.dumps(message)
        self.producer.produce(topic, value=jsonData.encode('utf-8'))
        self.producer.flush()

    @staticmethod
    def loadProducer() -> "Producer":
        config = ProducerConfig
        producer = Producer(config)
        return producer
    
    @staticmethod
    def loadConsumer() -> "Consumer":
        config = ConsumerConfig
        consumer = Consumer(config)
        return consumer

    def startListening(self, topic: str, handler: Callable[[str], None] = None, daemon: bool = None) -> Thread:
        exitEvent = threading.Event()
        def runConsumer():
            try:
                self.consumer.subscribe([topic])
                while not exitEvent.is_set():
                    msg = self.consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        print("Consumer error: {}".format(msg.error()))
                        continue
                    if handler != None:
                        handler(json.loads(msg.value().decode("utf-8")))
                    else:
                        print("Received message: {}".format(json.loads(msg.value().decode("utf-8"))))
            except KafkaException as e:
                print("KafkaException: {}".format(e))
                return
            self.consumer.close()

        thread = Thread(target=runConsumer, daemon=None)
        thread.start()
        return thread, exitEvent

    