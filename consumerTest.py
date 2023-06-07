#!source venv/bin/activate
from config import ConsumerConfig
from configparser import ConfigParser
from confluent_kafka import Consumer

def loadConfig(filename: str = None):
    if not filename:
        config = ConsumerConfig
    else:
        config = ConfigParser()
        config_file = open(filename, "r")
        config.read_file(config_file)
        config = dict(config["Kafka"])
    return config

def loadConsumer(filename: str = None):
    config = loadConfig(filename)
    consumer = Consumer(config)
    return consumer


def main():

    consumer = loadConsumer()
    topic = "wgcpspem-test"


    consumer.subscribe([topic])

    print("Consumer starting...")
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received message:\n {msg.value().decode('utf-8')}")

















if(__name__ == "__main__"):
    main()