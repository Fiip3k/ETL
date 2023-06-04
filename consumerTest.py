#!source venv/bin/activate
from configparser import ConfigParser
from confluent_kafka import Consumer

def main():

    config = ConfigParser()
    config_file = open("kafkaConfig.ini", "r")
    config.read_file(config_file)
    config = dict(config["Kafka"])

    consumer = Consumer(config)
    
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