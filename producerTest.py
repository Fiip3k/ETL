#!source venv/bin/activate
from configparser import ConfigParser
from confluent_kafka import Producer
import threading

def task(producer, topic, message, timer, i = 1):
    producer.produce(topic, value=message+" "+str(i))
    producer.flush()
    i+=1
    threading.Timer(timer, task, [producer, topic, message, timer, i]).start()

def main():
    config = ConfigParser()
    config_file = open("kafkaConfig.ini", "r")
    config.read_file(config_file)
    config = dict(config["Kafka"])

    producer = Producer(config)
    
    topic = "wgcpspem-test"
    message = "Test message"

    task(producer, topic, message, 5)

















if(__name__ == "__main__"):
    main()