#!source venv/bin/activate
from config import ProducerConfig
from configparser import ConfigParser
from confluent_kafka import Producer
import threading

def task(producer: Producer, topic, message, timer, i = 1):
    producer.produce(topic, value=message+" "+str(i))
    producer.flush()
    i+=1
    threading.Timer(timer, task, [producer, topic, message, timer, i]).start()

def loadConfigFromIni(filename: str = "kafkaConfig.ini"):
    config = ConfigParser()
    config_file = open(filename, "r")
    config.read_file(config_file)
    config = dict(config["Kafka"])
    return config

def loadProducer(filename: str = None):
    if not filename:
        config = ProducerConfig
    else:
        config = loadConfigFromIni(filename)
    producer = Producer(config)
    return producer

def sendMessage(topic, message, filename: str = None, producer: Producer = None):
    if not producer:
        producer = loadProducer(filename)
    producer.produce(topic, value=message)
    producer.flush()

def main():

    topic = "wgcpspem-test"
    message = "Test message"

    sendMessage(topic, message)
    #task(producer, topic, message, 5)

















if(__name__ == "__main__"):
    main()