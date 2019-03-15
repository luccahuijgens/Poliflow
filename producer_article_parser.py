from time import sleep
import json
from bson import json_util
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import datetime
from difflib import SequenceMatcher


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__ == '__main__':
    newarticles=[]
    with open('scraped_data_news_output_raw.json') as json_file:
        data = json.load(json_file)
        for p in data['newspapers']:
            for key in p:
                value=p[key]
            for article in value['articles']:
                newarticles.append(article)

    parsed_topic_name = 'rawarticles'
    # Notify if a recipe has more than 200 calories
    historicarticles=[]
    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        record = json.loads(msg.value)
        body = record['body']
        historicarticles.append(body)

    if len(newarticles) > 0:
        kafka_producer = connect_kafka_producer()
        for article in newarticles:
            shall_publish=True
            for historicarticle in historicarticles:
                if SequenceMatcher().ratio(None,article['text'],historicarticle)>.95:
                    shall_publish=False
            if shall_publish:
                publish_message(kafka_producer, 'rawarticles', 'raw', json.dumps(article))
    if kafka_producer is not None:
        kafka_producer.close()
