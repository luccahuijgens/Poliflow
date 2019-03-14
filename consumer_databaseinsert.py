import json
from cassandra.policies import DCAwareRoundRobinPolicy
from time import sleep
from cassandra.cqlengine import connection
import requests

from kafka import KafkaConsumer

if __name__ == '__main__':
    parsed_topic_name = 'parsedarticles'
    connection.setup(["localhost"],
    default_keyspace='poliflow',
    protocol_version=3,
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='DC1'),
    retry_connect=True)
    global _cql
    _cql = connection.get_session()

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    result=connection.query('select * from poliflw').count();

    for msg in consumer:
        result+=1;
        record = json.loads(msg.value)
        # importing the requests library
    #defining the api-endpoint
    API_ENDPOINT = "localhost:9200/poliflw/article/"+result

    # data to be sent to api
    data = record.dumps()

    # sending post request and saving response as response object
    r = requests.post(url = API_ENDPOINT, data = data)

    # extracting response text
    pastebin_url = r.text
    print("The pastebin URL is:%s"%pastebin_url)

    if consumer is not None:
        consumer.close()
