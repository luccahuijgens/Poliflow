import json
from time import sleep
from pymongo import MongoClient
import re
from textblob import TextBlob as tb
import math
from gensim.models import Word2Vec
import datetime

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer


def tf(word, blob):
    return blob.words.count(word) / len(blob.words)

def n_containing(word, bloblist):
    return sum(1 for blob in bloblist if word in blob.words)

def idf(word, bloblist):
    return math.log(len(bloblist) / (1 + n_containing(word, bloblist)))

def tfidf(word, blob, bloblist):
    return tf(word, blob) * idf(word, bloblist)

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
    print('Running Consumer..')
    datetime.datetime.now()
    corpus=[]
    wordcorpus=[]
    allarticletext=[]
    parsed_articles=[]
    topic_name = 'rawarticles'
    parsed_topic_name = 'parsedarticles'
    news_subjects=['zorg','onderwijs','verkiezingen','economie','koningshuis','regio','binnenland','buitenland','sport','cultuur','media','politiek','technologie']

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        message = json.loads(msg.value)
        messagetext = re.sub('[^a-zA-Z]', ' ', message['text'] )
        messagetext = re.sub(r'\s+', ' ', messagetext)
        bodyblob=tb(messagetext).lower()
        message['blobtext']=bodyblob
        corpus.append(message)
        allarticletext.append(bodyblob)

    for message in corpus:
        blob=message['blobtext']
        for sent in blob.sentences:
            wordcorpus.append(sent.words)

    model=Word2Vec(wordcorpus, min_count=2)

    for message in corpus:
        blob=message['blobtext']
        scores={}
        for word in blob.words:
            scores[word]=tfidf(word, blob, allarticletext)
        sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        tagarray=[]
        articlesubjects=[]
        for word, score in sorted_words[:3]:
            #print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))
            subjectdictionary={}
            for subject in news_subjects:
                try:
                    value=0
                    value=model.wv.similarity(word,subject)
                    subjectdictionary[subject]=value
                except:
                    print("Similarity ("+word+" + "+subject+": not enough data for subject determination")

            sorteddictionary = [(k, subjectdictionary[k]) for k in sorted(subjectdictionary, key=subjectdictionary.get, reverse=True)]
            counter=0
            for key,value in sorteddictionary:
                print(key)
                if counter<3:
                    articlesubjects.append(key)
                    counter+=1

        distinct_values = set(articlesubjects)
        for distinct_value in distinct_values:
            if articlesubjects.count(distinct_value)>=2:
                tagarray.append(str(distinct_value))
        message['tags']=tagarray
        parsed_articles.append(message)
    consumer.close()
    sleep(5)

    if len(parsed_articles) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_articles:
            del rec['blobtext']
            publish_message(producer, parsed_topic_name, 'parsed', json.dumps(rec))
