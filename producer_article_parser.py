from time import sleep
import json
from bson import json_util
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from pymongo import MongoClient
import datetime


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

def parse_article(article):
    date=""
    try:
        date=article[0:article.index("~")-1]
    except:
        date="-"
    title=""
    try:
        title=article[article.index("~")+2:article.index("-")-1]
    except:
        title=datetime.datetime.now().strftime("%Y-%m-%d")+" article "+str(datetime.datetime.now().microsecond)
    body=""
    try:
        body=article[article.index("-")+2:]
    except:
        body="-"
    jsonobject={}
    if (date!="-"):
        jsonobject['date']=date
    if (title!="-"):
        jsonobject['title']=title
    if (body!="-"):
        jsonobject['body']=body
    return json.dumps(jsonobject)

if __name__ == '__main__':
    all_raw=[]
    raw="""Utrecht 10 Feb 2018 ~ Death Race for Love - Het feest van de Willem Barentsz Zeevaartschool( Van een bijzondere correspondent in het Noorden ) .
Een fout in de overbrenging van ons telefonisch bericht over het jubileum der Willem Barentsz-zeevaartschool op Terschelling in ons blad van Zaterdag zoude stellig goede maritieme betrekking en tussen Terschelling en Den Helder kunnen verstoren , nademaal onze courant heeft vermeld , dat kapitein ter zee Dobbenga ,
commandant van het Marine opleidingskamp te Hilversum , de tafelpresident van het feestdiner , Terschelling het Marine Mekka had genoemd ; ter geruststelling zij gezegd , dat de Terschellinger feestredenaar zijn eiland een maritiem Mekka heeft genoemd en geprezen , hetgeen natuurlijk iets anders is. Wij moeten er voorts nog melding van maken ,
dat het provinciale bestuur van Friesland het gemeentebestuur van Terschelling een ingelijste foto van het schilderij heeft aangeboden , dat door de Friese kunstschilder Cor Reisma onlangs in op dracht van de provincie is vervaardigd.
Met sportfeesten en enige vertoningen van een revue door de leerlingen der zeevaartschool heeft de Terschellinger burgerij de feestviering over vier dagen voortgezet.
"""
    raw2="""Amsterdam 15 May 2017 ~ Syphilis - Regering verhoogt subsidie voor bestrijding alcoholisme
De nationale commissie tegen het alcoholisme heeft in een te Utrecht gehouden vergadering een commissie drankmisbruik door vissers « ingesteld , zulks naar aanleiding van ingekomen klachten als zou het drankmisbruik onder vissers toenemen.
De voorzitter deelde mede , dat de minister van Sociale Zaken de subsidie van f 32.000 tot f. 48. 000 zal verhogen onder voorwaarde , dat de nationale commissie geen organisaties zal weren , die niet op het standpunt staan der geheelonthouding Na enige discussies werd deze voorwaarde aanvaard.
"""
    raw3="""Rotterdam 12 Jun 2015 ~ Robbery - Maansverduistering op vele plaatsen te zien geweest

Van onze weerkundige medewerker )

PRECIES zoals de heren astronomen hadden voorspeld , is de maan gisteravond enige tijd in de schaduw-kegel van de aarde geweest. Vrijwel overal zal men dit interessante verschijnsel geheel of gedeeltelijk hebben kunnen zien ; het feit , dat wij Zondagmorgen juist in kersverse polaire lucht waren gekomen , was wel bijzonder gunstig voor de waarneming , omdat het zicht in deze lucht altijd buitengewoon goed is. Een minder gunstige noot vormden de vele wolken , die voor een belangrijk deel samenhingen met een trogvormige storing , die gedurende de tweede helft van de Zondagmiddag het Westen van het land was gepasseerd en 's avonds het weer in de Oostelijke provincies nog ongunstig influenceerde. Op vliegveld Twente heelt men tot half elf toen de totale verduistering reeds voorbij was , niets van de maan kunnen zien. In de Bilt was het veel beter ,
daar heeft men zowel het kleiner worden van de maan als de totale verduistering en ook het aangroeien van de maansikkel tot de normale volle maan duidelijk kunnen waarnemen ,
al waren er onderbreking en wegens overdrijven de wolkenvelden.
In het Westen van het land zal het nog wel iets beter zijn geweest. Ditmaal is alles dus nogal meegevallen.
Verleden jaar was dat op 13 April - eveneens in de week voor Pasen - anders. Toen bleef de maan achter de wolken , maar op 7 October was de verduistering beter te observeren. Nu zullen wij weer tot de nacht van 25 op 26 September moeten wachten voordat zich opnieuw een maansverduistering zal voordoen.
"""
    raw4="Without Me - bodytext"
    all_raw.append(raw)
    all_raw.append(raw2)
    all_raw.append(raw3)
    all_raw.append(raw4)
    all_article=[]
    for rawarticle in all_raw:
        all_article.append(parse_article(rawarticle))
    if len(all_article) > 0:
        kafka_producer = connect_kafka_producer()
        for article in all_article:
            publish_message(kafka_producer, 'rawarticletest', 'raw', article)
        if kafka_producer is not None:
            kafka_producer.close()
