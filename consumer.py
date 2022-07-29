from kafka.consumer import KafkaConsumer
from elasticsearch import Elasticsearch
import json
from datetime import datetime

consumer = KafkaConsumer('tweets',\
                         bootstrap_servers=['localhost:9092'],\
                         auto_offset_reset='earliest')

es = Elasticsearch('http://localhost:9200')

id = 1
for message in consumer:
    message = message.value.decode("utf-8")
    if message not in ['[]', '', ' ']:
        message = json.loads(message)
        if len(message) < 4:
            doc = {}
            for sentiment in message:
                doc[sentiment[0]] = sentiment[1]
                doc['window'] = id
            print(es.index(index='tweets', id=id, document=doc))
            id += 1
    
consumer.close()