from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

def get_wikimedia_consumer():
    return KafkaConsumer(
        'wikimedia.recentchange',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

def get_elasticsearch_client():
    return Elasticsearch(['http://localhost:9200'])

def process_messages(consumer, es_client):
    for message in consumer:
        doc = message.value
        try:
            es_client.index(
                index='wikimedia',
                document=doc
            )
        except Exception as e:
            print(f"Error indexing document: {e}")

if __name__ == '__main__':
    consumer = get_wikimedia_consumer()
    es_client = get_elasticsearch_client()
    process_messages(consumer, es_client)