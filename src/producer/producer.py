from kafka import KafkaProducer
import json
import requests
import sseclient

def get_wikimedia_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_wikimedia(producer):
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    headers = {'Accept': 'text/event-stream'}
    
    client = sseclient.SSEClient(url, headers=headers)
    for event in client.events():
        if event.data:
            try:
                change_data = json.loads(event.data)
                producer.send('wikimedia.recentchange', change_data)
            except json.JSONDecodeError:
                continue

if __name__ == '__main__':
    producer = get_wikimedia_producer()
    stream_wikimedia(producer)