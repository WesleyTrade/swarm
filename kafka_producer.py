from confluent_kafka import Producer
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'tina-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_messages(topic, messages):
    for message in messages:
        producer.produce(topic, json.dumps(message).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
    producer.flush()

if __name__ == '__main__':
    test_messages = [
        {'event': 'test1', 'value': 'Hello, Kafka!'},
        {'event': 'test2', 'value': 'Another message'},
    ]
    produce_messages('test-topic', test_messages)
