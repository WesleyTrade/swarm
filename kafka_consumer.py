from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'tina-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

def consume_messages(topic):
    try:
        consumer.subscribe([topic])
        logging.info(f'Subscribed to topic: {topic}')
        
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f'Reached end of partition: {msg.partition()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                message = json.loads(msg.value().decode('utf-8'))
                logging.info(f'Received message: {message}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_messages('test-topic')
