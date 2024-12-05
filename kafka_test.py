from confluent_kafka import Producer

def acked(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer({'bootstrap.servers': 'localhost:9092'})

producer.produce('my-topic', key='key', value='Hello, Kafka!', callback=acked)
producer.flush()
print("Message sent successfully!")
