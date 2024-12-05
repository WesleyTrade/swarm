from kafka import KafkaConsumer
import redis
import json

# Kafka Configuration
KAFKA_TOPIC = 'test-topic'
KAFKA_SERVER = 'localhost:9092'

# Redis Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6380
REDIS_PREFIX = 'kafka:message:'

def main():
    # Connect to Redis
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Connect to Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='redis-integration-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    
    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    
    # Process Messages
    for idx, message in enumerate(consumer):
        print(f"Received: {message.value}")
        
        # Store message in Redis
        redis_key = f"{REDIS_PREFIX}{idx}"
        redis_client.set(redis_key, message.value)
        print(f"Stored in Redis: {redis_key} -> {message.value}")

if __name__ == '__main__':
    main()
