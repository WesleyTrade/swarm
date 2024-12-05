import redis
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Redis Configuration
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

def set_key(key, value):
    try:
        redis_client.set(key, json.dumps(value))
        logging.info(f'Set key {key} with value {value}')
    except Exception as e:
        logging.error(f'Error setting key {key}: {e}')

def get_key(key):
    try:
        value = redis_client.get(key)
        if value:
            logging.info(f'Got key {key} with value {json.loads(value)}')
        else:
            logging.info(f'Key {key} not found.')
    except Exception as e:
        logging.error(f'Error getting key {key}: {e}')

if __name__ == '__main__':
    set_key('test_key', {'message': 'Hello, Redis!'})
    get_key('test_key')
