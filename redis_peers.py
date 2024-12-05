import redis
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Redis Configuration
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

def list_peers():
    try:
        peer_keys = redis_client.keys('tina:peer:*')
        peers = [json.loads(redis_client.get(key)) for key in peer_keys]
        logging.info(f'Active Peers: {peers}')
    except Exception as e:
        logging.error(f'Error listing peers: {e}')

if __name__ == '__main__':
    list_peers()
