import redis
import json
import time
from alpha_vantage.foreignexchange import ForeignExchange
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv()

# Alpha Vantage API Key
API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
if not API_KEY:
    raise ValueError("Alpha Vantage API Key not set in environment variables.")

# Configure Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
MIN_ROWS_REQUIRED = 50  # Minimum rows required for robust swarm usage
REDIS_TTL = 3600 * 6  # 6-hour TTL for stored data
REDIS_NAMESPACE = "tina:data"  # Redis namespace for Tina's swarm data


def fetch_supported_currency_pairs():
    """
    Fetch all supported currency pairs from Alpha Vantage.
    """
    fx = ForeignExchange(key=API_KEY, output_format='json')
    try:
        # Retrieve all currency symbols (example logic, adjust if needed)
        supported_pairs = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD',
                           'NZDUSD', 'EURJPY', 'GBPJPY', 'AUDJPY', 'CADJPY']  # Replace with a dynamic fetch if available
        logging.info(f"Fetched supported currency pairs: {supported_pairs}")
        return supported_pairs
    except Exception as e:
        logging.error(f"Error fetching supported currency pairs: {e}")
        return []


def fetch_historical_forex_data(pair, max_retries=3):
    """
    Fetch historical forex data for a currency pair.
    """
    fx = ForeignExchange(key=API_KEY, output_format='json')
    retries = 0
    while retries < max_retries:
        try:
            # Fetch full historical data
            data, _ = fx.get_currency_exchange_daily(
                from_symbol=pair[:3],
                to_symbol=pair[3:],
                outputsize='full'  # Fetch as much data as possible
            )
            if not data:
                raise ValueError(f"No historical data returned for {pair}")
            return data
        except Exception as e:
            retries += 1
            logging.warning(f"Error fetching historical data for {pair} (Attempt {retries}/{max_retries}): {e}")
            time.sleep(1)
    logging.error(f"Failed to fetch historical data for {pair} after {max_retries} retries.")
    return None


def preprocess_historical_data(pair, data):
    """
    Preprocess and validate historical data.
    """
    try:
        # Extract relevant fields and convert them into a standardized format
        processed_data = [
            {
                "Date": date,
                "Open": float(details["1. open"]),
                "High": float(details["2. high"]),
                "Low": float(details["3. low"]),
                "Close": float(details["4. close"])
            }
            for date, details in data.items()
        ]
        # Check if the data has enough rows
        if len(processed_data) < MIN_ROWS_REQUIRED:
            logging.warning(f"Insufficient data for {pair} ({len(processed_data)} rows). Minimum required: {MIN_ROWS_REQUIRED}.")
            return None
        return processed_data
    except KeyError as e:
        logging.error(f"Missing required field in data for {pair}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error preprocessing data for {pair}: {e}")
    return None


def store_historical_data_in_redis(pair, processed_data):
    """
    Store historical data in Redis for Tina's Swarm intelligence.
    """
    try:
        # Use namespaced key for storing data
        redis_key = f"{REDIS_NAMESPACE}:{pair}"
        redis_client.set(redis_key, json.dumps(processed_data), ex=REDIS_TTL)
        logging.info(f"Stored {pair} data in Redis successfully with {len(processed_data)} rows.")
    except Exception as e:
        logging.error(f"Unexpected error storing {pair} in Redis: {e}")


def main():
    """
    Continuously fetch and store historical data for Swarm intelligence Tina.
    """
    while True:
        # Dynamically fetch supported currency pairs
        currency_pairs = fetch_supported_currency_pairs()
        if not currency_pairs:
            logging.error("No currency pairs available. Retrying in 60 seconds.")
            time.sleep(60)
            continue

        # Fetch, process, and store historical data for each pair
        for pair in currency_pairs:
            raw_data = fetch_historical_forex_data(pair)
            if raw_data:
                processed_data = preprocess_historical_data(pair, raw_data)
                if processed_data:
                    store_historical_data_in_redis(pair, processed_data)
        logging.info("All currency pairs updated. Sleeping for 60 seconds...")
        time.sleep(60)


if __name__ == "__main__":
    main()
