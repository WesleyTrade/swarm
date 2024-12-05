import redis
import json
import pandas as pd
import numpy as np
import logging
import uuid
import time
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
from concurrent.futures import ThreadPoolExecutor
from subprocess import Popen
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision

import os

# Load environment variables
load_dotenv()

# Redis configuration
redis_client = redis.StrictRedis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    decode_responses=True
)

# InfluxDB configuration
influxdb_client = InfluxDBClient(
    url=os.getenv('INFLUXDB_URL'),
    token=os.getenv('INFLUXDB_TOKEN'),
    org=os.getenv('INFLUXDB_ORG')
)
bucket = os.getenv('INFLUXDB_BUCKET')

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global parameters
PEER_THRESHOLD = int(os.getenv('PEER_THRESHOLD', 5))
INACTIVITY_TIMEOUT = int(os.getenv('INACTIVITY_TIMEOUT', 3600))
RETRAIN_THRESHOLD = float(os.getenv('RETRAIN_THRESHOLD', 0.6))
SWARM_CONFIDENCE_THRESHOLD = float(os.getenv('SWARM_CONFIDENCE_THRESHOLD', 70.0))


# Utility Functions
def generate_mac_address():
    """Generate a unique MAC-like address."""
    return ':'.join(['{:02x}'.format(x) for x in uuid.uuid4().bytes[:6]])


def register_peer(name, mac_address):
    """Register the peer in Redis."""
    try:
        peer_data = {
            "name": name,
            "mac_address": mac_address,
            "status": "active",
            "timestamp": pd.Timestamp.now().isoformat()
        }
        redis_client.set(f"tina:peer:{mac_address}", json.dumps(peer_data))
        logging.info(f"Registered peer {name} with MAC {mac_address}.")
    except Exception as e:
        logging.error(f"Error registering peer {name}: {e}")


def create_peer():
    """Create a new Tina peer."""
    try:
        mac_address = generate_mac_address()
        name = f"Tina-Peer-{mac_address[-4:]}"
        peer_script = os.path.abspath(__file__)  # Get the full path of the current script

        register_peer(name, mac_address)

        # Spawn a new peer instance
        Popen(["python", peer_script], shell=False)
        logging.info(f"Created new peer: {name}")
    except Exception as e:
        logging.error(f"Error creating peer: {e}")


def manage_peers():
    """Dynamically manage the swarm of peers."""
    try:
        active_peers = redis_client.keys("tina:peer:*")
        logging.info(f"Active peers: {[key.split(':')[-1] for key in active_peers]}")

        # If workload increases, create more peers
        if len(active_peers) < PEER_THRESHOLD:
            logging.info("Workload high. Creating additional peers.")
            create_peer()

        # Prevent premature removal of newly created peers
        time.sleep(5)  # Allow new peers to update their status in Redis

        # Remove inactive peers
        for peer_key in active_peers:
            peer_data = json.loads(redis_client.get(peer_key))
            if (pd.Timestamp.now() - pd.Timestamp(peer_data["timestamp"])).seconds > INACTIVITY_TIMEOUT:
                redis_client.delete(peer_key)
                logging.info(f"Removed inactive peer: {peer_data['name']}.")
    except Exception as e:
        logging.error(f"Error managing peers: {e}")


def fetch_available_currency_pairs():
    """Fetch available currency pairs from Redis."""
    try:
        pair_keys = redis_client.keys("tina:data:*")
        if not pair_keys:
            logging.warning("No currency pairs found in Redis.")
            return []

        pairs = [key.split(":")[-1] for key in pair_keys]
        logging.info(f"Fetched available currency pairs: {pairs}")
        return pairs
    except Exception as e:
        logging.error(f"Error fetching currency pairs from Redis: {e}")
        return []


def fetch_data_from_redis(pair):
    """Fetch historical data for a currency pair from Redis."""
    try:
        key = f"tina:data:{pair}"
        data = redis_client.get(key)
        if not data:
            logging.warning(f"No data found for {pair} in Redis.")
            return pd.DataFrame()

        data = pd.DataFrame(json.loads(data))
        logging.info(f"Fetched data for {pair} from Redis. Data size: {len(data)} rows.")
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {pair} from Redis: {e}")
        return pd.DataFrame()


def write_to_influxdb(data, measurement, tags):
    """Write trading data to InfluxDB."""
    try:
        write_api = influxdb_client.write_api(write_options=WritePrecision.NS)
        point = Point(measurement)
        for key, value in tags.items():
            point = point.tag(key, value)
        for key, value in data.items():
            point = point.field(key, value)
        point = point.time(pd.Timestamp.now(), WritePrecision.NS)
        write_api.write(bucket=bucket, record=point)
        logging.info(f"Data written to InfluxDB for measurement: {measurement}")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB: {e}")


def calculate_indicators(data):
    """Calculate indicators for trading analysis."""
    try:
        data['SMA'] = data['Close'].rolling(window=5).mean()
        data['EMA'] = data['Close'].ewm(span=10, adjust=False).mean()
        data.dropna(inplace=True)
        return data
    except Exception as e:
        logging.error(f"Error calculating indicators: {e}")
        return pd.DataFrame()


def prepare_data_for_ml(data):
    """Prepare data for machine learning."""
    try:
        features = ['SMA', 'EMA']
        target = 'Target'  # Ensure 'Target' is precomputed and part of the dataset

        if all(col in data.columns for col in features + [target]):
            X = data[features]
            y = data[target]
            return X, y
        else:
            logging.error("Required columns for ML preparation are missing.")
            return None, None
    except Exception as e:
        logging.error(f"Error preparing data for ML: {e}")
        return None, None


def train_ml_model(X, y):
    """Train an ML model."""
    try:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        model = RandomForestClassifier()
        model.fit(X_train_scaled, y_train)

        accuracy = accuracy_score(y_test, model.predict(X_test_scaled))
        logging.info(f"Model trained with accuracy: {accuracy:.2f}")
        return model, scaler, accuracy
    except Exception as e:
        logging.error(f"Error training ML model: {e}")
        return None, None, 0.0


def process_currency_pair(pair):
    """Process individual currency pair for prediction and trading."""
    data = fetch_data_from_redis(pair)
    if data.empty:
        return

    data = calculate_indicators(data)
    if data.empty:
        return

    X, y = prepare_data_for_ml(data)
    if X is None or y is None:
        return

    model, scaler, accuracy = train_ml_model(X, y)
    if model is None:
        return

    # Simulate local prediction and decision-making
    latest_data = X.iloc[-1:]
    scaled_data = scaler.transform(latest_data)
    local_prediction = model.predict(scaled_data)[0]
    local_confidence = max(model.predict_proba(scaled_data)[0]) * 100

    write_to_influxdb(
        {"confidence": local_confidence, "prediction": local_prediction},
        "predictions",
        {"pair": pair}
    )

    logging.info(f"Prediction for {pair}: {local_prediction}, Confidence: {local_confidence:.2f}%.")


def main():
    """Master Tina managing the swarm and trading decisions."""
    available_pairs = fetch_available_currency_pairs()
    if not available_pairs:
        logging.warning("No available currency pairs to process. Exiting.")
        return

    manage_peers()

    with ThreadPoolExecutor(max_workers=len(available_pairs)) as executor:
        executor.map(process_currency_pair, available_pairs)


if __name__ == "__main__":
    main()
