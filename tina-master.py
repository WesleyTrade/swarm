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
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os
from kafka import KafkaProducer
from threading import Lock
from joblib import dump, load
from datetime import datetime, timedelta, timezone
import random  # For exploration/exploitation balance

# Load environment variables
load_dotenv()

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# InfluxDB Configuration
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

if not all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET]):
    logging.error("InfluxDB configuration incomplete. Please set INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, and INFLUXDB_BUCKET.")
    exit(1)

influxdb_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
bucket = INFLUXDB_BUCKET

# Kafka Configuration for Event-Driven Data Processing
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("tina_trading.log"),
    ]
)

# Global Parameters
PEER_THRESHOLD = int(os.getenv('PEER_THRESHOLD', 5))
INACTIVITY_TIMEOUT = int(os.getenv('INACTIVITY_TIMEOUT', 3600))
RETRAIN_THRESHOLD = float(os.getenv('RETRAIN_THRESHOLD', 0.6))
SWARM_CONFIDENCE_THRESHOLD = float(os.getenv('SWARM_CONFIDENCE_THRESHOLD', 70.0))
EXPLORATION_RATE = float(os.getenv('EXPLORATION_RATE', 0.1))  # For exploration/exploitation balance

# Lock for Synchronization in Multi-Threaded Context
lock = Lock()

# Peer Scoring and Trustworthiness Parameters
peer_scores = {}  # Dictionary to hold peer scores

# Initialize Alpaca Client
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit

ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
ALPACA_BASE_URL = os.getenv('ALPACA_BASE_URL')

if not all([ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL]):
    logging.error("Alpaca API credentials incomplete. Please set ALPACA_API_KEY, ALPACA_SECRET_KEY, and ALPACA_BASE_URL.")
    exit(1)

alpaca_client = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version='v2')

# Utility Functions

def generate_mac_address():
    """
    Generates a random MAC address.
    """
    return ':'.join(['{:02x}'.format(x) for x in uuid.uuid4().bytes[:6]])

def register_peer(name, mac_address):
    """
    Registers a peer in Redis.
    
    Parameters:
        name (str): The name of the peer.
        mac_address (str): The MAC address of the peer.
    """
    try:
        peer_data = {
            "name": name,
            "mac_address": mac_address,
            "status": "active",
            "timestamp": pd.Timestamp.now().isoformat(),
            "score": 100  # Initialize score
        }
        redis_client.set(f"tina:peer:{mac_address}", json.dumps(peer_data))
        logging.info(f"Registered peer {name} with MAC {mac_address}.")
        with lock:
            peer_scores[f"tina:peer:{mac_address}"] = peer_data["score"]
    except Exception as e:
        logging.error(f"Error registering peer {name}: {e}")

def create_peer():
    """
    Creates and registers a new peer in Redis.
    """
    try:
        peer_name = f"peer_{uuid.uuid4().hex[:6]}"
        mac_address = generate_mac_address()
        register_peer(peer_name, mac_address)
        logging.info(f"Created and registered new peer: {peer_name}, MAC: {mac_address}.")
    except Exception as e:
        logging.error(f"Error creating a new peer: {e}")

def manage_peers():
    """
    Manages active peers by checking their activity and updating their scores.
    Removes inactive peers.
    """
    global peer_scores
    try:
        active_peers = redis_client.keys("tina:peer:*")
        logging.info(f"Active peers: {[key.split(':')[-1] for key in active_peers]}")

        if len(active_peers) < PEER_THRESHOLD:
            create_peer()

        # Check peer activity and clean up inactive peers
        for peer_key in active_peers:
            peer_data = json.loads(redis_client.get(peer_key))
            last_active = pd.Timestamp(peer_data["timestamp"])
            time_active = (pd.Timestamp.now() - last_active).seconds

            if time_active > INACTIVITY_TIMEOUT:
                redis_client.delete(peer_key)
                logging.info(f"Removed inactive peer: {peer_data['name']}.")
                with lock:
                    peer_scores.pop(peer_key, None)
            else:
                # Update peer score based on the time they are active
                with lock:
                    peer_scores[peer_key] = calculate_peer_score(peer_data)
    except Exception as e:
        logging.error(f"Error managing peers: {e}")

def calculate_peer_score(peer_data):
    """
    Calculates the peer score based on activity.
    
    Parameters:
        peer_data (dict): Data of the peer.
    
    Returns:
        int: Updated score.
    """
    try:
        last_active_time = pd.Timestamp(peer_data["timestamp"])
        time_active = (pd.Timestamp.now() - last_active_time).seconds
        # Decay factor: Lower score for inactive peers
        score = max(0, 100 - (time_active // 60))  # Score decreases by 1 per minute
        return score
    except Exception as e:
        logging.error(f"Error calculating score for peer {peer_data.get('name', 'unknown')}: {e}")
        return 0

def fetch_peer_prediction(peer_key):
    """
    Fetches the prediction made by a peer.
    
    Parameters:
        peer_key (str): Redis key of the peer.
    
    Returns:
        int: Peer prediction (0 or 1).
    """
    try:
        peer_data = json.loads(redis_client.get(peer_key))
        # Assuming peer predictions are stored under 'last_prediction'
        return int(peer_data.get('last_prediction', random.choice([0, 1])))
    except Exception as e:
        logging.error(f"Error fetching peer prediction for {peer_key}: {e}")
        return random.choice([0, 1])  # Fallback to random prediction

def calculate_target(data):
    """
    Calculates the target variable for classification based on future price movement.
    
    Parameters:
        data (pd.DataFrame): Historical data with indicators.
    
    Returns:
        pd.DataFrame: Data with the 'Target' column added.
    """
    try:
        # Define target as whether the next minute's close price is higher than current
        data['Target'] = (data['Close'].shift(-1) > data['Close']).astype(int)
        data.dropna(inplace=True)
        return data
    except Exception as e:
        logging.error(f"Error calculating target: {e}")
        return pd.DataFrame()

def calculate_indicators(data):
    """
    Calculates technical indicators for the given data.
    
    Parameters:
        data (pd.DataFrame): Historical market data.
    
    Returns:
        pd.DataFrame: Data with technical indicators added.
    """
    try:
        data['SMA'] = data['Close'].rolling(window=5).mean()
        data['EMA'] = data['Close'].ewm(span=10, adjust=False).mean()
        # Add more indicators as needed (e.g., RSI, MACD)
        data.dropna(inplace=True)
        return data
    except Exception as e:
        logging.error(f"Error calculating indicators: {e}")
        return pd.DataFrame()

def process_stock(symbol, data):
    """
    Processes stock data through the swarm intelligence model to generate predictions.
    Returns the prediction and confidence level.
    
    Parameters:
        symbol (str): The stock symbol.
        data (pd.DataFrame): Historical data with indicators.
    
    Returns:
        tuple: (prediction (int), confidence (float))
    """
    try:
        # Prepare features and target
        data = calculate_target(data)
        if data.empty:
            logging.warning(f"No valid target for {symbol}.")
            return None, 0.0

        features = ['SMA', 'EMA']
        target = 'Target'
        X = data[features]
        y = data[target]

        # Load or train model
        model_file = f"models/{symbol}_model.joblib"
        scaler_file = f"models/{symbol}_scaler.joblib"
        
        if os.path.exists(model_file) and os.path.exists(scaler_file):
            model = load_model(model_file)
            scaler = load_scaler(scaler_file)
            if model is None or scaler is None:
                logging.warning(f"Model or scaler not loaded for {symbol}. Retraining...")
                raise FileNotFoundError("Model or scaler files missing.")
        else:
            # Train a new model
            logging.info(f"Training new model for {symbol}...")
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            model = RandomForestClassifier(n_estimators=100, random_state=42)
            model.fit(X_train_scaled, y_train)
            save_model(model, model_file)
            save_scaler(scaler, scaler_file)
            logging.info(f"Model trained and saved for {symbol}.")

        # Prepare the latest data point for prediction
        latest_data = data.iloc[-1:][['SMA', 'EMA']]
        latest_scaled = scaler.transform(latest_data)
        prediction = model.predict(latest_scaled)[0]
        confidence = model.predict_proba(latest_scaled).max() * 100  # Confidence as percentage

        # Swarm Intelligence: Adjust prediction based on peer input
        if len(peer_scores) >= 3:  # Ensure sufficient peer data
            prediction = swarm_confidence_adjustment(prediction)
            logging.info(f"Swarm Adjusted Prediction for {symbol}: {prediction}")

        return prediction, confidence
    except Exception as e:
        logging.error(f"Error processing stock {symbol}: {e}")
        return None, 0.0

def save_model(model, filepath):
    """
    Saves the trained model to a file.
    
    Parameters:
        model: Trained machine learning model.
        filepath (str): Path to save the model.
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        dump(model, filepath)
        logging.info(f"Model saved to {filepath}.")
    except Exception as e:
        logging.error(f"Error saving model to {filepath}: {e}")

def load_model(filepath):
    """
    Loads a trained model from a file.
    
    Parameters:
        filepath (str): Path to the saved model.
    
    Returns:
        Loaded model or None if failed.
    """
    try:
        model = load(filepath)
        logging.info(f"Model loaded from {filepath}.")
        return model
    except Exception as e:
        logging.error(f"Error loading model from {filepath}: {e}")
        return None

def save_scaler(scaler, filepath):
    """
    Saves the scaler to a file.
    
    Parameters:
        scaler: Trained scaler.
        filepath (str): Path to save the scaler.
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        dump(scaler, filepath)
        logging.info(f"Scaler saved to {filepath}.")
    except Exception as e:
        logging.error(f"Error saving scaler to {filepath}: {e}")

def load_scaler(filepath):
    """
    Loads a scaler from a file.
    
    Parameters:
        filepath (str): Path to the saved scaler.
    
    Returns:
        Loaded scaler or None if failed.
    """
    try:
        scaler = load(filepath)
        logging.info(f"Scaler loaded from {filepath}.")
        return scaler
    except Exception as e:
        logging.error(f"Error loading scaler from {filepath}: {e}")
        return None

def swarm_confidence_adjustment(prediction):
    """
    Adjusts the prediction based on swarm (peer) confidence and scores.
    
    Parameters:
        prediction (int): Initial prediction (0 or 1).
    
    Returns:
        int: Adjusted prediction.
    """
    try:
        total_score = sum(peer_scores.values())
        if total_score == 0:
            logging.warning("Total peer scores are zero. Skipping swarm adjustment.")
            return prediction  # Avoid division by zero

        weighted_sum = prediction * peer_scores.get('self', 1)  # Include self score
        total_weight = peer_scores.get('self', 1)

        for peer_key, score in peer_scores.items():
            if peer_key == 'self':
                continue  # Skip self if already included
            peer_prediction = fetch_peer_prediction(peer_key)
            weighted_sum += peer_prediction * score
            total_weight += score

        weighted_prediction = weighted_sum / total_weight
        adjusted_prediction = int(round(weighted_prediction))

        logging.info(f"Swarm Adjusted Prediction: {adjusted_prediction} based on total score {total_weight}")
        return adjusted_prediction
    except Exception as e:
        logging.error(f"Error in swarm_confidence_adjustment: {e}")
        return prediction  # Return original prediction on error

def fetch_alpaca_data(symbol, start_date, end_date):
    """
    Fetches historical market data from Alpaca for a given symbol and date range.
    
    Parameters:
        symbol (str): The stock symbol to fetch data for.
        start_date (str or datetime): The start date in ISO format.
        end_date (str or datetime): The end date in ISO format.
    
    Returns:
        pd.DataFrame: DataFrame containing historical bars data.
    """
    try:
        # Convert to datetime if strings are provided
        if isinstance(start_date, str):
            try:
                start_date = datetime.fromisoformat(start_date).astimezone(timezone.utc)
            except ValueError:
                start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if isinstance(end_date, str):
            try:
                end_date = datetime.fromisoformat(end_date).astimezone(timezone.utc)
            except ValueError:
                end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        
        # Fetch bars data
        bars = alpaca_client.get_bars(
            symbol,
            TimeFrame.Minute,
            start=start_date.isoformat(),
            end=end_date.isoformat()
        ).df

        if bars.empty:
            logging.warning(f"No bars returned for {symbol} between {start_date.isoformat()} and {end_date.isoformat()}.")
            return pd.DataFrame()
        
        # Add symbol as a column
        bars['symbol'] = symbol
        return bars
    except Exception as e:
        logging.error(f"Error fetching data for {symbol} from Alpaca: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def calculate_target(data):
    """
    Calculates the target variable for classification based on future price movement.
    
    Parameters:
        data (pd.DataFrame): Historical data with indicators.
    
    Returns:
        pd.DataFrame: Data with the 'Target' column added.
    """
    try:
        # Define target as whether the next minute's close price is higher than current
        data['Target'] = (data['Close'].shift(-1) > data['Close']).astype(int)
        data.dropna(inplace=True)
        return data
    except Exception as e:
        logging.error(f"Error calculating target: {e}")
        return pd.DataFrame()

def calculate_indicators(data):
    """
    Calculates technical indicators for the given data.
    
    Parameters:
        data (pd.DataFrame): Historical market data.
    
    Returns:
        pd.DataFrame: Data with technical indicators added.
    """
    try:
        data['SMA'] = data['Close'].rolling(window=5).mean()
        data['EMA'] = data['Close'].ewm(span=10, adjust=False).mean()
        # Add more indicators as needed (e.g., RSI, MACD)
        data.dropna(inplace=True)
        return data
    except Exception as e:
        logging.error(f"Error calculating indicators: {e}")
        return pd.DataFrame()

def process_stock(symbol, data):
    """
    Processes stock data through the swarm intelligence model to generate predictions.
    Returns the prediction and confidence level.
    
    Parameters:
        symbol (str): The stock symbol.
        data (pd.DataFrame): Historical data with indicators.
    
    Returns:
        tuple: (prediction (int), confidence (float))
    """
    try:
        # Prepare features and target
        data = calculate_target(data)
        if data.empty:
            logging.warning(f"No valid target for {symbol}.")
            return None, 0.0

        features = ['SMA', 'EMA']
        target = 'Target'
        X = data[features]
        y = data[target]

        # Load or train model
        model_file = f"models/{symbol}_model.joblib"
        scaler_file = f"models/{symbol}_scaler.joblib"
        
        if os.path.exists(model_file) and os.path.exists(scaler_file):
            model = load_model(model_file)
            scaler = load_scaler(scaler_file)
            if model is None or scaler is None:
                logging.warning(f"Model or scaler not loaded for {symbol}. Retraining...")
                raise FileNotFoundError("Model or scaler files missing.")
        else:
            # Train a new model
            logging.info(f"Training new model for {symbol}...")
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            model = RandomForestClassifier(n_estimators=100, random_state=42)
            model.fit(X_train_scaled, y_train)
            save_model(model, model_file)
            save_scaler(scaler, scaler_file)
            logging.info(f"Model trained and saved for {symbol}.")

        # Prepare the latest data point for prediction
        latest_data = data.iloc[-1:][['SMA', 'EMA']]
        latest_scaled = scaler.transform(latest_data)
        prediction = model.predict(latest_scaled)[0]
        confidence = model.predict_proba(latest_scaled).max() * 100  # Confidence as percentage

        # Swarm Intelligence: Adjust prediction based on peer input
        if len(peer_scores) >= 3:  # Ensure sufficient peer data
            prediction = swarm_confidence_adjustment(prediction)
            logging.info(f"Swarm Adjusted Prediction for {symbol}: {prediction}")

        return prediction, confidence
    except Exception as e:
        logging.error(f"Error processing stock {symbol}: {e}")
        return None, 0.0

def save_model(model, filepath):
    """
    Saves the trained model to a file.
    
    Parameters:
        model: Trained machine learning model.
        filepath (str): Path to save the model.
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        dump(model, filepath)
        logging.info(f"Model saved to {filepath}.")
    except Exception as e:
        logging.error(f"Error saving model to {filepath}: {e}")

def load_model(filepath):
    """
    Loads a trained model from a file.
    
    Parameters:
        filepath (str): Path to the saved model.
    
    Returns:
        Loaded model or None if failed.
    """
    try:
        model = load(filepath)
        logging.info(f"Model loaded from {filepath}.")
        return model
    except Exception as e:
        logging.error(f"Error loading model from {filepath}: {e}")
        return None

def save_scaler(scaler, filepath):
    """
    Saves the scaler to a file.
    
    Parameters:
        scaler: Trained scaler.
        filepath (str): Path to save the scaler.
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        dump(scaler, filepath)
        logging.info(f"Scaler saved to {filepath}.")
    except Exception as e:
        logging.error(f"Error saving scaler to {filepath}: {e}")

def load_scaler(filepath):
    """
    Loads a scaler from a file.
    
    Parameters:
        filepath (str): Path to the saved scaler.
    
    Returns:
        Loaded scaler or None if failed.
    """
    try:
        scaler = load(filepath)
        logging.info(f"Scaler loaded from {filepath}.")
        return scaler
    except Exception as e:
        logging.error(f"Error loading scaler from {filepath}: {e}")
        return None

def swarm_confidence_adjustment(prediction):
    """
    Adjusts the prediction based on swarm (peer) confidence and scores.
    
    Parameters:
        prediction (int): Initial prediction (0 or 1).
    
    Returns:
        int: Adjusted prediction.
    """
    try:
        total_score = sum(peer_scores.values())
        if total_score == 0:
            logging.warning("Total peer scores are zero. Skipping swarm adjustment.")
            return prediction  # Avoid division by zero

        weighted_sum = prediction * peer_scores.get('self', 1)  # Include self score
        total_weight = peer_scores.get('self', 1)

        for peer_key, score in peer_scores.items():
            if peer_key == 'self':
                continue  # Skip self if already included
            peer_prediction = fetch_peer_prediction(peer_key)
            weighted_sum += peer_prediction * score
            total_weight += score

        weighted_prediction = weighted_sum / total_weight
        adjusted_prediction = int(round(weighted_prediction))

        logging.info(f"Swarm Adjusted Prediction: {adjusted_prediction} based on total score {total_weight}")
        return adjusted_prediction
    except Exception as e:
        logging.error(f"Error in swarm_confidence_adjustment: {e}")
        return prediction  # Return original prediction on error

def fetch_peer_prediction(peer_key):
    """
    Fetches the prediction made by a peer.
    
    Parameters:
        peer_key (str): Redis key of the peer.
    
    Returns:
        int: Peer prediction (0 or 1).
    """
    try:
        peer_data = json.loads(redis_client.get(peer_key))
        # Assuming peer predictions are stored under 'last_prediction'
        return int(peer_data.get('last_prediction', random.choice([0, 1])))
    except Exception as e:
        logging.error(f"Error fetching peer prediction for {peer_key}: {e}")
        return random.choice([0, 1])  # Fallback to random prediction

def execute_trade(symbol, side, qty, stop_loss=None, take_profit=None):
    """
    Executes a trade on Alpaca with optional stop-loss and take-profit.
    
    Parameters:
        symbol (str): The stock symbol to trade.
        side (str): 'buy' or 'sell'.
        qty (int): Quantity to trade.
        stop_loss (float, optional): Price to trigger stop-loss.
        take_profit (float, optional): Price to trigger take-profit.
    """
    try:
        if stop_loss or take_profit:
            # Submit a bracket order
            order = alpaca_client.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                type='market',
                time_in_force='gtc',
                order_class='bracket',
                stop_loss={'stop_price': stop_loss},
                take_profit={'limit_price': take_profit}
            )
            logging.info(f"Bracket order submitted for {symbol}: {order}")
        else:
            # Submit a regular market order
            order = alpaca_client.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                type='market',
                time_in_force='gtc'
            )
            logging.info(f"Market order submitted for {symbol}: {order}")

        # Optional: Wait for order to be filled
        wait_for_order_fill(order)
    except Exception as e:
        logging.error(f"Error executing trade for {symbol}: {e}")

def wait_for_order_fill(order, timeout=60):
    """
    Waits for the order to be filled or times out.
    
    Parameters:
        order (dict): The order returned by Alpaca.
        timeout (int): Maximum time to wait in seconds.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            current_order = alpaca_client.get_order(order.id)
            if current_order.filled_qty == current_order.qty:
                logging.info(f"Order filled for {current_order.symbol}: {current_order}")
                return
            elif current_order.status in ['canceled', 'expired', 'rejected']:
                logging.warning(f"Order {current_order.status} for {current_order.symbol}: {current_order}")
                return
            else:
                time.sleep(5)  # Wait before checking again
        except Exception as e:
            logging.error(f"Error fetching order status: {e}")
            time.sleep(5)
    logging.warning(f"Order {order.id} for {order.symbol} not filled within timeout.")

def determine_position_size(symbol, max_position_size):
    """
    Determines the quantity to trade based on available funds and risk management rules.
    
    Parameters:
        symbol (str): The stock symbol.
        max_position_size (int): The maximum allowable position size.
    
    Returns:
        int: Quantity to trade.
    """
    try:
        # Example: Allocate 2% of available funds to each trade
        account = alpaca_client.get_account()
        available_funds = float(account.cash)
        allocation_percentage = float(os.getenv('ALLOCATION_PERCENTAGE', 0.02))  # 2%
        allocation = available_funds * allocation_percentage  # Allocation per trade

        # Get the current price of the symbol
        last_trade = alpaca_client.get_last_trade(symbol)
        last_price = float(last_trade.price)

        # Calculate quantity based on allocation and last price
        qty = int(allocation / last_price)
        qty = min(qty, max_position_size)
        return qty
    except Exception as e:
        logging.error(f"Error determining position size for {symbol}: {e}")
        return 0  # Return 0 to skip the trade in case of error

def calculate_stop_prices(data):
    """
    Calculates dynamic stop-loss and take-profit prices based on ATR.
    
    Parameters:
        data (pd.DataFrame): Historical data with indicators.
    
    Returns:
        tuple: (stop_loss (float), take_profit (float))
    """
    try:
        # Calculate ATR (Average True Range)
        high_low = data['High'] - data['Low']
        high_close_prev = (data['High'] - data['Close'].shift()).abs()
        low_close_prev = (data['Low'] - data['Close'].shift()).abs()
        tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
        atr = tr.rolling(window=14).mean().iloc[-1]  # 14-period ATR

        last_close = data['Close'].iloc[-1]
        stop_loss = round(last_close - (atr * 2), 2)  # 2 ATR below
        take_profit = round(last_close + (atr * 2), 2)  # 2 ATR above

        return stop_loss, take_profit
    except Exception as e:
        logging.error(f"Error calculating stop prices: {e}")
        return None, None  # Return None if calculation fails

def can_trade(symbol):
    """
    Determines if trading is allowed for a symbol based on cooldown periods.
    
    Parameters:
        symbol (str): The stock symbol.
    
    Returns:
        bool: True if trading is allowed, False otherwise.
    """
    try:
        last_trade_time_str = redis_client.get(f"tina:last_trade:{symbol}")
        if last_trade_time_str:
            last_trade_time = pd.Timestamp(last_trade_time_str)
            elapsed_time = (pd.Timestamp.now() - last_trade_time).seconds
            if elapsed_time < COOLDOWN_PERIOD:
                logging.info(f"Cooldown active for {symbol}. Time remaining: {COOLDOWN_PERIOD - elapsed_time} seconds.")
                return False
        return True
    except Exception as e:
        logging.error(f"Error checking trade cooldown for {symbol}: {e}")
        return False  # Prevent trading if an error occurs

def update_last_trade_time(symbol):
    """
    Updates the last trade time for a symbol in Redis.
    
    Parameters:
        symbol (str): The stock symbol.
    """
    try:
        current_time = pd.Timestamp.now().isoformat()
        redis_client.set(f"tina:last_trade:{symbol}", current_time)
        logging.info(f"Updated last trade time for {symbol} to {current_time}.")
    except Exception as e:
        logging.error(f"Error updating last trade time for {symbol}: {e}")

def publish_kafka_event(message):
    """
    Publishes a message to the Kafka 'tina_events' topic.
    
    Parameters:
        message (dict): The message to publish.
    """
    try:
        kafka_producer.send('tina_events', value=message)
        kafka_producer.flush()
        logging.info(f"Published event to Kafka: {message}")
    except Exception as e:
        logging.error(f"Error publishing Kafka event: {e}")

def main():
    """
    Main function to orchestrate data fetching, processing, trading, and monitoring.
    """
    # List of symbols to trade
    symbols = ["AAPL", "MSFT", "TSLA"]  # Extend as needed

    # Define maximum position size (configurable via environment variables)
    MAX_POSITION_SIZE = int(os.getenv('MAX_POSITION_SIZE', 1000))

    # Define cooldown period in seconds to prevent overtrading
    COOLDOWN_PERIOD = int(os.getenv('COOLDOWN_PERIOD', 300))  # 5 minutes

    # Swarm confidence threshold
    SWARM_CONFIDENCE_THRESHOLD = float(os.getenv('SWARM_CONFIDENCE_THRESHOLD', 70.0))

    # Define allocation percentage
    ALLOCATION_PERCENTAGE = float(os.getenv('ALLOCATION_PERCENTAGE', 0.02))  # 2%

    # Main trading loop
    while True:
        try:
            # Define date range for fetching historical data (last 5 days)
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=5)

            # Loop through each symbol to fetch and process data
            for symbol in symbols:
                logging.info(f"Processing {symbol}...")

                # Check if trading is allowed for the symbol
                if not can_trade(symbol):
                    logging.info(f"Cooldown active for {symbol}. Skipping trade.")
                    continue

                # Fetch data from Alpaca
                data = fetch_alpaca_data(symbol, start_date, end_date)
                if data.empty:
                    logging.warning(f"No data available for {symbol}. Skipping...")
                    continue

                # Calculate technical indicators
                data = calculate_indicators(data)
                if data.empty:
                    logging.warning(f"Indicators could not be calculated for {symbol}. Skipping...")
                    continue

                # Run swarm intelligence model to make a prediction
                prediction, confidence = process_stock(symbol, data)
                if prediction is None:
                    logging.warning(f"Prediction could not be made for {symbol}. Skipping...")
                    continue

                # Risk management: determine position size and stop-loss/take-profit
                qty = determine_position_size(symbol, MAX_POSITION_SIZE)
                if qty <= 0:
                    logging.warning(f"Calculated position size is {qty} for {symbol}. Skipping trade.")
                    continue

                stop_loss, take_profit = calculate_stop_prices(data)
                if stop_loss is None or take_profit is None:
                    logging.warning(f"Stop-loss or take-profit not set for {symbol}. Skipping trade.")
                    continue

                # Execute the trade based on prediction and confidence
                if prediction == 1 and confidence > SWARM_CONFIDENCE_THRESHOLD:  # Buy signal
                    logging.info(f"Prediction for {symbol}: Buy. Confidence: {confidence:.2f}%")
                    execute_trade(symbol, 'buy', qty, stop_loss, take_profit)
                    update_last_trade_time(symbol)  # Update trade timestamp
                elif prediction == 0 and confidence > SWARM_CONFIDENCE_THRESHOLD:  # Sell signal
                    logging.info(f"Prediction for {symbol}: Sell. Confidence: {confidence:.2f}%")
                    execute_trade(symbol, 'sell', qty, stop_loss, take_profit)
                    update_last_trade_time(symbol)  # Update trade timestamp
                else:
                    logging.info(f"No strong prediction for {symbol}. Skipping trade.")

                logging.info(f"Completed processing for {symbol}.")

            # Publish system status and manage peers
            system_status = {
                "status": "running",
                "active_peers": len(redis_client.keys("tina:peer:*"))
            }
            publish_kafka_event(system_status)

            manage_peers()

            # Sleep to avoid hitting API rate limits and control polling frequency
            time.sleep(60)  # Adjust sleep time as necessary

        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            # Implement exponential backoff to handle persistent errors
            for i in range(5):
                sleep_time = min(300, 2 ** i * 60)  # Cap at 5 minutes
                logging.info(f"Sleeping for {sleep_time} seconds before retrying...")
                time.sleep(sleep_time)
            logging.info("Retrying the main loop after backoff.")
