import pandas as pd
import numpy as np
import logging
import talib  # Ensure you have TA-Lib installed

# Configure logging
logging.basicConfig(level=logging.INFO)

def compute_rsi(close_prices, window=14):
    try:
        rsi = talib.RSI(close_prices, timeperiod=window)
        return rsi
    except Exception as e:
        logging.error(f"Error computing RSI: {e}")
        return pd.Series()

def compute_macd(close_prices):
    try:
        macd, signal, hist = talib.MACD(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
        return macd, signal
    except Exception as e:
        logging.error(f"Error computing MACD: {e}")
        return pd.Series(), pd.Series()

def calculate_indicators(data):
    try:
        data['SMA'] = data['Close'].rolling(window=5).mean()
        data['EMA'] = data['Close'].ewm(span=10).mean()
        data['RSI'] = compute_rsi(data['Close'])
        data['MACD'], data['Signal'] = compute_macd(data['Close'])
        data.dropna(inplace=True)
        return data
    except Exception as e:
        logging.error(f"Error calculating indicators: {e}")
        return pd.DataFrame()
