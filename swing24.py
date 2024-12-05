import pandas as pd
import numpy as np
from alpha_vantage.foreignexchange import ForeignExchange
from plyer import notification
import logging
import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

# Check if API_KEY is set
if not API_KEY:
    logging.error("API Key not found. Please set the ALPHAVANTAGE_API_KEY in the .env file.")
    exit(1)

CURRENCY_PAIRS = [
    'GBPAUD', 'EURJPY', 'EURAUD', 'GBPJPY', 'GBPCAD', 'AUDJPY', 'USDJPY', 'EURCAD',
    'AUDCAD', 'NZDJPY', 'CADJPY', 'GBPUSD', 'EURUSD', 'EURCHF', 'AUDUSD', 'USDCHF',
    'USDCAD', 'EURGBP', 'NZDUSD'
]
START_DATE = '2023-01-01'  # Adjusted to ensure data availability
INITIAL_BALANCE = 10000

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_forex_data(pair):
    fx = ForeignExchange(key=API_KEY, output_format='pandas')
    try:
        data, _ = fx.get_currency_exchange_daily(
            from_symbol=pair[:3],
            to_symbol=pair[3:],
            outputsize='full'
        )
        if not data.empty:
            data.index = pd.to_datetime(data.index)
            data = data.sort_index()  # Ensure chronological order
            data = data[data.index >= START_DATE]  # Filter by start date
            logging.info(f"Data for {pair} fetched successfully")
            return data
    except Exception as e:
        logging.error(f"Error fetching data for {pair}: {e}")
    return pd.DataFrame()

def calculate_mad(series):
    return np.fabs(series - series.mean()).mean()

def calculate_indicators(hist):
    # Define constants
    EMA_SHORT, EMA_MEDIUM, EMA_LONG = 10, 20, 50
    MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
    RSI_PERIOD = 14
    ATR_PERIOD = 14
    ADX_PERIOD = 14
    BOLLINGER_PERIOD, BOLLINGER_STD_DEV = 20, 2
    STOCH_K, STOCH_D = 14, 3
    CCI_PERIOD = 20
    RVI_PERIOD = 10  # Relative Vigor Index

    # Calculate EMAs
    hist['EMA10'] = hist['4. close'].ewm(span=EMA_SHORT, adjust=False).mean()
    hist['EMA20'] = hist['4. close'].ewm(span=EMA_MEDIUM, adjust=False).mean()
    hist['EMA50'] = hist['4. close'].ewm(span=EMA_LONG, adjust=False).mean()

    # Calculate MACD
    ema_fast = hist['4. close'].ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = hist['4. close'].ewm(span=MACD_SLOW, adjust=False).mean()
    hist['MACD'] = ema_fast - ema_slow
    hist['Signal'] = hist['MACD'].ewm(span=MACD_SIGNAL, adjust=False).mean()

    # Calculate RSI
    delta = hist['4. close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=RSI_PERIOD).mean()
    avg_loss = loss.rolling(window=RSI_PERIOD).mean()
    rs = avg_gain / avg_loss
    hist['RSI'] = 100 - (100 / (1 + rs))

    # Calculate ATR
    high_low = hist['2. high'] - hist['3. low']
    high_close = np.abs(hist['2. high'] - hist['4. close'].shift())
    low_close = np.abs(hist['3. low'] - hist['4. close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    hist['TR'] = tr
    hist['ATR'] = tr.rolling(window=ATR_PERIOD).mean()

    # Calculate ADX
    hist['+DM'] = hist['2. high'].diff()
    hist['-DM'] = hist['3. low'].diff()
    hist['+DM'] = hist['+DM'].where((hist['+DM'] > hist['-DM']) & (hist['+DM'] > 0), 0.0)
    hist['-DM'] = hist['-DM'].where((hist['-DM'] > hist['+DM']) & (hist['-DM'] > 0), 0.0)
    tr14 = hist['TR'].rolling(window=ADX_PERIOD).sum()
    plus_dm14 = hist['+DM'].rolling(window=ADX_PERIOD).sum()
    minus_dm14 = hist['-DM'].rolling(window=ADX_PERIOD).sum()
    hist['+DI14'] = 100 * (plus_dm14 / tr14)
    hist['-DI14'] = 100 * (minus_dm14 / tr14)
    hist['DX'] = 100 * np.abs(hist['+DI14'] - hist['-DI14']) / (hist['+DI14'] + hist['-DI14'])
    hist['ADX'] = hist['DX'].rolling(window=ADX_PERIOD).mean()

    # Calculate Bollinger Bands
    hist['Middle Band'] = hist['4. close'].rolling(window=BOLLINGER_PERIOD).mean()
    hist['Std Dev'] = hist['4. close'].rolling(window=BOLLINGER_PERIOD).std()
    hist['Upper Band'] = hist['Middle Band'] + (hist['Std Dev'] * BOLLINGER_STD_DEV)
    hist['Lower Band'] = hist['Middle Band'] - (hist['Std Dev'] * BOLLINGER_STD_DEV)

    # Calculate Stochastic Oscillator
    hist['Lowest Low'] = hist['3. low'].rolling(window=STOCH_K).min()
    hist['Highest High'] = hist['2. high'].rolling(window=STOCH_K).max()
    hist['%K'] = 100 * ((hist['4. close'] - hist['Lowest Low']) / (hist['Highest High'] - hist['Lowest Low']))
    hist['%D'] = hist['%K'].rolling(window=STOCH_D).mean()

    # Calculate CCI
    tp = (hist['2. high'] + hist['3. low'] + hist['4. close']) / 3
    sma_tp = tp.rolling(window=CCI_PERIOD).mean()
    mad = tp.rolling(window=CCI_PERIOD).apply(calculate_mad)
    hist['CCI'] = (tp - sma_tp) / (0.015 * mad)

    # Calculate Relative Vigor Index (RVI)
    close_open = hist['4. close'] - hist['1. open']
    high_low = hist['2. high'] - hist['3. low']
    numerator = close_open.rolling(window=RVI_PERIOD).mean()
    denominator = high_low.rolling(window=RVI_PERIOD).mean()
    hist['RVI'] = numerator / denominator

    # Smoothing RVI
    hist['RVI_Signal'] = hist['RVI'].rolling(window=4).mean()

    return hist

def interpret_analysis(pair, data):
    latest_data = data.iloc[-1]
    ema20 = latest_data['EMA20']
    ema50 = latest_data['EMA50']
    ema20_prev = data.iloc[-2]['EMA20']
    ema50_prev = data.iloc[-2]['EMA50']
    macd = latest_data['MACD']
    signal = latest_data['Signal']
    atr = latest_data['ATR']
    rsi = latest_data['RSI']
    rvi = latest_data['RVI']
    rvi_signal = latest_data['RVI_Signal']
    adx = latest_data['ADX']
    cci = latest_data['CCI']
    stoch_k = latest_data['%K']
    stoch_d = latest_data['%D']

    action = None
    duration = None
    reasons = []

    # EMA Crossover Signal
    if (ema20 > ema50) and (ema20_prev <= ema50_prev):
        reasons.append('EMA20 crossed above EMA50')
        ema_signal = 'Buy'
    elif (ema20 < ema50) and (ema20_prev >= ema50_prev):
        reasons.append('EMA20 crossed below EMA50')
        ema_signal = 'Sell'
    else:
        ema_signal = None

    # MACD Signal
    if (macd > signal):
        reasons.append('MACD above Signal line')
        macd_signal = 'Buy'
    elif (macd < signal):
        reasons.append('MACD below Signal line')
        macd_signal = 'Sell'
    else:
        macd_signal = None

    # RSI Signal
    if rsi < 30:
        reasons.append('RSI below 30 (Oversold)')
        rsi_signal = 'Buy'
    elif rsi > 70:
        reasons.append('RSI above 70 (Overbought)')
        rsi_signal = 'Sell'
    else:
        rsi_signal = None

    # RVI Signal
    if (rvi > rvi_signal):
        reasons.append('RVI above Signal line')
        rvi_signal = 'Buy'
    elif (rvi < rvi_signal):
        reasons.append('RVI below Signal line')
        rvi_signal = 'Sell'
    else:
        rvi_signal = None

    # Stochastic Oscillator Signal
    if (stoch_k > stoch_d) and (stoch_k < 20):
        reasons.append('Stochastic K crossed above D in Oversold region')
        stoch_signal = 'Buy'
    elif (stoch_k < stoch_d) and (stoch_k > 80):
        reasons.append('Stochastic K crossed below D in Overbought region')
        stoch_signal = 'Sell'
    else:
        stoch_signal = None

    # CCI Signal
    if cci < -100:
        reasons.append('CCI below -100 (Oversold)')
        cci_signal = 'Buy'
    elif cci > 100:
        reasons.append('CCI above 100 (Overbought)')
        cci_signal = 'Sell'
    else:
        cci_signal = None

    # Consolidate Signals
    buy_signals = [s for s in [ema_signal, macd_signal, rsi_signal, rvi_signal, stoch_signal, cci_signal] if s == 'Buy']
    sell_signals = [s for s in [ema_signal, macd_signal, rsi_signal, rvi_signal, stoch_signal, cci_signal] if s == 'Sell']

    if len(buy_signals) >= 3:
        action = 'Strong Buy'
        duration = 'Swing (>1h)'
    elif len(sell_signals) >= 3:
        action = 'Strong Sell'
        duration = 'Swing (>1h)'
    elif len(buy_signals) >= 2:
        action = 'Buy'
        duration = 'Scalping (<1h)'
    elif len(sell_signals) >= 2:
        action = 'Sell'
        duration = 'Scalping (<1h)'

    # Calculate Strength
    strength = (abs(macd - signal) / atr) * 100 if atr != 0 else 0

    return {
        'Pair': pair,
        'Close': latest_data['4. close'],
        'EMA20': ema20,
        'EMA50': ema50,
        'MACD': macd,
        'Signal': signal,
        'ATR': atr,
        'RSI': rsi,
        'ADX': adx,
        'Action': action,
        'Duration': duration,
        'Strength': strength,
        'Reasons': reasons
    }

def select_top_pairs(recommendations, top_n=3):
    # Filter out None actions
    valid_recommendations = [rec for rec in recommendations if rec['Action']]
    sorted_recommendations = sorted(valid_recommendations, key=lambda x: x['Strength'], reverse=True)
    return sorted_recommendations[:top_n]

def send_notification(title, message):
    if len(message) > 256:
        message = message[:253] + '...'
    notification.notify(
        title=title,
        message=message,
        timeout=10
    )

def main():
    # Fetch data
    results = {pair: fetch_forex_data(pair) for pair in CURRENCY_PAIRS}
    recommendations = []

    # Analyze data
    for pair, data in results.items():
        if not data.empty and len(data) > 50:
            data_with_indicators = calculate_indicators(data)
            analysis = interpret_analysis(pair, data_with_indicators)
            if analysis['Action']:
                recommendations.append(analysis)
                logging.info(f"{pair} analysis: {analysis}")

    # Select top pairs
    top_pairs = select_top_pairs(recommendations)

    # Display top pairs
    print("Top pairs to trade:")
    for pair in top_pairs:
        print(
            f"Pair: {pair['Pair']}, Action: {pair['Action']}, Close: {pair['Close']}\n"
            f"EMA20: {pair['EMA20']}, EMA50: {pair['EMA50']}, MACD: {pair['MACD']}, Signal: {pair['Signal']}\n"
            f"ATR: {pair['ATR']}, RSI: {pair['RSI']}, ADX: {pair['ADX']}, Duration: {pair['Duration']}, Strength: {pair['Strength']:.2f}%\n"
            f"Reasons: {', '.join(pair['Reasons'])}\n"
        )

    # Send notifications for top pairs
    for pair in top_pairs:
        message = (
            f"Pair: {pair['Pair']}\n"
            f"Action: {pair['Action']}\n"
            f"Close: {pair['Close']}\n"
            f"EMA20: {pair['EMA20']}\n"
            f"EMA50: {pair['EMA50']}\n"
            f"MACD: {pair['MACD']}\n"
            f"Signal: {pair['Signal']}\n"
            f"ATR: {pair['ATR']}\n"
            f"RSI: {pair['RSI']}\n"
            f"ADX: {pair['ADX']}\n"
            f"Duration: {pair['Duration']}\n"
            f"Strength: {pair['Strength']:.2f}%\n"
            f"Reasons: {', '.join(pair['Reasons'])}"
        )
        print(f"Sending notification for {pair['Pair']}")
        send_notification(f"Trading Signal Alert: {pair['Action']} {pair['Pair']}", message)

if __name__ == "__main__":
    main()
