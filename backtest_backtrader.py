import backtrader as bt
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

class RandomForestStrategy(bt.Strategy):
    def __init__(self):
        self.dataclose = self.datas[0].close
        # Initialize ML model here or load pre-trained model
        # For simplicity, use a dummy model
    
    def next(self):
        # Dummy decision: Buy if price is increasing, Sell otherwise
        if self.dataclose[0] > self.dataclose[-1]:
            self.buy()
        elif self.dataclose[0] < self.dataclose[-1]:
            self.sell()

def run_backtest(data):
    cerebro = bt.Cerebro()
    cerebro.addstrategy(RandomForestStrategy)
    
    data_feed = bt.feeds.PandasData(dataname=data)
    cerebro.adddata(data_feed)
    
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    
    logging.info(f'Starting Portfolio Value: {cerebro.broker.getvalue()}')
    cerebro.run()
    logging.info(f'Final Portfolio Value: {cerebro.broker.getvalue()}')
    cerebro.plot()

if __name__ == '__main__':
    # Load historical data
    data = pd.read_csv('historical_data.csv', parse_dates=True, index_col='Date')
    run_backtest(data)
