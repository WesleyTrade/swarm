from base_agent import BaseAgent
import requests
import json
from kafka import KafkaProducer

class WorldBankAgent(BaseAgent):
    def __init__(self, agent_id):
        super().__init__(agent_id)

    def fetch_data(self, indicator, countries='all', date='2000:2023', per_page=1000):
        base_url = 'https://api.worldbank.org/v2'
        endpoint = f'/country/{countries}/indicator/{indicator}'
        params = {
            'date': date,
            'format': 'json',
            'per_page': per_page
        }
        url = base_url + endpoint
        response = requests.get(url, params=params)
        data = response.json()
        # Check for errors
        if not data or isinstance(data, dict) and 'message' in data:
            self.log_activity('Error fetching data from World Bank API.')
            return []
        # Handle pagination
        total_pages = data[0]['pages']
        results = data[1]
        for page in range(2, total_pages + 1):
            params['page'] = page
            response = requests.get(url, params=params)
            page_data = response.json()
            results.extend(page_data[1])
        return results

    def process_data(self, data):
        processed_data = []
        for entry in data:
            if entry.get('value') is not None:
                processed_entry = {
                    'country': entry['country']['value'],
                    'indicator_id': entry['indicator']['id'],
                    'indicator_name': entry['indicator']['value'],
                    'date': entry['date'],
                    'value': entry['value']
                }
                processed_data.append(processed_entry)
        return processed_data

    def publish_to_kafka(self, topic, data):
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for record in data:
            producer.send(topic, value=record)
        producer.flush()

    def run(self):
        indicators = [
            'NY.GDP.MKTP.CD',     # GDP (current US$)
            'FP.CPI.TOTL.ZG',     # Inflation, consumer prices (annual %)
            'SL.UEM.TOTL.ZS',     # Unemployment, total (% of total labor force)
            # Add more indicators as needed
        ]
        for indicator in indicators:
            raw_data = self.fetch_data(indicator=indicator, date='2010:2023')
            if raw_data:
                data = self.process_data(raw_data)
                self.publish_to_kafka('world-bank-data', data)
                self.log_activity(f'Published data for indicator {indicator}')
            else:
                self.log_activity(f'No data for indicator {indicator}')

