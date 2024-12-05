from influxdb_client import InfluxDBClient
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# InfluxDB Configuration
token = os.getenv('INFLUXDB_TOKEN')
org = os.getenv('INFLUXDB_ORG')
bucket = os.getenv('INFLUXDB_BUCKET')

client = InfluxDBClient(url='http://localhost:8086', token=token, org=org)
query_api = client.query_api()

def query_data():
    try:
        query = f'from(bucket:"{bucket}") |> range(start: -1h)'
        tables = query_api.query(query, org=org)
        for table in tables:
            for record in table.records:
                logging.info(f'Time: {record.get_time()}, Measurement: {record.get_measurement()}, Value: {record.get_value()}')
    except Exception as e:
        logging.error(f"Error querying InfluxDB: {e}")

if __name__ == '__main__':
    query_data()
