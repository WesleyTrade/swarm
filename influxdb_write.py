from influxdb_client import InfluxDBClient, Point, WritePrecision
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# InfluxDB Configuration
token = os.getenv('INFLUXDB_TOKEN')
org = os.getenv('INFLUXDB_ORG')
bucket = os.getenv('INFLUXDB_BUCKET')

client = InfluxDBClient(url='http://localhost:8086', token=token, org=org)
write_api = client.write_api(write_options=WritePrecision.NS)

def write_sample_data():
    try:
        point = Point("test_measurement").tag("sensor", "test_sensor").field("value", 25.3)
        write_api.write(bucket=bucket, org=org, record=point)
        logging.info("Sample data written to InfluxDB.")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB: {e}")

if __name__ == '__main__':
    write_sample_data()
