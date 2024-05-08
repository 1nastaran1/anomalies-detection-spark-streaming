import pandas as pd
from kafka import KafkaProducer
import time

# Read the data from the CSV file using pandas
data = pd.read_csv('timeseries.csv')

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: str(v).encode('utf-8'))

# Convert each row of the dataframe to a JSON string and send it to the Kafka topic
for index, row in data.iterrows():
    json_data = row.to_json()
    producer.send('final-project', json_data)
    time.sleep(0.01)  # Introduce a small delay to simulate streaming behavior

producer.flush()
