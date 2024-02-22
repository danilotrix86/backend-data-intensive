import json
import time
from datetime import datetime
import random
import sys
import six
from cryptography.fernet import Fernet

# Workaround for Python 3.12 and kafka-python compatibility issue
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
from kafka import KafkaProducer

# Kafka configuration
kafka_topic = 'sensor_data'
kafka_server = 'localhost:9092' 

# Read encryption key from file
with open('encryption_key.txt', 'rb') as file:
    encryption_key = file.read()
cipher_suite = Fernet(encryption_key)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_server],
    acks='all'  # Ensure all in-sync replicas acknowledge receipt of the message, providing the highest data durability and reliability
)

def generate_sensor_data():
    """Generates a fake sensor data reading with more structure and variability."""
    sensor_type = random.choice(['temperature', 'humidity', 'pressure'])
    sensor_id = f'sensor_{random.randint(1, 100)}'
    timestamp = datetime.now().isoformat()

    value = 0
    if sensor_type == 'temperature':
        # Simulating daily temperature variation
        hour = datetime.now().hour
        base_temp = 20 if 6 <= hour <= 18 else 15  # warmer during the day
        value = round(random.uniform(base_temp, base_temp + 10), 2)
    elif sensor_type == 'humidity':
        value = round(random.uniform(30, 90), 2)  # Random humidity percentage
    elif sensor_type == 'pressure':
        value = round(random.uniform(750, 770), 2)  # Pressure in millibars

    # Simulate occasional erroneous data
    if random.random() < 0.01:  # 1% chance of error
        value = None  # Simulating a missing or faulty reading

    sensor_data = {
        'timestamp': timestamp,
        'sensor_id': sensor_id,
        'type': sensor_type,
        'value': value
    }
    return sensor_data

def encrypt_data(data):
    """Encrypts the data."""
    return cipher_suite.encrypt(data.encode('utf-8'))

def send_data_to_kafka(data):
    """Sends encrypted data to a Kafka topic."""
    encrypted_data = encrypt_data(json.dumps(data))
    producer.send(kafka_topic, value=encrypted_data)
    producer.flush()  # Ensure data is sent to Kafka

if __name__ == "__main__":
    try:
        while True:
            data = generate_sensor_data()
            send_data_to_kafka(data)
            print(f"Encrypted Data sent: {data}")
            time.sleep(1)  # Data is generated every second
    except KeyboardInterrupt:
        print("Data generation stopped.")
