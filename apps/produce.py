from pyspark.sql import SparkSession
from datetime import datetime
import random
import json
import time

# Kafka configuration settings
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "invoice"

# Initialize SparkSession with a specific application name
spark = SparkSession.builder.appName("write_test_stream").getOrCreate()

# Set log level to WARN to reduce verbosity of Spark output
spark.sparkContext.setLogLevel("WARN")

def generate_random_json():
    """
    Generates a random JSON string representing an invoice with a unique bill number,
    creation time, store ID, payment mode, and total value.
    """
    # Convert milliseconds since epoch to a datetime object
    created_time = datetime.utcfromtimestamp(random.randint(1590000000000, 1600000000000) / 1000.0)

    # Format the datetime object as a string in ISO 8601 format
    created_time_str = created_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Return a JSON string with random data for the invoice
    return json.dumps({
        "BillNum": str(random.randint(10000000, 99999999)),
        "CreatedTime": created_time_str,
        "StoreID": "STR" + str(random.randint(1000, 9999)),
        "PaymentMode": random.choice(["CARD", "CASH", "ONLINE", "WALLET"]),
        "TotalValue": round(random.uniform(100, 10000), 2)
    })

try:
    while True:
        # Generate a random JSON message representing an invoice
        message = generate_random_json()

        # Create a DataFrame with a single row containing the message
        df = spark.createDataFrame([(message,)], ["value"])

        # Convert the DataFrame column to a string and write the message to Kafka
        df.selectExpr("CAST(value AS STRING)") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
          .option("topic", KAFKA_TOPIC) \
          .save()

        print(f"Sent message: {message}")

        # Pause for 1 second to regulate the message send rate
        time.sleep(1)

except KeyboardInterrupt:
    # Gracefully handle a manual script stop
    print("Stopped.")
