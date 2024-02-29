from pyspark.sql import SparkSession
import random
import json
import time

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "invoice"

spark = SparkSession.builder.appName("write_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

def generate_random_json():
    return json.dumps({
        "BillNum": str(random.randint(10000000, 99999999)),
        "CreatedTime": random.randint(1590000000000, 1600000000000),
        "StoreID": "STR" + str(random.randint(1000, 9999)),
        "PaymentMode": random.choice(["CARD", "CASH", "ONLINE", "WALLET"]),
        "TotalValue": round(random.uniform(100, 10000), 2)
    })

try:
    while True:
        # Generate a single JSON message
        message = generate_random_json()

        # Create a DataFrame with a single row
        df = spark.createDataFrame([(message,)], ["value"])

        # Write the single message to Kafka
        df.selectExpr("CAST(value AS STRING)") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
          .option("topic", KAFKA_TOPIC) \
          .save()

        print(f"Sent message: {message}")

        # Sleep for 1 second to send the next message approximately every second
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopped.")
