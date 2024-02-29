from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_SOURCE = "invoice"

POSTGRES_URL = "jdbc:postgresql://postgresql:5432/invoice_db"  # Make sure this matches your docker-compose and script settings
POSTGRES_USER = "my_user"
POSTGRES_PASSWORD = "my_password"

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC_SOURCE) \
    .option("startingOffsets", "earliest") \
    .load()

json_schema = StructType([
  StructField("BillNum", StringType()),
  StructField("CreatedTime", TimestampType()),
  StructField("StoreID", StringType()), 
  StructField("PaymentMode", StringType()),
  StructField("TotalValue", DoubleType()),
])



# Read data stream from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC_SOURCE) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON from the Kafka message
json_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

def foreach_batch_function(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "invoices") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
query = json_df.writeStream.foreachBatch(foreach_batch_function).start()

query.awaitTermination()