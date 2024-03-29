from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Kafka and PostgreSQL configuration settings
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_SOURCE = "invoice"
POSTGRES_URL = "jdbc:postgresql://postgresql:5432/invoice_db" 
POSTGRES_USER = "my_user"
POSTGRES_PASSWORD = "my_password"

# Initialize SparkSession
spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

# Set log level to WARN to reduce verbosity of Spark output
spark.sparkContext.setLogLevel("WARN")

# Read the stream from Kafka, specifying bootstrap servers, topic, and starting offsets
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC_SOURCE) \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema for the JSON data to ensure correct data types
json_schema = StructType([
  StructField("BillNum", StringType()),
  StructField("CreatedTime", TimestampType()),
  StructField("StoreID", StringType()), 
  StructField("PaymentMode", StringType()),
  StructField("TotalValue", DoubleType()),
])


# Parse the incoming JSON data from Kafka and extract the fields according to the predefined schema
json_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

# Function to write the streaming data to PostgreSQL for each batch
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

# Set up the streaming query to process each batch using the `foreachBatch` function
query = json_df.writeStream.foreachBatch(foreach_batch_function).start()



# AGGREGRATION

# Aggregate data over a 10-second window
aggregated_df = json_df \
    .withWatermark("CreatedTime", "2 minutes") \
    .groupBy(
        window("CreatedTime", "10 seconds"),  # 10-second window
        "StoreID"
    ) \
    .agg(
        sum("TotalValue").alias("TotalSales")  # Sum TotalValue for each StoreID in the window
    ) \
    .select(
        col("window.start").alias("WindowStart"), 
        col("window.end").alias("WindowEnd"), 
        col("StoreID"), 
        col("TotalSales")
    )

def foreach_batch_function_aggregated(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "store_sales_summary") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Set up the streaming query for the aggregated data
query_aggregated = aggregated_df.writeStream.foreachBatch(foreach_batch_function_aggregated).outputMode("update").start()



queries = [query, query_aggregated]

# Wait for all streaming queries to terminate
for q in queries:
    q.awaitTermination()