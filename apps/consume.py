from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

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

# Define the schema for the JSON data to ensure correct data types
json_schema = StructType([
  StructField("BillNum", StringType()),
  StructField("CreatedTime", TimestampType()),
  StructField("StoreID", StringType()), 
  StructField("PaymentMode", StringType()),
  StructField("TotalValue", DoubleType()),
])

# Read the stream from Kafka, specifying bootstrap servers, topic, and starting offsets
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC_SOURCE) \
    .option("startingOffsets", "latest") \  # Use "latest" to only read new messages
    .load()

# Parse the incoming JSON data from Kafka and extract the fields according to the predefined schema
json_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

# Function to write the streaming data to PostgreSQL for each batch
def foreach_batch_function(df, epoch_id):
    # Write the DataFrame to PostgreSQL, specifying connection properties and table
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "invoices") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \  # Append mode ensures data is added to the table, not overwritten
        .save()

# Set up the streaming query to process each batch using the `foreachBatch` function
query = json_df.writeStream.foreachBatch(foreach_batch_function).start()

# Wait for the streaming query to terminate, processing indefinitely until stopped
query.awaitTermination()
