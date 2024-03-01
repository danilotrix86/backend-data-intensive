# Streamlit Application with Apache Spark, Kafka, and PostgreSQL
This application demonstrates a real-time data pipeline using Apache Spark for processing, Apache Kafka for messaging, and PostgreSQL for storage, all visualized through a Streamlit interface.

## Prerequisites
Before you begin, ensure you have Docker and Docker Compose installed on your system. This application uses Docker Compose to orchestrate multiple services, including Apache Spark, Apache Kafka, PostgreSQL, and a Streamlit application.

## Setup and Run

### Clone from GIT
```
git clone https://github.com/danilotrix86/backend-data-intensive.git
```

### Enter the folder
```
cd .\backend-data-intensive\
```

### Step 1: Start All Services
To start all services defined in the docker-compose.yml, run the following command. This command builds and starts the containers necessary for the application, including the setup for Apache Spark, Apache Kafka, PostgreSQL, and the Streamlit app.

```
docker-compose up --build
```
This command builds the Docker images for all services if they're not already present and starts the containers. 

### Step 2: Submit Spark Job for Data Production
Once the services are running, execute the following command to start the Spark job responsible for producing data to Kafka.

```
docker-compose exec spark-master /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /apps/produce.py
```
This command runs a Spark job on the spark-master service that produces data to a Kafka topic. The --packages option includes the necessary Spark Kafka integration package, enabling Spark to publish data to Kafka.

### Step 3: Submit Spark Job for Data Consumption
When the producer starts sending data you can proceed with step 3.
To consume the data from Kafka, process it, and save it to PostgreSQL, run the following command:

```
docker-compose exec spark-master /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --jars /apps/postgresql-42.7.2.jar /apps/consume.py
```
Similar to the previous step, this command submits a Spark job for execution. However, this job consumes data from Kafka, processes it, and then saves the results to PostgreSQL. The --jars option includes the PostgreSQL JDBC driver necessary for Spark to interact with the PostgreSQL database.

## Accessing the Streamlit Application
After all services are up and the Spark jobs are running, you can access the Streamlit application by navigating to [http://localhost:8501](http://localhost:8501) in your web browser. 
The application displays data fetched from PostgreSQL, refreshing every 5 seconds to reflect new data processed by Spark.

## Architecture Overview
### Apache Spark
Used for real-time data processing. In this setup, it consumes data from Kafka, processes it, and then writes the results to PostgreSQL.
### Apache Kafka
Acts as the messaging layer, facilitating the decoupled communication between data production (Spark job) and consumption (another Spark job).
### PostgreSQL
Serves as the persistent storage for processed data, which is then visualized through the Streamlit application.
### Streamlit
Provides a web interface for visualizing the data stored in PostgreSQL, updating in real-time as new data is processed and saved.
