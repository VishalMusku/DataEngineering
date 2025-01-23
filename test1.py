from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import json

def consume_messages_from_kafka():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkIntegration")\
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8") \
        .getOrCreate()

    # Set up logging for debugging
    spark.sparkContext.setLogLevel("INFO")

    # Read data from Kafka topic 'users_created'
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "users_created") \
        .option("startingOffsets", "earliest") \
        .load()

    # The Kafka data is in binary format, so we need to decode the message value
    messages_df = kafka_df.selectExpr("CAST(value AS STRING)")  # Cast binary data to string
    
    # Optional: Parse the JSON message (if messages are in JSON format)
    parsed_df = messages_df.select(
        expr("json_tuple(value, 'user_id', 'user_name', 'user_email') as (user_id, user_name, user_email)")
    )

    # Write the output to the console (this is for debugging)
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Await termination of the streaming query
    query.awaitTermination()

if __name__ == "__main__":
    consume_messages_from_kafka()
