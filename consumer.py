import os
import shutil 
import argparse
from pyspark.sql import SparkSession
from transformation import *


def batch_processing(df, id):
    """Apply transformation on each batch of streaming data, and save to disk."""

    # First operation: count # of 404 requests by weekday
    df_404 = weekday_404_count(df)
    df_404.coalesce(1).write.format('csv') \
        .option("header", True) \
        .mode("append") \
        .save("output/weekday_404_count", header=True)


def main(kafka_bootstrap_servers, kafka_topic):
    # Set up Spark session
    spark = SparkSession.builder.appName("KafkaSparkStreaming")\
        .config("spark.sql.streaming.checkpointLocation", "checkpoint")\
        .getOrCreate()

    # Read streaming data from Kafka topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Preprocess incoming data
    df = proprocess_streaming_input(df)

    # Start the streaming query and write the data to a text file
    query = df.coalesce(1) \
        .writeStream \
        .option("checkpointLocation", "checkpoint") \
        .trigger(processingTime='10 seconds') \
        .foreachBatch(batch_processing) \
        .start()

    # Wait for the query to terminate
    query.awaitTermination()


def reset_checkpoint_output():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    checkpoint_dir = os.path.join(script_dir, 'checkpoint')

    # Remove the output dir if it exists
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    # Remove the checkpoint dir if it exists
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Spark Streaming")
    parser.add_argument("--bootstrap-servers", type=str,
                        default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str,
                        default="kafka_test", help="Kafka topic name")
    parser.add_argument("--reset", action=argparse.BooleanOptionalAction,
                        default=False, help="Clean up Spark checkpoint and output before consuming new data.")
    args = parser.parse_args()

    if args.reset:
        reset_checkpoint_output()

    main(args.bootstrap_servers, args.topic)
