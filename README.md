# Log Analytics Pipeline

This project implements a log analytics pipeline that utilizes a combination of Big Data technologies, including Kafka, Spark Streaming, Parquet files, and the HDFS file system. The pipeline aims to process and analyze log files by establishing a flow that begins with a Kafka producer, followed by a Kafka consumer that performs transformations using Spark. The transformed data is then converted into the Parquet file format and stored in HDFS.

## Files

1. `producer.py` - script to read & inject data into a Kafka topic line by line

    Arguments:

    - `--bootstrap-servers`: Kafka bootstrap servers. Default is `localhost:9092`.
    - `--topic`: Kafka topic to publish the data to. Default is `kafka_test`.
    - `--file`: Path to the log file to be injected into Kafka. Default is `40MBFile.log`.
    - `--limit`: Limit the number of log record to be injected. Default is -1, which will inject all lines.
    - `--reset`: Clean up Kafka topic before producing new messages. Default is `False`.


2. `consumer.py` - script to read & process Kafka streaming data

    Arguments:

    - `--bootstrap-servers`: Kafka bootstrap servers. Default is `localhost:9092`.
    - `--topic`: Kafka topic to listen to. Default is `kafka_test`.
    - `--reset`: Clean up previous Spark checkpoint and output before consuming new data. Default is `False`.


1. `transformation.py` - contains helper functions for wrangling Spark streaming dataframes


## Usage

1. Spin up HDFS

``` sh
hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh
```

2. Inject all lines from input file into Kafka:

```sh
python producer.py --topic log_analytics --file 40MBFile.log --limit -1
```

2. Consume the streaming data from Kafka via Spark Streaming
   
```sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 consumer.py --topic log_analytics
```
