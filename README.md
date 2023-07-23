# Scalable Streaming Log Analytics Pipeline
Log analysis is essential for business decisions, investigations, troubleshooting, and security. It is an effective approach to extract meaningful data from logs to pinpoint the root cause of errors, gain insights into performance, usage, and user behavior, and find trends and patterns for guidance.
This project implements a log analytics pipeline that utilizes a combination of Big Data technologies, including Kafka, Spark Streaming, Parquet files, and the HDFS file system. The pipeline aims to process and analyze log files by establishing a flow that begins with a Kafka producer, followed by a Kafka consumer that performs transformations using Spark. The transformed data is then converted into the Parquet file format and stored in HDFS.

# Features
- Load and parse log dataset
- Transform the parsed log data into smaller datasets for downstream analysis
- Save the log records in Hadoop Distributed File System (HDFS)
- Perform Exploratory Data Analysis (EDA) for the log data

# Methods
## Data Streaming
### Producer
The producer script serves as a data streaming component in a scalable data pipeline. Its overall purpose is to read log data from an input file and stream it to a specified Apache Kafka topic. 
The producer script performs the following key tasks:
- Creates a Kafka producer instance.
- Reads data from the specified input file.
- Streams the data, line by line, to the specified Kafka topic.
- Closes the Kafka producer once all data is streamed.

### Consumer
The consumer script serves as the consumer component in the scalable data pipeline. Its purpose is to consume streaming data from an Apache Kafka topic, perform batch processing on the received data, and save the transformed data to HDFS using Apache Spark.
The consumer script performs the following key tasks:
- Sets up a Spark session for streaming processing and reads the streaming data from the specified Kafka topic.
- Applies various transformations to each batch of streaming data received. For each transformation, it applies the transformation to the incoming data and saves the transformed data to HDFS in Parquet format.

## Data Transformation and EDA
A transformation is applied to the incoming streaming data to parse and split the log records into structured columns, making the data easier to work with for downstream analysis and exploration.
Multiple aggregation and filtration operations are applied to each batch of streaming data to fit the needs of the downstream EDA process. The aggregation operations include:
- Count the occurrence of missing values for each column
- Calculate statistics (min, max, mean, standard deviation, and total count) of the content size attribute
- Filtrate the occurrences of 404 status codes 
- Count occurrences of status code / host / endpoint / daily request 

These intermediate DataFrame outputs can then be combined together to perform further exploratory data analysis. By saving the intermediate outputs, it becomes possible to reuse the aggregated data without reprocessing the entire streaming dataset, thus improving efficiency and reducing computational overhead.

The NASA_log_analysis notebook is an example of the EDA on real-world production log data from NASA. The dataset is available on the website:
[NASA HTTP](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)

## Data Storage
The transformed data is then converted into the Parquet file format and stored in HDFS.

# Files
The following scripts in this repository are used to establish the scalable streaming pipeline, ingest and transform the log data, and load it to HDFS:

1. `producer.py` - script to read & inject data into a Kafka topic line by line

    Arguments:

    - `--bootstrap-servers`: Kafka bootstrap servers. The default is `localhost:9092`.
    - `--topic`: Kafka topic to publish the data to. The default is `kafka_test`.
    - `--file`: Path to the log file to be injected into Kafka. The default is `40MBFile.log`.
    - `--limit`: Limit the number of log records to be injected. The default is -1, which will inject all lines.
    - `--reset`: Clean up Kafka topic before producing new messages. The default is `False`.

2. `consumer.py` - script to read & process Kafka streaming data

    Arguments:

    - `--bootstrap-servers`: Kafka bootstrap servers. The default is `localhost:9092`.
    - `--topic`: Kafka topic to listen to. The default is `kafka_test`.
    - `--reset`: Clean up the previous Spark checkpoint and output before consuming new data. The default is `False`.

3. `transformation.py` - contains helper functions for wrangling Spark streaming dataframes

# How to use
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
# References:
- [Scalable Log Analytics with Apache Spark — A Comprehensive Case-Study](https://github.com/XinyiWang-Jessica/log-streaming-analytics/assets/108918930/4c241e74-8600-4dc0-8926-4c38d6c9d517)
- [NASA HTTP](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)

