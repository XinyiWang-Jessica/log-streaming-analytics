import argparse
from kafka import KafkaProducer

def main(kafka_bootstrap_servers, kafka_topic, input_file_path, limit):
    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    # Read data from the input text file
    with open(input_file_path, "r") as file:
        lines = file.readlines()

    # Inject data into Kafka topic line by line
    if limit == -1: 
        for line in lines:
            producer.send(kafka_topic, line.encode("utf-8"))
    else:
        for line in lines[:limit]:
            producer.send(kafka_topic, line.encode("utf-8"))

    # Close the Kafka producer
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer Configurations")
    parser.add_argument("--bootstrap-servers",
                        default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="kafka_test", help="Kafka topic name")
    parser.add_argument(
        "--file", default="40MBFile.log", help="Input file path")
    parser.add_argument("--limit", type=int, default=-1, help="Limit the number of lines to inject into Kafka")
    args = parser.parse_args()

    main(args.bootstrap_servers, args.topic, args.file, args.limit)
