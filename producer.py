import argparse
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient


def main(kafka_bootstrap_servers, kafka_topic, input_file_path, limit):
    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    # Read data from the input text file
    sent = 0
    with open(input_file_path, "r") as file:
        # Inject data into Kafka topic line by line
        for line in file:
            if sent == limit:
                break
            producer.send(kafka_topic, line.encode("utf-8"))
            sent += 1

    # Close the Kafka producer
    producer.close()


def reset_topic(servers, topic):
    admin_client = KafkaAdminClient(bootstrap_servers=[servers])
    admin_client.delete_topics(topics=[topic])
    admin_client.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Kafka Producer Configurations")
    parser.add_argument("--bootstrap-servers",
                        default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="kafka_test",
                        help="Kafka topic name")
    parser.add_argument(
        "--file", default="40MBFile.log", help="Input file path")
    parser.add_argument("--limit", type=int, default=-1,
                        help="Limit the number of lines to inject into Kafka")
    parser.add_argument("--reset", action=argparse.BooleanOptionalAction,
                        default=False, help="Clean up Kafka topic before producing new messages.")
    args = parser.parse_args()

    if args.reset:
        reset_topic(args.bootstrap_servers, args.topic)

    main(args.bootstrap_servers, args.topic, args.file, args.limit)
