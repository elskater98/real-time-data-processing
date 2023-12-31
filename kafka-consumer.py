import argparse

from kafka import KafkaConsumer

from topics import topics

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Kafka producer that sends random data regarding to topic selected.')
    parser.add_argument('-t', '--topics', choices=topics, default=topics[0])
    args = parser.parse_args()

    # Specify the Kafka broker(s)
    kafka_broker = 'localhost:9094'

    # Create a Kafka producer instance
    consumer = KafkaConsumer(args.topics,
                             bootstrap_servers=kafka_broker,
                             )

    for message in consumer:
        # Access the key, value, and other message information
        key = message.key
        value = message.value
        partition = message.partition
        offset = message.offset

        print(f"Received message: Key={key}, Value={value}, Partition={partition}, Offset={offset}")

    consumer.close()
