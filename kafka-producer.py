import argparse
import datetime
import random
from time import time, sleep

from kafka import KafkaProducer

from topics import topics

random_value = lambda min, max: round(random.uniform(-min, max), )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka producer that sends random data regarding to topic selected.')
    parser.add_argument('-t', '--topics', choices=topics, default=topics[0])
    args = parser.parse_args()

    # Specify the Kafka broker(s)
    kafka_broker = 'localhost:9094'

    # Create a Kafka producer instance
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: str(v).encode('utf-8'),  # Convert values to UTF-8 bytes
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,  # Key serializer
    )

    kafka_topic = args.topics
    t1 = time()
    while (time() - t1) <= 10:
        value = random_value(0, 100)
        producer.send(kafka_topic, value=value, key=datetime.datetime.utcnow())
        print(f"Sent: {value}")
        sleep(1)

    producer.close()
