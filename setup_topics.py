from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from topics import topics

if __name__ == '__main__':
    kafka_topics = []

    # Kafka broker address
    kafka_broker = 'localhost:9094'  # Update with your Kafka broker address

    # Create a Kafka Admin Client
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_broker,
    )

    new_topic_partitions = 1  # Number of partitions
    new_topic_replication_factor = 1  # Replication factor

    for topic in topics:
        # Specify the new topic name and configuration

        # Create a NewTopic instance

        kafka_topics.append(NewTopic(
            name=topic,
            num_partitions=new_topic_partitions,
            replication_factor=new_topic_replication_factor,
        ))

        # Create the new topic
    admin_client.create_topics(new_topics=kafka_topics, validate_only=False)

    # Close the admin client
    admin_client.close()
