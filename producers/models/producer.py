"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "broker.urls": "PLAINTEXT://localhost:9092, PLAINTEXT://localhost:9093, PLAINTEXT://localhost:9094",
            "schema.registry.url": "http://localhost:8081",
        }

        self.client  = AdminClient({"bootstrap.servers": self.broker_properties["broker.urls"]})

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer

        self.producer = AvroProducer(
        {"bootstrap.servers": self.broker_properties["broker.urls"]},
        schema_registry= CachedSchemaRegistryClient(self.broker_properties["schema.registry.url"])
        )

    def topic_exists(self):
        """Check to see if topic exists on the broker.

        Returns:
            Boolean: True for exists.
        """
        topic_metadata = self.client.list_topics(timeout=5)
        return topic_metadata.topics.get(self.topic_name) is not None

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        if not self.topic_exists():
            futures = self.client.create_topics(
                [
                    NewTopic(
                        topic = self.topic_name,
                        num_partitions=5,
                        replication_factor=1,
                        config={}
                    )
                ]
            )
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"topic {self.topic_name} created")
                except Exception as e:
                    print(f"failed to create topic {self.topic_name}: {e}")
                    raise
        else:
            logger.info(f"{self.topic_name} already exists on the broker")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        if self.producer is not None:
            self.producer.flush()


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
