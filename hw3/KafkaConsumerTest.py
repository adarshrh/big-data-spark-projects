import sys
import re
import string

from kafka import KafkaConsumer
from pyspark import SparkContext
#from elasticsearch import Elasticsearch

if __name__ == "__main__":


    kafka_server = "localhost:9092"
    kafka_topic = "test-topic"

    spark = SparkContext()
    kafkaConsumer = None


    try:
        # Load models

        # Start kafka consumer
        kafkaConsumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_server],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            api_version=(0, 10),
            consumer_timeout_ms=500,
            value_deserializer=lambda x: x.decode('utf-8')
        )

        # Connect to the elastic cluster
        #es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

        # Frame test data and evaluate model
        counter = 0
        for message in kafkaConsumer:
           print(message)

    finally:
        kafkaConsumer.close()
        spark.stop()

