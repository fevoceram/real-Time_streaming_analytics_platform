from kafka import KafkaConsumer
import json

def consume_kafka_messages(topic, bootstrap_servers="localhost:9092"):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        group_id='spark-streaming-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        yield message.value
