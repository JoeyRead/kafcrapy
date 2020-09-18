from kafka import KafkaConsumer
from kafka import KafkaProducer

import json
import time

DEFAULT_CONSUMER = {
    'bootstrap_servers': ['localhost:9092'],
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 5 * 1000,
    'group_id': 'my_group',
    'client_id': 'my_group',
    'max_poll_records': 10,
    'max_poll_interval_ms': 60 * 1000,
    'consumer_timeout_ms': 30 * 1000,
    'session_timeout_ms': 60 * 1000,
    'request_timeout_ms': 305 * 1000,
    'heartbeat_interval_ms': 5 * 1000,
    'key_deserializer': lambda v: json.loads(v.decode('utf-8')) if v else None,
    'value_deserializer': lambda v: json.loads(v.decode('utf-8')) if v else None,
}

DEFAULT_PRODUCER = {
    'bootstrap_servers': ['localhost:9092'],
    'value_serializer': lambda x: json.dumps(x).encode('utf-8')
}


def producer_from_settings(settings, topic_name):
    producer_settings = settings.get('PRODUCER_SETTINGS', DEFAULT_PRODUCER)
    return KafkaProducer(**DEFAULT_PRODUCER)


def consumer_from_settings(topic_name, settings):
    consumer_settings = settings.get('CONSUMER_SETTINGS', DEFAULT_CONSUMER)
    consumer = KafkaConsumer(topic_name, **consumer_settings)
    return consumer


def test_producer():
    producer = producer_from_settings({}, DEFAULT_PRODUCER)
    for x in range(1, 10):
        time.sleep(.1)
        print('producing result: ', x)
        data = {'url': "https://www.thebookofjoel.com/python-kafka-consumers"}
        producer.send('my_topic', value=data)


def test_consumer():
    consumer = consumer_from_settings('my_topic', {})
    metrics = consumer.metrics()
    print(metrics)
    while True:
        message_batch = consumer.poll(timeout_ms=5000, max_records=5)
        for partition_batch in message_batch.values():
            for message in partition_batch:
                print("message: ", message.value)
        print("batch processed")
        time.sleep(1)


import sys

if __name__ == "__main__":
    run = sys.argv[1]
    if run == 'producer':
        test_producer()
    else:
        test_consumer()
