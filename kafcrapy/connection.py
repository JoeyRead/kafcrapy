from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import time

DEFAULT_CONSUMER = {
    'bootstrap_servers': ['localhost:9092'],
    'auto_offset_reset': 'earliest',
    # 'value_deserializer': lambda v: der(v),
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 100,
    'group_id': 'my_group'
}


def der(v):
    print("value: ", v)
    return json.loads(v.decode('utf-8'))


DEFAULT_PRODUCER = {
    'bootstrap_servers': ['localhost:9092'],
    'value_serializer': lambda x: json.dumps(x).encode('utf-8')
}


def producer_from_settings(settings, topic_name):
    producer_settings = settings.get('PRODUCER_SETTINGS', DEFAULT_PRODUCER)
    return KafkaProducer(**DEFAULT_PRODUCER)


def consumer_from_settings(settings, topic_name):
    consumer_settings = settings.get('CONSUMER_SETTINGS', DEFAULT_CONSUMER)
    consumer = KafkaConsumer(topic_name, **consumer_settings)
    return consumer


def test_producer():
    producer = producer_from_settings({}, DEFAULT_PRODUCER)
    for x in range(1010, 1020):
        print('producing result: ', x)
        data = {'number': x}
        producer.send('my_topic', value=data)


def test_consumer():
    consumer = consumer_from_settings({}, 'my_topic')
    for message in consumer:
        time.sleep(.01)
        print("message: ", message.value)


import sys

if __name__ == "__main__":
    run = sys.argv[1]
    if run == 'producer':
        test_producer()
    else:
        test_consumer()
