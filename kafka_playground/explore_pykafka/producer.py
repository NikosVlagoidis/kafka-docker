import json

from pykafka import KafkaClient

client = KafkaClient(hosts='localhost:9092',
                     zookeeper_hosts='localhost:2181')

topic = client.topics[b'Topic2']

with topic.get_producer(delivery_reports=True) as producer:
    for i in range(4):
        producer.produce(
            message=bytes(
                json.dumps({"key": "pykafka",
                            "Sample": "message"}).encode('ascii')),
            partition_key=b'key1')
