from pykafka import KafkaClient

client = KafkaClient(hosts='localhost:9092')
print(client.topics)
topic = client.topics[b'Topic2']

balanced_consumer = topic.get_balanced_consumer(consumer_group=b'pykafka',
                                                auto_commit_enable=True,
                                                managed=True,
                                                )
for message in balanced_consumer:
    if message is not None:
        print(message.offset, message.value)
