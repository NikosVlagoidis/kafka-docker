import json

from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(),
                                                    msg.partition()))


for data in range(4):
    # Trigger any available delivery report callbacks from previous produce(
    # ) calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the
    # message has
    # been successfully delivered or failed permanently.
    p.produce('Topic2', bytes(
        json.dumps({"key": "confluent",
                    "Sample": "message"}).encode('ascii')),
              callback=delivery_report, key='key1')

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
