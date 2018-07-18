"""
Testing producer of kafka-python package
"""

import json
import random
import time
from pprint import pprint

from kafka import KafkaProducer

producer = KafkaProducer(
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
    bootstrap_servers=['localhost:9092']
)
# print partitions for Topic2
print(producer.partitions_for('Topic2'))

# produce json messages
for i in range(4):
    # use sleep to simulate network traffic
    time.sleep(random.random())
    producer.send(topic='Topic2',
                  key=b'key1',  # use key for partitioning
                  value={'key': "python-kafka",
                         'Sample': "message"})

metrics = producer.metrics()
# produce is asynchronous
# block until all async messages are sent
producer.flush()

# print some metrics
pprint(metrics, indent=2)

"""
{ 'kafka-metrics-count': {'count': 56.0},
  'producer-metrics': { 'batch-size-avg': 127.0,
                        'batch-size-max': 127.0,
                        'bufferpool-wait-ratio': 0.0,
                        'byte-rate': 11.544587129891017,
                        'compression-rate-avg': 1.0,
                        'connection-close-rate': 0.028479103513420274,
                        'connection-count': 1.0,
                        'connection-creation-rate': 0.05695469172928955,
                        'incoming-byte-rate': 104.8852135570614,
                        'io-ratio': 4.03670118555319e-05,
                        'io-time-ns-avg': 121116.63818359375,
                        'io-wait-ratio': 0.12123259392475685,
                        'io-wait-time-ns-avg': 363745364.2758456,
                        'metadata-age': 3.00372314453125,
                        'network-io-rate': 0.3986872604562676,
                        'outgoing-byte-rate': 21.301312099136624,
                        'produce-throttle-time-avg': 0.0,
                        'produce-throttle-time-max': 0.0,
                        'record-error-rate': 0.0,
                        'record-queue-time-avg': 0.0005633036295572916,
                        'record-queue-time-max': 0.0009732246398925781,
                        'record-retry-rate': 0.0,
                        'record-send-rate': 0.09090057301537485,
                        'record-size-avg': 66.0,
                        'record-size-max': 66.0,
                        'records-per-request-avg': 1.0,
                        'request-latency-avg': 15.655653817313057,
                        'request-latency-max': 101.43041610717773,
                        'request-rate': 0.19934191166821447,
                        'request-size-avg': 106.85714285714286,
                        'request-size-max': 194.0,
                        'requests-in-flight': 0.0,
                        'response-rate': 0.19934704387495503,
                        'select-rate': 0.33328723658789056},
  'producer-node-metrics.node-1001': { 'incoming-byte-rate': 59.26527983550315,
                                       'outgoing-byte-rate': 20.13521645792955,
                                       'request-latency-avg': 
                                       17.994046211242676,
                                       'request-latency-max': 
                                       101.43041610717773,
                                       'request-rate': 0.17087683211707932,
                                       'request-size-avg': 117.83333333333333,
                                       'request-size-max': 194.0,
                                       'response-rate': 0.17136888160062885},
  'producer-node-metrics.node-bootstrap': { 'incoming-byte-rate': 
  45.79300993116741,
                                            'outgoing-byte-rate': 
                                            1.1675867906343265,
                                            'request-latency-avg': 
                                            1.6252994537353516,
                                            'request-latency-max': 
                                            1.6252994537353516,
                                            'request-rate': 
                                            0.028477444265066824,
                                            'request-size-avg': 41.0,
                                            'request-size-max': 41.0,
                                            'response-rate': 
                                            0.028478159420958927},
  'producer-topic-metrics.test': { 'byte-rate': 11.544536144670605,
                                   'compression-rate': 1.0,
                                   'record-error-rate': 0.0,
                                   'record-retry-rate': 0.0,
                                   'record-send-rate': 0.09089967733721468}}
"""
