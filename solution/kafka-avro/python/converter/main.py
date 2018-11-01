from confluent_kafka import Consumer, KafkaError, avro
from confluent_kafka.avro import AvroProducer
import re

key_schema_str = """
{"namespace": "app",
 "type": "record",
 "name": "ShakespeareKey",
 "fields": [
     {"name": "work", "type": "string", "doc" : "The name of the work"},
     {"name": "year", "type": "int", "doc" : "The year the work was published"}
 ]
}
"""

value_schema_str = """
{"namespace": "app",
 "type": "record",
 "name": "ShakespeareValue",
 "fields": [
     {"name": "line_number", "type": "int", "doc" : "The line number for line"},
     {"name": "line", "type": "string", "doc" : "The line from Shakespeare"}
 ]
}
"""

inputTopicName = "shakespeare_topic_python"
outputTopicName = "shakespeare_avro_topic_python"

key_schema = avro.loads(key_schema_str)
value_schema = avro.loads(value_schema_str)

avroProducer = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'kafka-consumer-python',
    'auto.offset.reset': 'earliest'
})

shakespeareWorkToYearWritten = {
    'Hamlet': 1600,
    'Julius Caesar': 1599,
    'Macbeth': 1605,
    'Merchant of Venice': 1596,
    'Othello': 1604,
    'Romeo and Juliet': 1594
}

def get_shakespeare_key(key):
    return {
      'work': key,
      'year': int(shakespeareWorkToYearWritten[key])
    }


def get_shakespeare_value(value):
    m = re.search(r'^\s*(\d*)\s*(.*)$', value)
    return {
      'line_number': int(m.group(1)),
      'line': m.group(2)
    }


c.subscribe([inputTopicName])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    print('Received message: {}: {}'.format(msg.key(), msg.value()))

    key = get_shakespeare_key(msg.key().decode('utf-8'))
    value = get_shakespeare_value(msg.value().decode('utf-8'))

    avroProducer.produce(topic=outputTopicName, value=value, key=key)

avroProducer.flush()

c.close()