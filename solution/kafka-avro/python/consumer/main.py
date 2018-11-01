from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


inputTopicName = "shakespeare_avro_topic_python"
c = AvroConsumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'kafka-avro-consumer-python',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://schema-registry:8081'})

c.subscribe([inputTopicName])

while True:
    try:
        msg = c.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    print('{}: {}'.format(msg.key(), msg.value()))

c.close()