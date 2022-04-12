"Python Avro Consumer"
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.avro.serializer import SerializerError

KAFKA_TOPIC = "driver-positions-avro"

print("Starting Python Avro Consumer.")

with open("position_value.avsc","r") as avro_file:
    value_schema = avro_file.read()

schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

avro_deserializer = AvroDeserializer(value_schema, schema_registry_client)

# Configure the group id, location of the bootstrap server,
# Confluent interceptors, and schema registry location
consumer_conf = {'bootstrap.servers': "kafka:9092",
                 'key.deserializer': StringDeserializer('utf_8'),
                 'value.deserializer': avro_deserializer,
                 'group.id': 'python-consumer-avro',
                 'plugin.library.paths': 'monitoring-interceptor',
                 'auto.offset.reset': "earliest"}

consumer = DeserializingConsumer(consumer_conf)

# Subscribe to our topic
consumer.subscribe([KAFKA_TOPIC])

try:
    while True:
        try:
            # Poll for available records
            msg = consumer.poll(1.0)
        except SerializerError as ex:
            print("Message deserialization failed for {}: {}".format(msg, ex))
            break

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print("Key:{} Value:{} [partition {}]".format(
            msg.key(),
            msg.value(),
            msg.partition()
        ))
except KeyboardInterrupt:
    pass
finally:
    # Clean up when the application exits or errors
    print("Closing consumer.")
    consumer.close()
