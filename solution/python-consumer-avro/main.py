"Python Avro Consumer"
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

KAFKA_TOPIC = "driver-positions-pyavro"

print("Starting Python Avro Consumer.")

# Configure the group id, location of the bootstrap server,
# Confluent interceptors, and schema registry location
consumer = AvroConsumer({
    'bootstrap.servers': 'kafka:9092',
    'plugin.library.paths': 'monitoring-interceptor',
    'group.id': 'python-consumer-avro',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://schema-registry:8081'
})

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
