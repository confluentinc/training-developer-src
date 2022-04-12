"Python Avro Producer"

from time import sleep
import os
import atexit

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

DRIVER_FILE_PREFIX = "./drivers/"
KAFKA_TOPIC = "driver-positions-avro"
# Load a driver id from an environment variable
# if it isn't present use "driver-3"
DRIVER_ID = os.getenv("DRIVER_ID", "driver-3")

print("Starting Python Avro producer.")

with open("position_value.avsc","r") as avro_file:
    value_schema = avro_file.read()

# Configure the location of the bootstrap server, Confluent interceptors
# and a partitioner compatible with Java, and key/value schemas
# see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

avro_serializer = AvroSerializer(value_schema, schema_registry_client)

producer_conf = {'bootstrap.servers': 'kafka:9092',
                 'key.serializer': StringSerializer('utf_8'),
                 'value.serializer': avro_serializer,
                 'plugin.library.paths': 'monitoring-interceptor',
                 'partitioner': 'murmur2_random'}

producer = SerializingProducer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed {}: {}".format(msg.key(), err))
        return
    print("Sent Key:{} Value:{}".format(key, value))


def exit_handler():
    """Run this on exit"""
    print("Flushing producer and exiting.")
    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()

atexit.register(exit_handler)

with open(os.path.join(DRIVER_FILE_PREFIX, DRIVER_ID + ".csv")) as f:
    lines = f.readlines()

pos = 0
# Loop forever over the driver CSV file..
while True:
    line = lines[pos]
    # Trigger any available delivery report callbacks from previous produce() calls
    producer.poll(0)
    key = DRIVER_ID
    latitude = line.split(",")[0].strip()
    longitude = line.split(",")[1].strip()
    value = {"latitude" : float(latitude), "longitude" : float(longitude)}
    # ..and write the lat/long position to a Kafka topic
    producer.produce(
        topic=KAFKA_TOPIC,
        value=value,
        key=key,
        on_delivery=delivery_report
        )
    sleep(1)
    pos = (pos + 1) % len(lines)

# Confirm the topic is being written to with kafka-avro-console-consumer
#
# kafka-avro-console-consumer --bootstrap-server kafka:9092 \
#  --property schema.registry.url=http://schema-registry:8081 \
#  --topic driver-positions-pyavro --property print.key=true \
#  --from-beginning
