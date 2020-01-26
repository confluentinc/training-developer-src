"Python Consumer"
import time
from confluent_kafka import Consumer

KAFKA_TOPIC = "driver-positions"

print("Starting Python Consumer.")

# Configure the group id, location of the bootstrap server,
# Confluent interceptors
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'plugin.library.paths': 'monitoring-interceptor',
    'group.id': 'python-consumer-prev',
    'auto.offset.reset': 'earliest'
})

def my_on_assign(konsumer, partitions):
    "On partition assignment move the offsets to 5 min ago"
    # Calculate the time 5 minutes ago
    timestamp = (time.time() - (5 * 60)) * 1000

    # Create topic+partitions with timestamps in the TopicPartition.offset field
    for part in partitions:
        part.offset = timestamp

    #TODO: Request the offsets for the start timestamp
    new_offsets = konsumer.offsets_for_times(partitions)
    for part in new_offsets:
        #TODO: Print the new offset for each partition
        print("Setting partition {} to offset {}".format(part.partition, part.offset))
    # Assign partitions with the new offset
    konsumer.assign(new_offsets)

# Subscribe to our topic, pass in the partition assignment implementation
consumer.subscribe([KAFKA_TOPIC], on_assign=my_on_assign)
record_count = 0

try:
    while True:
        # Poll for available records
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print("{},{}".format(
            msg.key().decode('utf-8'),
            msg.value().decode('utf-8')
        ))
        record_count += 1
        # Exit processing after 100 records
        if record_count >= 100:
            break
except KeyboardInterrupt:
    pass
finally:
    # Clean up when the application exits or errors
    print("Closing consumer.")
    consumer.close()
