"Python Consumer"
from confluent_kafka import Consumer

KAFKA_TOPIC = "driver-positions"

print("Starting Python Consumer.")

# Configure the group id, location of the bootstrap server,
# Confluent interceptors
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'plugin.library.paths': 'monitoring-interceptor',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
})

# Subscribe to our topic
consumer.subscribe([KAFKA_TOPIC])

try:
    while True:
        #TODO: Poll for available records
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        #TODO: print the contents of the record
        print("Key:{} Value:{} [partition {}]".format(
            msg.key().decode('utf-8'),
            msg.value().decode('utf-8'),
            msg.partition()
        ))
except KeyboardInterrupt:
    pass
finally:
    # Clean up when the application exits or errors
    print("Closing consumer.")
    consumer.close()
