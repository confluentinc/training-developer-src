from confluent_kafka import Consumer, KafkaError

print("Starting VP-Consumer (Python)")

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'vp-consumer-group-python',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['vehicle-positions'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
