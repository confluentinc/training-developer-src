from confluent_kafka import Consumer, KafkaError

print("Starting VP-Consumer (Python)")

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'vp-consumer-group-python',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['vehicle-positions'])
try:
    while True:
        msg = c.poll(1.0)

        if not msg:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))
except:
    print("An exception occurred")
finally:
    c.close()
