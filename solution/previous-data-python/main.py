from confluent_kafka import Consumer, KafkaError, TopicPartition


def my_on_assign(consumer, parts):
    print('*** Enter my_on_assign ***')
    partitions = []
    for p in parts:
        partitions.append(TopicPartition(topic=p.topic, partition=p.partition, offset=0))
    consumer.assign(partitions)


c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'previous-data-python',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['hello-world-topic'], on_assign=my_on_assign)

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

    print('-> {}: {}'.format(msg.key(), msg.value()))

c.close()