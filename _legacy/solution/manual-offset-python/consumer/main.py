from confluent_kafka import Consumer, KafkaError, TopicPartition
from os import path

OFFSET_FILE = "topic-offset.txt"
topic_name = 'sample-topic-python'

def my_on_assign(consumer, parts):
    print('*** Enter my_on_assign ***')
    offset = 0
    if path.isfile(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as myfile:
            offset=int(myfile.read()) + 1   
    partitions = []
    # only care about a single partition in this sample!
    partitions.append(TopicPartition(topic=topic_name, partition=0, offset=offset))
    consumer.assign(partitions)
    print('>>> Start reading from offset = ' + str(offset))


c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'manual-offset-client-python-2',
    'auto.offset.reset': 'earliest'
})

c.subscribe([topic_name], on_assign=my_on_assign)

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

    print('-> {}: {} (p={}, o={})'.format(msg.key(), msg.value(), msg.partition(), msg.offset()))
    with open(OFFSET_FILE, "w") as text_file:
        text_file.write(str(msg.offset()))
        text_file.flush()

c.close()