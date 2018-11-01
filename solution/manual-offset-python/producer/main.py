from confluent_kafka import Producer
from time import sleep


p = Producer({'bootstrap.servers': 'kafka:9092'})

print('Writing records to topic "sample-topic-python"\nHit CTRL-c to stop...')
for x in range(1000000):
    p.produce('sample-topic-python', key=str(x), value=str(x))
    sleep(0.01) # 10 milliseconds

p.flush()