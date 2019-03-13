from confluent_kafka import Producer
from os import listdir, path
from os.path import isfile, join

INPUT_PATH_NAME = "/datasets/shakespeare"

if __name__ == '__main__':

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


    onlyfiles = [f for f in listdir(INPUT_PATH_NAME) if isfile(join(INPUT_PATH_NAME, f))]
    p = Producer({'bootstrap.servers': 'kafka:9092'})
    for filename in onlyfiles:
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        key = path.splitext(filename)[0]
        print("Working on file: " + filename)

        with open(INPUT_PATH_NAME + "/" + filename) as f:
            lines = f.readlines()

        for line in lines:
            #print(key + ": " + line.rstrip())

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            p.produce('shakespeare_topic_python', key=key, value=line, callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()