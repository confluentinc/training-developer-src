import requests
import json
import sys
import time
import signal

CONSUMER_GROUP_ID = "my_consumer_group"
INSTANCE_ID = "my_consumer_instance"

def signal_handler(signal, frame):
    print('Ctrl+C pressed.')
    delete_consumer()

def delete_consumer():
    print('Deleting consumer instance ' + INSTANCE_ID)
    uri = "http://rest-proxy:8082/consumers/{0}/instances/{1}".format(CONSUMER_GROUP_ID, INSTANCE_ID)

    headers = {
        "Accept" : "application/vnd.kafka.v2+json"
    }

    r = requests.delete(uri, headers=headers)

    if r.status_code != 204:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        sys.exit("Error thrown while getting message")

    sys.exit(0)

# Register the Ctrl+C handler
signal.signal(signal.SIGINT, signal_handler)

print("Creating the consumer instance")

headers = {
    "Content-Type": "application/vnd.kafka.v2+json"
}

url = "http://rest-proxy:8082/consumers/{0}".format(CONSUMER_GROUP_ID)

payload = {
    "name": INSTANCE_ID, 
    "format": "json", 
    "auto.offset.reset": "earliest"
}

r = requests.post(url, data=json.dumps(payload), headers=headers)

if r.status_code != 200:
	print("Status Code: " + str(r.status_code))
	print(r.text)
	sys.exit("Error thrown while creating consumer")

print("Base URI: " + r.json()["base_uri"])

print("Subscribing the consumer to the topic 'hello_world_topic'")
url = "http://rest-proxy:8082/consumers/{0}/instances/{1}/subscription".format(CONSUMER_GROUP_ID, INSTANCE_ID)

payload = {
    "topics": ["hello_world_topic"]
}

r = requests.post(url, data=json.dumps(payload), headers=headers)

if r.status_code != 204:
    print("Status Code: " + str(r.status_code))
    print(r.text)
    delete_consumer()
    sys.exit("Error thrown while subscribing the consumer to the topic")

url = "http://rest-proxy:8082/consumers/{0}/instances/{1}/records".format(CONSUMER_GROUP_ID, INSTANCE_ID)


headers = {
    "Accept": "application/vnd.kafka.json.v2+json"
}

while True:
    r = requests.get(url, headers=headers, timeout=20)

    if r.status_code != 200:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        delete_consumer()
        sys.exit("Error thrown while getting message")

    for message in r.json():
        print(message)

    time.sleep(1)
