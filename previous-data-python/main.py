import requests
import json
import sys
import time
import signal

CONSUMER_GROUP_ID = "demo_group_2"
INSTANCE_ID = "my_consumer_instance"

def signal_handler(signal, frame):
    print('Ctrl+C pressed.')
    delete_consumer(ignore_failure = False)

def delete_consumer(ignore_failure):
    print('Deleting consumer instance ' + INSTANCE_ID)
    uri = "http://rest-proxy:8082/consumers/{0}/instances/{1}".format(CONSUMER_GROUP_ID, INSTANCE_ID)

    headers = {
        "Accept" : "application/vnd.kafka.v2+json"
    }

    r = requests.delete(uri, headers=headers)

    if ignore_failure and r.status_code == 404:
        return

    if r.status_code != 204:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        sys.exit("Error thrown while getting message")

    sys.exit(0)

def create_consumer():
    delete_consumer(ignore_failure=True)
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

def subscribe_consumer():
    url = "http://rest-proxy:8082/consumers/{0}/instances/{1}/subscription".format(CONSUMER_GROUP_ID, INSTANCE_ID)
    headers = {
        "Content-Type": "application/vnd.kafka.v2+json"
    }
    payload = {
        "topics": ["hello_world_topic"]
    }

    r = requests.post(url, data=json.dumps(payload), headers=headers)

    if r.status_code != 204:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        delete_consumer(ignore_failure=True)
        sys.exit("Error thrown while subscribing the consumer to the topic")


def poll(timeout):
    poll_url = "http://rest-proxy:8082/consumers/{0}/instances/{1}/records".format(CONSUMER_GROUP_ID, INSTANCE_ID)
    poll_headers = {
        "Accept": "application/vnd.kafka.json.v2+json"
    }
    r = requests.get(poll_url, headers=poll_headers, timeout=timeout)
    return r

def reset_offset_to_zero():
    poll(100)
    offset_url = "http://rest-proxy:8082/consumers/{0}/instances/{1}/offsets".format(CONSUMER_GROUP_ID, INSTANCE_ID)
    headers = {
        "Content-Type": "application/vnd.kafka.v2+json"
    }
    offset_payload = {
        "offsets": [
            {
                "topic": "hello_world_topic",
                "partition": 0,
                "offset": 0
            }
        ]
    }

    r = requests.post(offset_url, data=json.dumps(offset_payload), headers=headers)

    if r.status_code != 200:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        delete_consumer(ignore_failure=True)
        sys.exit("Error thrown while setting offset of topic to 0")


#======================================================
# Register the Ctrl+C handler
signal.signal(signal.SIGINT, signal_handler)

print("Creating the consumer instance")
create_consumer()

print("Subscribing the consumer to the topic 'hello_world_topic'")
subscribe_consumer()

print("Resetting the offset to 0")
#reset_offset_to_zero()

print("Getting the data...")
while True:
    r = poll(20)

    if r.status_code != 200:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        delete_consumer(ignore_failure=True)
        sys.exit("Error thrown while getting message")

    for message in r.json():
        print(message)

    time.sleep(1)
