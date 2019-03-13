import requests
import json

url = "http://rest-proxy:8082/topics/hello-python-topic"

headers = {
    "Content-Type": "application/vnd.kafka.json.v2+json"
}

for i in range(1,6):
    payload = {
        "records": [
            {
                "key": "python-key-" + str(i),
                "value": "python-value-" + str(i)
            }
        ]
    }

    r = requests.post(url, data=json.dumps(payload), headers=headers)

    if r.status_code != 200:
        print("Status Code: " + str(r.status_code))
        print(r.text)