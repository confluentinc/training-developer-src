echo Waiting for Kafka to be ready... 

cub kafka-ready -b kafka:9092 1 300

echo Creating the topics...

kafka-topics --bootstrap-server kafka:9092 \
    --create \
    --topic vehicle-positions \
    --partitions 6 \
    --replication-factor 1

kafka-topics --bootstrap-server kafka:9092 \
    --create \
    --topic operators \
    --partitions 1 \
    --replication-factor 1

cat << EOF | kafka-console-producer \
    --broker-list kafka:9092 \
    --topic operators \
    --property "parse.key=true" \
    --property "key.separator=,"
6,{"id": 6, "name": "Oy Pohjolan Liikenne Ab"}
12,{"id": 12, "name": "Helsingin Bussiliikenne Oy"}
17,{"id": 17, "name": "Tammelundin Liikenne Oy"}
18,{"id": 18, "name": "Pohjolan Kaupunkiliikenne Oy"}
19,{"id": 19, "name": "Etelä-Suomen Linjaliikenne Oy"}
20,{"id": 20, "name": "Bus Travel Åbergin Linja Oy"}
21,{"id": 21, "name": "Bus Travel Oy Reissu Ruoti"}
22,{"id": 22, "name": "Nobina Finland Oy"}
36,{"id": 36, "name": "Nurmijärven Linja Oy"}
40,{"id": 40, "name": "HKL-Raitioliikenne"}
45,{"id": 45, "name": "Transdev Vantaa Oy"}
47,{"id": 47, "name": "Taksikuljetus Oy"}
51,{"id": 51, "name": "Korsisaari Oy"}
54,{"id": 54, "name": "V-S Bussipalvelut Oy"}
55,{"id": 55, "name": "Transdev Helsinki Oy"}
58,{"id": 58, "name": "Koillisen Liikennepalvelut Oy"}
59,{"id": 59, "name": "Tilausliikenne Nikkanen Oy"}
90,{"id": 90, "name": "VR Oy"}
EOF
