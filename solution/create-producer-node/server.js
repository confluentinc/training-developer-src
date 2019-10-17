const mqtt = require('mqtt');
const Kafka = require('node-rdkafka');

//<prefix>/<version>/<journey_type>/<temporal_type>/<event_type>/<transport_mode>/<operator_id>/<vehicle_number>/<route_id>/<direction_id>/<headsign>/<start_time>/<next_stop>/<geohash_level>/<geohash>/#
const topic = '/hfp/v2/journey/ongoing/vp/+/+/+/+/+/+/+/+/+/#';

const client  = mqtt.connect('mqtts://mqtt.hsl.fi:8883');
const producer = new Kafka.Producer({
    'client.id': 'vp-producer',
    'metadata.broker.list': 'kafka:9092',
    'dr_cb': true
});
  
client.on('connect', function () {
    client.subscribe(topic);
    console.log('Connected');
});

producer.setPollInterval(100);
producer.connect();

producer.on('ready', () => {
    client.on('message', (topic, message) => {
        try {
            const vehicle_position = JSON.parse(message);
            const key = topic;
            const value = JSON.stringify(vehicle_position);
            producer.produce(
                'vehicle-positions',
                null,
                Buffer.from(value),
                key,
                Date.now()
            );
        } catch (err) {
            client.end(true);
            console.error('A problem occurred when sending our message');
            console.error(err);
        }
    });
});

producer.on('delivery-report', (err, report) => {
  // Report of delivery statistics here:
  console.log(report);
});

producer.on('event.error', err => {
    console.error('Error from producer');
    console.error(err);
});
