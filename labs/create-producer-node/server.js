const mqtt = require('mqtt');

//<prefix><version>/journey/<temporal_type>/<transport_mode>/<operator_id>/<vehicle_number>/<route_id>/<direction_id>/<headsign>/<start_time>/<next_stop>/<geohash_level>/<geohash>/
const topic = '/hfp/v1/journey/ongoing/+/+/+/+/+/+/+/+/+/#';

const client  = mqtt.connect('mqtts://mqtt.hsl.fi:8883');

client.on('connect', function () {
  client.subscribe(topic);
  console.log('Connected');
});
 
let count = 0;

client.on('message', function (topic, message) {
    const vehicle_position = JSON.parse(message).VP;

    //Skip vehicles with invalid location
    if (!vehicle_position.lat || !vehicle_position.long) {
      return;
    }

    const route = vehicle_position.desi;
    //vehicles are identified with combination of operator id and vehicle id
    const vehicle = vehicle_position.oper + "/" + vehicle_position.veh;
    
    const position = vehicle_position.lat + "," + vehicle_position.long;
    const speed = vehicle_position.spd;

    console.log("Route "+route+" (vehicle "+vehicle+"): "+position+" - "+speed+"m/s");

    //Close connection after receiving 100 messages
    if (++count >= 100) {
      client.end(true);
    }
});