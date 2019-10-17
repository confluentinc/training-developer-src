const sqlite3 = require('sqlite3').verbose();
const mqtt = require('mqtt');

let dbIsClose = false;
let db = new sqlite3.Database('../db/vehicle-positions.db');
db.run('CREATE TABLE IF NOT EXISTS vehicle_positions(id INTEGER PRIMARY KEY AUTOINCREMENT, key text, value text)');

//<prefix>/<version>/<journey_type>/<temporal_type>/<event_type>/<transport_mode>/<operator_id>/<vehicle_number>/<route_id>/<direction_id>/<headsign>/<start_time>/<next_stop>/<geohash_level>/<geohash>/#
const topic = '/hfp/v2/journey/ongoing/vp/+/+/+/+/+/+/+/+/+/#';
const client  = mqtt.connect('mqtts://mqtt.hsl.fi:8883');

process.on('SIGINT', function() {
    console.log("Caught interrupt signal");
    client.end();
    db.close();
    dbIsClose = true;
});
  
client.on('connect', function () {
    client.subscribe(topic);
    console.log('Connected');
});

client.on('message', (topic, message) => {
    try {
        const vehicle_position = JSON.parse(message);
        const key = topic;
        const value = JSON.stringify(vehicle_position);
        persistValue(key, value);
    } catch (err) {
        client.end(true);
        console.error('A problem occurred when sending our message');
        console.error(err);
    }
});

let index = 0;
function persistValue(key, value){
    if(dbIsClose) return;
    db.run(`INSERT INTO vehicle_positions(key, value) VALUES(?,?)`, [key, value], function(err) {
        if (err) {
            return console.log(err.message);
        }
        index++;
        if(index%1000==0){
            // get the last insert id
            console.log(`A row has been inserted with rowid ${this.lastID}`);
        }
    });
}
