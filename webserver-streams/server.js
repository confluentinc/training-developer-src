const os = require('os');
const Kafka = require('node-rdkafka');
const avro = require('avsc');
const topic = process.env.TOPIC || 'driver-positions-string-avro';

// Avro schema for driver-positions-string-avro
const typeValue = avro.Type.forSchema({
  type: 'record',
  fields: [
    {'name': 'latitude', 'type': 'double'},
    {'name': 'longitude', 'type': 'double'},
    {'name': 'positionString', 'type': 'string'},
  ],
});

console.log(`Subscribing to topic: ${topic}`);
const stream = Kafka.createReadStream({
  'group.id': `${os.hostname()}`,
  'metadata.broker.list': 'kafka:29092',
  'client.id': 'webserver-streams-consumer',
}, {'auto.offset.reset': 'earliest'}, {
  topics: [topic],
  waitInterval: 0,
});

stream.on('data', function(avroData) {
  // Decode Avro value (skip 5 bytes for magic byte + schema ID)
  const data = typeValue.decode(avroData.value, 5).value;
  
  const message = {
    'topic': avroData.topic,
    'key': avroData.key.toString(),
    'latitude': data.latitude.toFixed(6),
    'longitude': data.longitude.toFixed(6),
    'positionString': data.positionString,
    'timestamp': avroData.timestamp,
    'partition': avroData.partition,
    'offset': avroData.offset,
  };
  
  io.sockets.emit('new message', message);
});

// Setup basic express server
const express = require('express');
const app = express();
const path = require('path');
const server = require('http').createServer(app);
const io = require('socket.io')(server);
const port = process.env.PORT || 3000;

server.listen(port, () => {
  console.log('Server listening at port %d', port);
});

// Routing
app.use(express.static(path.join(__dirname, 'public')));

// log when we get a websocket connection
io.on('connection', (socket) => {
  console.log('new connection, socket.id: ' + socket.id);
});

