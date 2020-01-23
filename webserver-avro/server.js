const os = require('os');
const Kafka = require('node-rdkafka');
const avro = require('avsc');
const topic = process.env.TOPIC || 'driver-positions-avro';
const schemas = {
  'driver-positions-avro': {
    type: 'record',
    fields: [
      {'name': 'latitude', 'type': 'double'},
      {'name': 'longitude', 'type': 'double'},
    ],
  },
  'driver-positions-pyavro': {
    type: 'record',
    fields: [
      {'name': 'latitude', 'type': 'double'},
      {'name': 'longitude', 'type': 'double'},
    ],
  },
  'driver-distance-avro': {
    type: 'record',
    fields: [
      {'name': 'latitude', 'type': 'double'},
      {'name': 'longitude', 'type': 'double'},
      {'name': 'distance', 'type': 'double'},
    ],
  },
  'driver-augmented-avro': {
    type: 'record',
    fields: [
      {'name': 'LATITUDE', 'type': ['null', 'double'], 'default': null},
      {'name': 'LONGITUDE', 'type': ['null', 'double'], 'default': null},
      {'name': 'FIRSTNAME', 'type': ['null', 'string'], 'default': null},
      {'name': 'LASTNAME', 'type': ['null', 'string'], 'default': null},
      {'name': 'MAKE', 'type': ['null', 'string'], 'default': null},
      {'name': 'MODEL', 'type': ['null', 'string'], 'default': null},
    ],
  },
};

// only the python topic has an avro key
const typeKey = avro.Type.forSchema({type: 'record',
  fields: [{'name': 'key', 'type': 'string'}]});
const typeValue = avro.Type.forSchema(schemas[topic]);

console.log(`subscribing to ${topic}`);
const stream = Kafka.createReadStream({
  'group.id': `${os.hostname()}`,
  'metadata.broker.list': 'kafka:9092',
  'plugin.library.paths': 'monitoring-interceptor',
}, {'auto.offset.reset': 'earliest'}, {
  topics: [topic],
  waitInterval: 0,
});

stream.on('data', function(avroData) {
  const data = typeValue.decode(avroData.value, 5).value;
  const message = {
    'topic': avroData.topic,
    'key': avroData.key.toString(),
    'timestamp': avroData.timestamp,
    'partition': avroData.partition,
    'offset': avroData.offset,
  };
  if (topic=='driver-positions-pyavro') {
    const avroKey = typeKey.decode(avroData.key, 5).value;
    message['key'] = avroKey.key;
  }

  if (data.latitude) message['latitude'] = data.latitude;
  if (data.longitude) message['longitude'] = data.longitude;
  if (data.LATITUDE) message['latitude'] = data.LATITUDE;
  if (data.LONGITUDE) message['longitude'] = data.LONGITUDE;
  if (data.distance) message['distance'] = Math.round(data.distance);

  // different format for ksql avro stream
  if (avroData.topic == 'driver-augmented-avro') {
    message['firstname'] = data.FIRSTNAME ?
       data.FIRSTNAME : null;
    message['lastname'] = data.LASTNAME ?
       data.LASTNAME : null;
    message['make'] = data.MAKE ?
        data.MAKE : null;
    message['model'] = data.MODEL ?
        data.MODEL : null;
  }

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
