const os = require('os');
const Kafka = require('node-rdkafka');
const topics = [
  'driver-positions',
];
const stream = Kafka.createReadStream({
  'group.id': `${os.hostname()}`,
  'metadata.broker.list': 'kafka:9092',
  'plugin.library.paths': 'monitoring-interceptor',
}, {'auto.offset.reset': 'earliest'}, {
  topics: topics,
  waitInterval: 0,
});

stream.on('data', function(data) {
  const arr = data.value.toString().split(',');
  const message = {
    'topic': data.topic,
    'key': data.key.toString(),
    'latitude': parseFloat(arr[0]).toFixed(6),
    'longitude': parseFloat(arr[1]).toFixed(6),
    'timestamp': data.timestamp,
    'partition': data.partition,
    'offset': data.offset,
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
