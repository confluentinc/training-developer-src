const SQL = require('sql-template-strings');
const sqlite = require('sqlite');
const { Kafka } = require('kafkajs')


const kafka = new Kafka({
  clientId: 'vp-producer-fallback',
  brokers: ['kafka:9092']
})

const producer = kafka.producer();
const run = async () => {
    await producer.connect()
    await sendMessages();
}

async function sendMessages(){
    const db = await sqlite.open('../db/vehicle-positions.db');
    
    let lastId = 0;
    let currentId = 0;
    
    while(true){
        const data = await db.all(SQL`SELECT id,key,value FROM vehicle_positions WHERE id>${lastId} LIMIT 100`)
        data.forEach(row => {
            produceData(row.key, row.value);
            currentId = row.id;
        })
        if(lastId == currentId){
            console.log('Reached end of DB. Starting over...');
            lastId = 0;
        } else {
            lastId = currentId;
            await sleep(1000);
        }
    }
}

function sleep(ms){
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

function produceData(key,value){
    producer.send({
        topic: 'vehicle-positions',
        messages: [{
            key: key,
            value: value,
            timestamp: Date.now()
        }]
    });
}

run().catch(e => console.error(`[producer] ${e.message}`, e));