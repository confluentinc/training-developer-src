package clients;

import java.io.IOException;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import clients.VehiclePosition.VehicleValues;
import solution.model.PositionKey;
import solution.model.PositionValue;

public class Subscriber implements MqttCallback {

    private final int qos = 1;
    private String host = "ssl://mqtt.hsl.fi:8883";
    private String clientId = "MQTT-Java-Example";
    private String topic = "/hfp/v2/journey/ongoing/vp/#";
    private String kafka_topic = "vehicle-positions-avro";
    private MqttClient client;

    private KafkaProducer<PositionKey, PositionValue> producer;

    public Subscriber(KafkaProducer<PositionKey, PositionValue> producer) {
        this.producer = producer;
    }

    public void start() throws MqttException {
        MqttConnectOptions conOpt = new MqttConnectOptions();
        conOpt.setCleanSession(true);

        final String uuid = UUID.randomUUID().toString().replace("-", "");
    
        String clientId = this.clientId + "-" + uuid;
        this.client = new MqttClient(this.host, clientId, new MemoryPersistence());
        this.client.setCallback(this);
        this.client.connect(conOpt);
        
        this.client.subscribe(this.topic, this.qos);
    }

    public void connectionLost(Throwable cause) {
        System.out.println("Connection lost because: " + cause);
        System.exit(1);
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void messageArrived(String topic, MqttMessage message) throws MqttException {
        try {
            System.out.println(String.format("[%s] %s", 
                    topic, new String(message.getPayload())));
            final PositionKey key = new PositionKey(topic);
            final PositionValue value = getPositionValue(message.getPayload());
            final ProducerRecord<PositionKey, PositionValue> record = 
                    new ProducerRecord<>(this.kafka_topic, key, value);
            producer.send(record);
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
    }

    private PositionValue getPositionValue(byte[] payload) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = new String(payload);
        VehiclePosition pos =  mapper.readValue(json, VehiclePosition.class);
        VehicleValues vv = pos.VP;

        return new PositionValue(vv.desi, vv.dir, vv.oper, vv.veh, vv.tst,
            vv.tsi, vv.spd, vv.hdg, vv.lat, vv.longitude, vv.acc, vv.dl,
            vv.odo, vv.drst, vv.oday, vv.jrn, vv.line, vv.start, vv.loc,
            vv.stop, vv.route, vv.occu, vv.seq);
    }
}
