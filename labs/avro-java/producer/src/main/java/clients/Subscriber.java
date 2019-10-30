package clients;

import java.io.IOException;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

// import clients.VehiclePosition.VehicleValues;
import solution.model.PositionKey;
import solution.model.PositionValue;

public class Subscriber implements MqttCallback {

    private final int qos = 1;
    private String host = "ssl://mqtt.hsl.fi:8883";
    private String clientId = "MQTT-Java-Example";
    private String topic = "/hfp/v2/journey/ongoing/vp/#";
    private String kafka_topic = "vehicle-positions";
    private MqttClient client;

    private KafkaProducer<String, String> producer;

    public Subscriber(KafkaProducer<String, String> producer) {
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
        System.out.println(String.format("[%s] %s", topic, new String(message.getPayload())));
        final String key = topic;
        final String value = new String(message.getPayload());
        final ProducerRecord<String, String> record = new ProducerRecord<>(this.kafka_topic, key, value);
        producer.send(record);
    }
}
