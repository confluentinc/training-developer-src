package clients;

import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Subscriber implements MqttCallback {
    private KafkaProducer<String, String> producer;

    private final int qos = 1;
    private String host = "ssl://mqtt.hsl.fi:8883";
    private String clientId = "MQTT-Java-Example";
    private String topic = "/hfp/v2/journey/ongoing/vp/#";
    private String kafka_topic = "vehicle-positions";
    private MqttClient client;

    public Subscriber(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }


    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("Connection lost because: " + cause);
        System.exit(1);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println(String.format("[%s] %s", topic, new String(message.
                getPayload())));
        final String key = topic;
        final String value = new String(message.getPayload());
        final ProducerRecord<String, String> record =
                new ProducerRecord<>(this.kafka_topic, key, value);
        producer.send(record);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {

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
}
