package clients;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Subscriber implements MqttCallback {

    private final int qos = 1;
    private String topic = "/hfp/v1/journey/ongoing/+/+/+/+/+/+/+/+/+/#";
    private MqttClient client;

    public Subscriber() throws MqttException {
        // String host = "mqtts://mqtt.hsl.fi:8883";
        String host = "tcp://mqtt.hsl.fi:8883";
        String clientId = "MQTT-Java-Example";

        MqttConnectOptions conOpt = new MqttConnectOptions();
        conOpt.setCleanSession(true);

        // this.client = new MqttClient(host, clientId, new MemoryPersistence());
        // this.client.setCallback(this);
        // this.client.connect(conOpt);
        MqttClient client = new MqttClient(host, MqttClient.generateClientId());
        client.setCallback( this );
        client.connect();
        
        this.client.subscribe(this.topic, qos);
    }

    public void connectionLost(Throwable cause) {
        System.out.println("Connection lost because: " + cause);
        System.exit(1);
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void messageArrived(String topic, MqttMessage message) throws MqttException {
        System.out.println(String.format("[%s] %s", topic, new String(message.getPayload())));
    }
}
