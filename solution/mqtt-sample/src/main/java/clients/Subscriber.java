package clients;

import java.util.Scanner;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

public class Subscriber implements MqttCallback {
    public static void main(String[] args) throws MqttException {

        String topic = "/hfp/v1/journey/ongoing/+/+/+/+/+/+/+/+/+/#";
        consume("ws://mqtt.hsl.fi:1883", topic);
        
        // consume("tcp://localhost:1883", "iot_data");

        // for(int i=0; i<10; i++)
        //     produce("tcp://localhost:1883", "iot_data", "Hello world from Java " + i);
    }

    private static void consume(String url, String topic) throws MqttException, MqttSecurityException {
        MqttClient client = new MqttClient(url, MqttClient.generateClientId());
        client.setCallback(new Subscriber());
        client.connect();
        client.subscribe(topic);
    }

    private static void produce(String url, String topic, String body) throws MqttException {
        MqttClient client = new MqttClient(url, MqttClient.generateClientId());
        client.connect();

        MqttMessage message = new MqttMessage();
        message.setPayload(body.getBytes());
        client.publish(topic, message);
        client.disconnect();
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("Connection to MQTT broker lost!");
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("Message received:\n\t"+ new String(message.getPayload()) );
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // not used in this example
    }
}