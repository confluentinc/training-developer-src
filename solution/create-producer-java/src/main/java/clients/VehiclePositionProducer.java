package clients;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.*;

public class VehiclePositionProducer {
    public static void main(String[] args) throws MqttException {
        System.out.println("*** Starting VP Producer ***");

        Properties settings = new Properties();
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, "vp-producer");
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(settings);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping VP Producer ###");
            producer.close();
        }));
        
        Subscriber subscriber = new Subscriber(producer);
        subscriber.start();
    }
}