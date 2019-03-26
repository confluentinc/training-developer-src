package clients;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;

public class VehiclePositionProducer {
    public static void main(String[] args) throws MqttException {
        System.out.println("*** Starting VP Producer ***");

        Subscriber s = new Subscriber();

        System.out.println("Enter something to end: ");
        Scanner scanner = new Scanner(System.in);
        String text = scanner.nextLine();

        // Properties settings = new Properties();
        // settings.put("client.id", "vp-producer");
        // settings.put("bootstrap.servers", "kafka:9092");
        // settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // settings.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // final KafkaProducer<String, String> producer = new KafkaProducer<>(settings);

        // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        //     System.out.println("### Stopping VP Producer ###");
        //     producer.close();
        // }));

        // final String topic = "vehicle-positions";
        // for(int i=1; i<=5; i++){
        //     final String key = "key-" + i;
        //     final String value = "value-" + i;
        //     final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        //     producer.send(record);
        // }
    }
}