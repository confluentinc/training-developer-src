package clients;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import solution.model.ProductValue;

public class ProductProducer {
    public static void main(String[] args) {
        System.out.println("*** Starting prod-producer ***");

        Properties settings = new Properties();
        settings.put("client.id", "prod-producer");
        settings.put("bootstrap.servers", "<YOUR_BOOTSTRAP_SERVER>>");
        settings.put("sasl.mechanism", "PLAIN");
        settings.put("request.timeout.ms", "20000");
        settings.put("retry.backoff.ms", "500");
        settings.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<YOUR_API_KEY>\" password=\"<YOUR_API_SECRET>\";");
        settings.put("security.protocol", "SASL_SSL");

        settings.put("key.serializer", StringSerializer.class);
        settings.put("value.serializer", KafkaAvroSerializer.class);
        settings.put("schema.registry.url", "<YOUR_SR_URL>");
        settings.put("basic.auth.credentials.source", "USER_INFO");
        settings.put("schema.registry.basic.auth.user.info", "<YOUR_SR_KEY>:<YOUR_SR_SECRET>");
        
        final KafkaProducer<String, ProductValue> producer = new KafkaProducer<>(settings);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping prod-producer ###");
            producer.close();
        }));
        
        String key = "1";
        ProductValue value = new ProductValue(1, "apples", 1.25);
        final ProducerRecord<String, ProductValue> record = 
            new ProducerRecord<>("products", key, value);
        producer.send(record);
    }
}