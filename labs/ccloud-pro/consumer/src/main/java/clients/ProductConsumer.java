package clients;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import solution.model.ProductValue;

public class ProductConsumer {
    public static void main(String[] args) {
        System.out.println("*** Starting Product Consumer Avro ***");

        Properties settings = new Properties();
        
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "prod-consumer-avro");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "<YOUR_BOOTSTRAP_SERVER>");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put("sasl.mechanism", "PLAIN");
        settings.put("request.timeout.ms", "20000");
        settings.put("retry.backoff.ms", "500");
        settings.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<YOUR_API_KEY>\" password=\"<YOUR_API_SECRET>\";");
        settings.put("security.protocol", "SASL_SSL");

        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        settings.put("schema.registry.url", "<YOUR_SR_URL>");
        settings.put("basic.auth.credentials.source", "USER_INFO");
        settings.put("schema.registry.basic.auth.user.info", "<YOUR_SR_KEY>:<YOUR_SR_SECRET>");
        settings.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        KafkaConsumer<String, ProductValue> consumer = new KafkaConsumer<>(settings);
        try {
            consumer.subscribe(Arrays.asList("products"));

            while (true) {
                ConsumerRecords<String, ProductValue> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, ProductValue> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s\n", 
                        record.offset(), record.key().toString(), record.value().toString());
            }
        }
        finally{
            System.out.println("*** Ending Product Consumer Avro ***");
            consumer.close();
        }
    }
}