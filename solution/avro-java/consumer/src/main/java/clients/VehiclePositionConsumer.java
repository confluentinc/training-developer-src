package clients;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import solution.model.PositionKey;
import solution.model.PositionValue;

public class VehiclePositionConsumer {
    public static void main(String[] args) {
        System.out.println("*** Starting VP Consumer Avro ***");
        
        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "vp-consumer-avro");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        settings.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        settings.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");

        KafkaConsumer<PositionKey, PositionValue> consumer = new KafkaConsumer<>(settings);
        try {
            consumer.subscribe(Arrays.asList("vehicle-positions-avro"));

            while (true) {
                ConsumerRecords<PositionKey, PositionValue> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<PositionKey, PositionValue> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s\n", 
                        record.offset(), record.key().toString(), record.value().toString());
            }
        }
        finally{
            System.out.println("*** Ending VP Consumer Avro ***");
            consumer.close();
        }
    }
}