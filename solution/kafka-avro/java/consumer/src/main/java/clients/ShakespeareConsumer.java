package clients;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import solution.model.ShakespeareKey;
import solution.model.ShakespeareValue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ShakespeareConsumer
 {
    public static void main(String[] args) {
        System.out.println(">>> Starting Sample Avro Consumer Application");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "shakespeare-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // Use Specific Record or else you get Avro GenericRecord.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put("schema.registry.url", "http://schema-registry:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<ShakespeareKey, ShakespeareValue> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("shakespeare_avro_topic"));

        try {
            while (true) {
                ConsumerRecords<ShakespeareKey, ShakespeareValue> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ShakespeareKey, ShakespeareValue> record : records) {
                    System.out.printf("%s, %s: %s \n", record.key().work, record.key().year, record.value().line);
                }
            }
        } finally {
            consumer.close();
        }
    }
 }