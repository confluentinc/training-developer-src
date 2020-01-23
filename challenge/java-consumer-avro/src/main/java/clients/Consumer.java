package clients;

import clients.avro.PositionValue;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
  static final String KAFKA_TOPIC = "driver-positions-avro";

  /**
   * Java consumer.
   */
  public static void main(String[] args) {
    System.out.println("Starting Java Avro Consumer.");

    // Configure the group id, location of the bootstrap server, default deserializers,
    // Confluent interceptors, Schema Registry location
    final Properties settings = new Properties();
    settings.put(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-avro");
    settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    settings.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    settings.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
    settings.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

    final KafkaConsumer<String, PositionValue> consumer = new KafkaConsumer<>(settings);

    try {
      // Subscribe to our topic
      consumer.subscribe(Arrays.asList(KAFKA_TOPIC));
      while (true) {
        final ConsumerRecords<String, PositionValue> records =
            consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, PositionValue> record : records) {
          System.out.printf("Key:%s Latitude:%s Longitude:%s [partition %s]\n",
              record.key(),
              record.value().getLatitude(),
              record.value().getLongitude(),
              record.partition());
        }
      }
    } finally {
      // Clean up when the application exits or errors
      System.out.println("Closing consumer.");
      consumer.close();
    }
  }
}
