package clients;

import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
  static final String KAFKA_TOPIC = "driver-positions";

  /**
   * Java previous consumer.
   */
  public static void main(String[] args) {
    System.out.println("Starting Java Consumer.");

    // Configure the group id, location of the bootstrap server, default deserializers,
    // Confluent interceptors
    final Properties settings = new Properties();
    settings.put(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-prev");
    settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    settings.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        List.of(MonitoringConsumerInterceptor.class));

    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings);
    
    // Anonymous class implementation ConsumerRebalanceListener
    final ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // nothing to do...
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Calculate the time 5 minutes ago
        final long startTimestamp = Instant.now().minusSeconds(5 * 60).toEpochMilli();

        // Build a map key=partition value=startTimestamp
        final Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition partition : partitions) {
          timestampsToSearch.put(partition, startTimestamp);
        }

        // TODO: Request the offsets for the start timestamp
        final Map<TopicPartition, OffsetAndTimestamp> startOffsets =
            consumer.offsetsForTimes(timestampsToSearch);
        // Seek each partition to the new offset
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : startOffsets.entrySet()) {
          if (entry.getValue() != null) {
            // TODO: Print the new offset for each partition
            System.out.printf("Seeking partition %d to offset %d\n", entry.getKey().partition(),
                entry.getValue().offset());
            consumer.seek(entry.getKey(), entry.getValue().offset());
          }
        }
      }
    };

    try {
      // Pass in the ConsumerRebalanceListener implementation when you subscribe
      consumer.subscribe(Arrays.asList(KAFKA_TOPIC), listener);
      int recordCount = 0;
      while (true) {
        // Poll for available records
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("%s,%s\n", record.key(), record.value());
          recordCount++;
          // Exit processing after 100 records
          if (recordCount >= 100) {
            break;
          }
        }
        if (recordCount >= 100) {
          break;
        }
      }
    } finally {
      // Clean up when the application exits or errors
      System.out.println("Closing consumer.");
      consumer.close();
    }
  }
}
