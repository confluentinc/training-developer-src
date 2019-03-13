package clients;

import java.time.Duration;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


public class ManualOffsetManagement {
    public static void main(String[] args) throws IOException {
        final String OFFSET_FILE_PREFIX = "/data/offset_";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // nothing to do...
			}
            
            @Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    try{
                        if (Files.exists(Paths.get(OFFSET_FILE_PREFIX + partition.partition()))) {
                            long offset = Long
                                .parseLong(Files.readAllLines(Paths.get(OFFSET_FILE_PREFIX + partition.partition()),
                                            Charset.defaultCharset()).get(0));
                            consumer.seek(partition, offset);
                        }
                    } catch(IOException e) {
                        System.out.printf("ERR: Could not read offset from file.\n");
                    }
                }
			}
		};

        try {
            consumer.subscribe(Arrays.asList("two-p-topic"), listener);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.format("Offset = %s, Key = %s, Value = %s\n", record.offset(), record.key(),
                            record.value());
                    Files.write(Paths.get(OFFSET_FILE_PREFIX + record.partition()),
                            Long.valueOf(record.offset() + 1).toString().getBytes());
                }

            }
        }
        finally{
            consumer.close();
        }
    }
}
