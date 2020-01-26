package clients;

import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
  static final String DRIVER_FILE_PREFIX = "./drivers/";
  static final String KAFKA_TOPIC = "driver-positions";

  /**
   * Java producer.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    System.out.println("Starting Java producer.");

    // Load a driver id from an environment variable
    // if it isn't present use "driver-1"
    String driverId  = System.getenv("DRIVER_ID");
    driverId = (driverId != null) ? driverId : "driver-1";

    // Configure the location of the bootstrap server, default serializers,
    // Confluent interceptors
    final Properties settings = new Properties();
    settings.put(ProducerConfig.CLIENT_ID_CONFIG, driverId);
    // TODO: configure the location of the bootstrap server
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    settings.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        List.of(MonitoringProducerInterceptor.class));

    final KafkaProducer<String, String> producer = new KafkaProducer<>(settings);
    
    // Adding a shutdown hook to clean up when the application exits
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Closing producer.");
      producer.close();
    }));

    int pos = 0;
    final String[] rows = Files.readAllLines(Paths.get(DRIVER_FILE_PREFIX + driverId + ".csv"),
      Charset.forName("UTF-8")).toArray(new String[0]);

    // Loop forever over the driver CSV file..
    while (true) {
      final String key = driverId;
      final String value = rows[pos];
      // TODO: populate the message object
      final ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, key, value);
      // TODO: write the lat/long position to a Kafka topic
      // TODO: print the key and value in the callback lambda
      producer.send(record, (md, e) -> {
        System.out.printf("Sent Key:%s Value:%s\n", key, value);
      });
      Thread.sleep(1000);
      pos = (pos + 1) % rows.length;
    }
  }
}
