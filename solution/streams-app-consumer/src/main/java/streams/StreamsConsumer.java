package streams;

import java.util.Properties;
import java.util.UUID;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import solution.model.ShakespeareKey;
import solution.model.ShakespeareValue;


public class StreamsConsumer
 {
    public static void main(String[] args) {
        System.out.println(">>> Starting Sample Streams Consumer Application");

        String appID = UUID.randomUUID().toString();
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");    
        
        StreamsBuilder builder = new StreamsBuilder();
        KStream<ShakespeareKey,ShakespeareValue> input = builder.stream("shakespeare_avro_topic");

        input.foreach((key,value) -> {
            System.out.printf("%s, %s: %s\n", key.work, key.year, value.line);
        });

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, settings);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("<<< Stopping Sample Streams Consumer Application");
            streams.close();
        }));
    }
}