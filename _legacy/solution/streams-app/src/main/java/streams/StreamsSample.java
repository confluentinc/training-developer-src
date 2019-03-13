package streams;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;


public class StreamsSample {
    public static void main(String[] args) {
        System.out.println(">>> Starting Sample Streams Application");

        String appID = UUID.randomUUID().toString();
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

        final Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> lines = builder
            .stream("shakespeare_topic", Consumed.with(stringSerde, stringSerde));

        lines.mapValues(value -> value.toUpperCase())
            .to("shakespeare_upper_topic", Produced.with(stringSerde, stringSerde));

        lines.filter((key,value) -> key.contains("Macbeth"))
            .to("shakespeare_macbeth_topic", Produced.with(stringSerde, stringSerde));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, settings);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("<<< Stopping Sample Streams Application");
            streams.close();
        }));
    }
}
