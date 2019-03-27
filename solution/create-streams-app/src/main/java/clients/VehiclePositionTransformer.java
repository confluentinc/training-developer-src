package clients;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

public class VehiclePositionTransformer {
    public static void main(String[] args) {
        System.out.println(">>> Starting Sample Streams Application");
        
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streams Sample App");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        
        Topology topology = getTopology();
        KafkaStreams streams = new KafkaStreams(topology, settings);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("<<< Stopping Sample Streams Application");
            streams.close();
        }));

        streams.start();
    }

    private static Topology getTopology(){
        final Serde<String> stringSerde = Serdes.String();
        final Serde<VehiclePosition> vpSerde = getJsonSerde();
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String,VehiclePosition> positions = builder
            .stream("vehicle-positions", Consumed.with(stringSerde, vpSerde));

        KStream<String,VehiclePosition> operator_47_Only = 
            positions.filter((key,value) -> value.oper == 47);

        operator_47_Only.to("vehicle-positions-oper-47", 
                            Produced.with(stringSerde, vpSerde));
        
        Topology topology = builder.build();
        return topology;
    }

    private static Serde<VehiclePosition> getJsonSerde(){
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("json.value.type", VehiclePosition.class);

        final Serializer<VehiclePosition> vpSerializer = new KafkaJsonSerializer<>();
        vpSerializer.configure(serdeProps, false);
                
        final Deserializer<VehiclePosition> vpDeserializer = new KafkaJsonDeserializer<>();
        vpDeserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(vpSerializer, vpDeserializer);
    }
}