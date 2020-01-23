package clients;

import clients.avro.PositionDistance;
import clients.avro.PositionValue;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import net.sf.geographiclib.Geodesic;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class StreamsApp {

  /**
   * Our first streams app.
   */
  public static void main(String[] args) {

    System.out.println(">>> Starting the streams-app Application");

    final Properties settings = new Properties();
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app-1");
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    // Disabling caching ensures we get a complete "changelog" from the
    // aggregate(...) step above (i.e.
    // every input event will have a corresponding output event.
    // see
    // https://kafka.apache.org/23/documentation/streams/developer-guide/memory-mgmt.html#record-caches-in-the-dsl
    settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    final Topology topology = getTopology();
    // you can paste the topology into this site for a vizualization: https://zz85.github.io/kafka-streams-viz/
    System.out.println(topology.describe());
    final KafkaStreams streams = new KafkaStreams(topology, settings);
    final CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("<<< Stopping the streams-app Application");
      streams.close();
      latch.countDown();
    }));

    // don't do this in prod as it clears your state stores
    streams.cleanUp();
    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  private static Topology getTopology() {
    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                                                                    "http://schema-registry:8081");
    final Serde<PositionValue> positionValueSerde = new SpecificAvroSerde<>();
    positionValueSerde.configure(serdeConfig, false); 
    final Serde<PositionDistance> positionDistanceSerde = new SpecificAvroSerde<>();
    positionDistanceSerde.configure(serdeConfig, false); 

    final StreamsBuilder builder = new StreamsBuilder();

    // Aggregate ideas from
    // https://github.com/cloudboxlabs/blog-code/blob/master/citibikekafkastreams/src/main/java/com/cloudboxlabs/TurnoverRatio.java#L82
    // https://github.com/confluentinc/kafka-streams-examples/blob/5.3.1-post/src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java#L362

    final KStream<String, PositionValue> testing = builder.stream(
        "driver-positions-avro",
        Consumed.with(Serdes.String(),
        positionValueSerde));

    final KTable<String, PositionDistance> reduced = testing.groupByKey().aggregate(
        () -> null,
        (aggKey, newValue, aggValue) -> {
          final Double newLatitude = newValue.getLatitude();
          final Double newLongitude = newValue.getLongitude();

          // initial record - no distance to calculate
          if (aggValue == null) {
            return new PositionDistance(newLatitude, newLongitude, 0.0);
          }

          final Double aggLatitude = aggValue.getLatitude();
          final Double aggLongitude = aggValue.getLongitude();
          Double aggDistance = aggValue.getDistance();
          final Double distance = Geodesic.WGS84.Inverse(aggLatitude, aggLongitude,
              newLatitude, newLongitude).s12;
          aggDistance += distance;
          return new PositionDistance(newLatitude, newLongitude, aggDistance);
      }, Materialized.with(
          Serdes.String(),
          positionDistanceSerde)); // , Materialized.as("queryable-store-name")

    reduced.toStream().to(
        "driver-distance-avro",
        Produced.with(Serdes.String(), positionDistanceSerde));
    final Topology topology = builder.build();
    return topology;
  }

}
