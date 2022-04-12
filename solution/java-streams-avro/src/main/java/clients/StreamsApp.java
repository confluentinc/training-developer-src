package clients;

import clients.avro.PositionString;
import clients.avro.PositionValue;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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
    // aggregate(...) (i.e. every input event will have a corresponding output event.
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
    final Serde<PositionString> positionStringSerde = new SpecificAvroSerde<>();
    positionStringSerde.configure(serdeConfig, false);

    // Create the StreamsBuilder object to create our Topology
    final StreamsBuilder builder = new StreamsBuilder();

    // Create a KStream from the `driver-positions-avro` topic
    // configure a serdes that can read the string key, and avro value
    final KStream<String, PositionValue> positions = builder.stream(
            "driver-positions-avro",
            Consumed.with(Serdes.String(),
                    positionValueSerde));

    // TO-DO: Use filter() method to filter out the events from `driver-2`.
    //        Define the predicate in the lambda expression of the filter().
    final KStream<String, PositionValue> positionsFiltered = positions.filter(
            (key,value) -> !key.equals("driver-2"));

    // TO-DO: Use mapValues() method to change the value of each
    //        event from PositionValue to PositionString class.
    //        You can check the two schemas under src/main/avro/.
    //        Notice that position_string.avsc contains a new field
    //        `positionString` as String type.
    final KStream<String, PositionString> positionsString = positionsFiltered.mapValues(
            value -> {
              final Double latitude = value.getLatitude();
              final Double longitude = value.getLongitude();
              final String positionString = "Latitude: " + String.valueOf(latitude) +
                      ", Longitude: " + String.valueOf(longitude);
              return new PositionString(latitude, longitude, positionString);
            }
    );

    // Write the results to topic `driver-positions-string-avro`
    // configure a serdes that can write the string key, and new avro value
    positionsString.to(
            "driver-positions-string-avro",
            Produced.with(Serdes.String(), positionStringSerde));

    // Build the Topology
    final Topology topology = builder.build();
    return topology;
  }

}