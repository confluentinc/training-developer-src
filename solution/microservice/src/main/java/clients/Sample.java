package clients;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;

public class Sample {
    public static void main(String[] args){
        System.out.println("*** Starting Microservice ***");
        
        KafkaConsumer<String, JsonNode> consumer = getConsumer();
        KafkaProducer<String, DoorStatusChanged> producer = getProducer();

        try {
            consumer.subscribe(Arrays.asList("TRAM_DOOR_STATUS"));

            HashMap<String, Integer> cache = new HashMap<>();
            while (true) {
                ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, JsonNode> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(),
                            record.value());
                    JsonNode node = record.value();
                    String operator = node.get("OPER").asText();
                    String designation = node.get("DESI").asText();
                    String vehicleNo = node.get("VEH").asText();
                    int doorStatus = node.get("DRST").asInt();
                    // assume combination of operator and vehicleNo is unique
                    String key = operator + "|" + vehicleNo;
                    boolean hasChanged = true;
                    if (cache.containsKey(key)) {
                        int prevDoorStatus = cache.get(key);
                        hasChanged = prevDoorStatus != doorStatus;
                    }
                    if (hasChanged) {
                        publishEvent(producer, operator, designation, vehicleNo, doorStatus);
                    }
                    cache.put(key, doorStatus);
                }
            }
        } finally {
            System.out.println("*** Ending Microservice ***");
            consumer.close();
        }
    }

    private static KafkaConsumer<String, JsonNode> getConsumer() {
        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "tram-door-status");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(settings);
        return consumer;
    }

    private static KafkaProducer<String, DoorStatusChanged> getProducer(){
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new KafkaProducer<>(settings);
    }

    private static void publishEvent(KafkaProducer<String,DoorStatusChanged> producer, String operator, String designation, String vehicleNo, int doorStatus){
        String type = doorStatus == 0 ? "DOOR_CLOSED" : "DOOR_OPENED";
        DoorStatusChanged event = new DoorStatusChanged(operator, designation, vehicleNo, type);
        ProducerRecord<String, DoorStatusChanged> record = new ProducerRecord<>("tram-door-status-changed",operator,event);
        producer.send(record);
    }
}