package clients;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import solution.model.ShakespeareKey;
import solution.model.ShakespeareValue;

public class ShakespeareConverter {
    Pattern pattern = Pattern.compile("^\\s*(\\d*)\\s*(.*)$");
    static HashMap<String, Integer> shakespeareWorkToYearWritten = new HashMap<>();

    private KafkaConsumer<String,String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private KafkaProducer<Object,Object> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://schema-registry:8081");

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
        return producer;
    }

    private ShakespeareKey getShakespeareKey(String key) {
        Integer yearWritten = shakespeareWorkToYearWritten.get(key);
        if (yearWritten == null) {
            throw new RuntimeException(
                    "Could not find year written for \"" + key + "\"");
        }
        return new ShakespeareKey(key, yearWritten);
    }

    private ShakespeareValue getShakespeareLine(String line) {
        Matcher matcher = pattern.matcher(line);

        // Use a regex to parse out the line number from the rest of the line
        if (matcher.matches()) {
            // Get the line number and line and create the ShakespeareLine
            int lineNumber = Integer.parseInt(matcher.group(1));
            String lineOfWork = matcher.group(2);

            return new ShakespeareValue(lineNumber, lineOfWork);
        } else {
            // Line didn't match the regex
            System.out.println("Did not match Regex:" + line);

            return null;
        }
    }

    private void convert() {
        KafkaProducer<Object,Object> producer = createProducer();

        KafkaConsumer<String,String> consumer = createConsumer(); 
        consumer.subscribe(Arrays.asList("shakespeare_topic"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            // give some feedback...
            if(records.count() > 0){
                System.out.print(".");
            }
            for (ConsumerRecord<String, String> record : records) {

                // Get original strings from message and convert to Avro
                ShakespeareKey shakespeareKey = getShakespeareKey(record.key());
                ShakespeareValue shakespeareLine = getShakespeareLine(record.value());

                // Create the ProducerRecord with the Avro objects and send them
                ProducerRecord<Object, Object> avroRecord = 
                    new ProducerRecord<>("shakespeare_avro_topic", shakespeareKey, shakespeareLine);

                producer.send(avroRecord);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("*** Starting Shakespeare Converter ***");

        shakespeareWorkToYearWritten.put("Hamlet", 1600);
        shakespeareWorkToYearWritten.put("Julius Caesar", 1599);
        shakespeareWorkToYearWritten.put("Macbeth", 1605);
        shakespeareWorkToYearWritten.put("Merchant of Venice", 1596);
        shakespeareWorkToYearWritten.put("Othello", 1604);
        shakespeareWorkToYearWritten.put("Romeo and Juliet", 1594);

        ShakespeareConverter converter = new ShakespeareConverter();
        converter.convert();
    }
}