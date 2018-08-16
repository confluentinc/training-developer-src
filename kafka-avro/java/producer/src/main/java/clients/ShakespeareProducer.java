package clients;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ShakespeareProducer {
    private final static String INPUT_PATH_NAME = "/datasets/shakespeare";

    private KafkaProducer<String, String> createProducer(){
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(settings);
        return producer;
    }

    private void sendFile(File inputFile, KafkaProducer<String, String> producer)
        throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String key = inputFile.getName().split("\\.")[0];
        String line = null;

        while ((line = reader.readLine()) != null) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>("shakespeare_topic", key, line);
            producer.send(record);
        }

        reader.close();

        System.out.println("Finished producing file:" + inputFile.getName());
    }

    private void runProducer() throws IOException {
        KafkaProducer<String, String> producer = createProducer();
        File inputFile = new File(INPUT_PATH_NAME);
        if(inputFile.isDirectory()){
            for(File fileInDirectory : inputFile.listFiles()) {
                sendFile(fileInDirectory, producer);
            }
        } else {
            sendFile(inputFile, producer);
        }
    }

    public static void main(String[] args) {
        System.out.println("%%% Starting Shakespeare Producer %%%");
        try {
            ShakespeareProducer shakespeareProducer = new ShakespeareProducer();
            shakespeareProducer.runProducer();
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}