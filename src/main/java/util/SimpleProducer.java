package util;

import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

    private static final String path = "data/bus-breakdown-and-delays.csv";

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ProducerConfig.ACKS_CONFIG, Constants.ACKS);
        props.put(ProducerConfig.RETRIES_CONFIG, Constants.RETRIES);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Constants.BATCH_SIZE);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void produce() throws IOException, InterruptedException {
        Producer<String, String> producer = createProducer();
        /*producer.send(new ProducerRecord<String, String>(Constants.TOPIC_NAME,
                StreamSimulator.simulateStream(CsvReader.getCsvLines(path))));*/
        producer.send(new ProducerRecord<String, String>(Constants.TOPIC_NAME, "hello guys have a nice day!"));
        producer.close();
    }

    public static void main(String[] args) {
        try {
            produce();
        } catch (InterruptedException ie) {
            System.out.println("errore del produttore");
        } catch (IOException io) {
            System.out.println("errore del file");
        }
    }

}
