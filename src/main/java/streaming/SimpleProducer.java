package streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import util.Constants;
import util.CsvParser;
import util.CsvReader;
import util.Failure;

public class SimpleProducer {

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /** Creazione di un KafkaProducer e simulazione dello streaming dei messaggi
     * @param path
     * @throws IOException
     * @throws InterruptedException
     */
    public static void produce(String path) throws IOException, InterruptedException {
        Producer<String, String> producer = createProducer();
        ArrayList<String[]> arrayList = CsvReader.getCsvLines(path);
        for(int i = 2; i <= arrayList.size(); i++) {
            String string = StreamSimulator.simulateStream(arrayList, i);
            Failure failure = CsvParser.customizedParsing(string);
            if(failure != null) {
                producer.send(new ProducerRecord<String, String>(Constants.TOPIC_A, failure.toJson()));
                System.out.println(failure.toJson());
            }
        }
        producer.close();
    }

    public static void main(String[] args) {

        final String pathToFile = "s3://" + args[0] + "/bus-breakdown-and-delays.csv";
        //final String pathToFile = "data/bus-breakdown-and-delays.csv";

        try {
            produce(pathToFile);
        } catch (InterruptedException ie) {
            System.out.println("Errore del producer");
        } catch (IOException io) {
            System.out.println("Errore di IO");
        }
    }


}
