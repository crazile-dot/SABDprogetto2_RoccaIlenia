package Streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Minutes;
import util.CsvReader;
import util.DateParser;
import util.QuickSort;
import util.Sorter;

public class SimpleProducer {

    private static final String path = "data/bus-breakdown-and-delays.csv";

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void produce() throws IOException, InterruptedException {
        Producer<String, String> producer = createProducer();
        //int numLines = CsvReader.getNumCsvLines(path);
        ArrayList<String[]> arrayList = CsvReader.getCsvLinesTest(path);
        //QuickSort.quickSort(arrayList, 0, arrayList.size() - 1);
        //ArrayList<String[]> sorted = Sorter.sortTuples(arrayList);
        /*for(String[] elem : sorted) {
            System.out.println(String.join(";", elem));
        }*/
        for(int i = 1; i <= arrayList.size(); i++) { //righe del csv
            String tuple = StreamSimulator.simulateStream(arrayList, i);
            producer.send(new ProducerRecord<String, String>(Constants.TOPIC_NAME, tuple));
            System.out.println(tuple);
            //count++;
        }
        //System.out.println(count);
        producer.close();
    }

    public static void main(String[] args) {
        try {
            produce();
        } catch (InterruptedException ie) {
            System.out.println("Errore del producer");
        } catch (IOException io) {
            System.out.println("Errore di IO");
        }
    }


}
