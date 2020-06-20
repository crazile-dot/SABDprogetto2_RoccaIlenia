package util;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {

    public static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Constants.ENABLE_AUTO_COMMIT);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Constants.AUTO_COMMIT_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Constants.SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(Constants.TOPIC_NAME));
        return consumer;
    }

    public static void consume() {
        KafkaConsumer<String, String> consumer = createConsumer();
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

        consumerRecords.forEach(record -> {
            System.out.println("Record Key " + record.key());
            System.out.println("Record value " + record.value());
            System.out.println("Record partition " + record.partition());
            System.out.println("Record offset " + record.offset());
        });
        // commits the offset of record to broker.
        consumer.commitAsync();
        consumer.close();
    }

    public static void main(String[] args) {
        consume();
    }
}

