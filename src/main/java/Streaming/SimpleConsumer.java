package Streaming;

import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple6;
import util.Constants;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import util.CustomDeserializer;

public class SimpleConsumer {

    public static FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Constants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Constants.ENABLE_AUTO_COMMIT);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET_RESET_EARLIER);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Constants.SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>> consumer = new FlinkKafkaConsumer<>(Constants.TOPIC_A, new CustomDeserializer(), props);
        consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Long, String, Integer, String, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple6<Long, String, Integer, String, Integer, Integer> element) {
                //System.out.println("Extract: " + Instant.ofEpochMilli(element.f0.getMillis()).atZone(ZoneId.systemDefault()).toLocalDateTime());
                return element.f0;
            }
        });
        //consumer.setStartFromLatest();
        return consumer;
    }
}

