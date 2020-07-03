package util;

import streaming.SimpleConsumer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.joda.time.DateTime;

public class Environment {

    private static final String outputPath = "data/out.csv";

    /** Creazione dell'enviroment e di un FlinkKafkaConsumer
     *
     * @return
     */
    public static Tuple2<StreamExecutionEnvironment, FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>>> getEnvironment() {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setLatencyTrackingInterval(2000L);
        environment.registerTypeWithKryoSerializer(DateTime.class, JodaDateTimeSerializer.class);

        FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>> consumer = SimpleConsumer.createConsumer();

        return new  Tuple2<>(environment, consumer);
    }
}
