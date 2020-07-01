import Streaming.SimpleConsumer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Environment {

    private static final String outputPath = "data/out.csv";

    public static Tuple4<StreamExecutionEnvironment, StreamingFileSink<Double>, StreamingFileSink<Tuple2<String, Integer>>, FlinkKafkaConsumer<String>> getEnvironment() {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));

        final StreamingFileSink<Double> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Double>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        final StreamingFileSink<Tuple2<String, Integer>> testSink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple2<String, Integer>>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        FlinkKafkaConsumer<String> consumer = SimpleConsumer.createConsumer();
        return new  Tuple4<>(environment, sink, testSink, consumer);
    }
}
