import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final String outputPath = "data/out.csv";

    public static void main(String[] args) throws Exception{
        Tuple2<StreamExecutionEnvironment, FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>>> tuple2 = Environment.getEnvironment();
        StreamExecutionEnvironment environment = tuple2.f0;
        FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>> consumer = tuple2.f1;

        StreamingFileSink<Tuple2<DateTime, ArrayList<Tuple2<String, Double>>>> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple2<DateTime, ArrayList<Tuple2<String, Double>>>>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        DataStream<Tuple6<Long, String, Integer, String, Integer, Integer>> stream = environment.addSource(consumer)
                .filter(t -> t != null && !t.f1.equals("") && t.f2 != 0 && !t.f3.equals(""));

        Query1.start(stream);
        Query2.start(stream);

        environment.execute();
    }
}
