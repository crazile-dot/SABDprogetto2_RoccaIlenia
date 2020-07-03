import Query1.Average;
import Query1.AverageAggregate;
import Query1.TimestampGetterApply;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Query1MainClass {

    private static final String outputPath = "data/out.csv";

    public static void main(String[] args) throws Exception {

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
                .filter(t -> t != null && !t.f1.equals("") && t.f2 != 0);

        DataStream<Tuple6<Long, String, Double, String, Integer, Integer>> dailyStream = stream
                .keyBy(1).window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AverageAggregate());

        dailyStream.windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new TimestampGetterApply());

        /** settimanale */
        dailyStream.keyBy(1).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new Average())
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new TimestampGetterApply());

        /** mensile */
        dailyStream.keyBy(1).window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new Average())
                .windowAll(TumblingEventTimeWindows.of(Time.days(30)))
                .apply(new TimestampGetterApply()).print();

        environment.execute();
    }
}
