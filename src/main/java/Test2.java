import Query1.AverageAggregate;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.joda.time.DateTime;
import java.util.concurrent.TimeUnit;

public class Test2 {

    private static final String outputPath = "data/out.csv";

    public static void main(String[] args) throws Exception {

        Tuple4<StreamExecutionEnvironment, StreamingFileSink<Double>, StreamingFileSink<Tuple2<String, Integer>>, FlinkKafkaConsumer<Tuple3<Long, String, Integer>>> tuple3 = Environment.getEnvironment();
        StreamExecutionEnvironment environment = tuple3.f0;
        //StreamingFileSink<Tuple2<String, Integer>> sink = tuple3.f2;
        StreamingFileSink<Tuple3<String, DateTime, Integer>> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple3<String, DateTime, Integer>>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        FlinkKafkaConsumer<Tuple3<Long, String, Integer>> consumer = tuple3.f3;

        DataStream<Tuple3<Long, String, Double>> stream = environment.addSource(consumer)
                .filter(t -> t != null && !t.f1.equals("") && t.f2 != 0)
        .keyBy(1).window(TumblingEventTimeWindows.of(Time.hours(24)))
                /*.reduce(new ReduceFunction<Tuple3<Long, String, Integer>>() {
                    @Override
                    public Tuple3<Long, String, Integer> reduce(Tuple3<Long, String, Integer> failure1, Tuple3<Long, String, Integer> failure2) throws Exception {
                        int sum = failure1.f2 + failure2.f2;
                        return new Tuple3<>(failure1.f0, failure1.f1, sum);
                    }
                });*/
                .aggregate(new AverageAggregate());
        stream.print();
        //parsed.addSink(sink);

        environment.execute();
    }
}
