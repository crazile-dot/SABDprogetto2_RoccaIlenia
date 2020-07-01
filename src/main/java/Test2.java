import Query1.AverageAggregate;
import Query1.Failure;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.joda.time.DateTime;
import util.CsvParser;
import util.CustomWatermarkEmitter;

import java.util.concurrent.TimeUnit;

public class Test2 {

    private static final String outputPath = "data/out.csv";

    public static void main(String[] args) throws Exception {

        Tuple4<StreamExecutionEnvironment, StreamingFileSink<Double>, StreamingFileSink<Tuple2<String, Integer>>, FlinkKafkaConsumer<String>> tuple3 = Environment.getEnvironment();
        StreamExecutionEnvironment environment = tuple3.f0;
        //StreamingFileSink<Tuple2<String, Integer>> sink = tuple3.f2;
        StreamingFileSink<Tuple3<DateTime, String, Integer>> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple3<DateTime, String, Integer>>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        FlinkKafkaConsumer<String> consumer = tuple3.f3;

        DataStream<Failure> parsed = environment.addSource(consumer)
                .filter(s -> s != null && !s.contains("School_Year") && !s.equals(""))
                .map(s -> CsvParser.customized1Parsing(s))
                .filter(f -> f != null && f.getHowLongDelayed() != 0 && !f.getBoro().equals(""))
                .assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        DataStream<Tuple3<DateTime, String, Integer>> stream = parsed
                .map(new MapFunction<Failure, Tuple3<DateTime, String, Integer>>() {
                    @Override
                    public Tuple3<DateTime, String, Integer> map(Failure failure) throws Exception {
                        return new Tuple3<>(failure.getOccurredOn(), failure.getBoro(), failure.getHowLongDelayed());
                    }
                });
        //SingleOutputStreamOperator<Double> res = stream.keyBy(1).window(TumblingEventTimeWindows.of(Time.hours(24)))
          //      .aggregate(new AverageAggregate());
        stream.print();
        stream.addSink(sink);

        environment.execute();
    }
}
