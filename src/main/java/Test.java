import Streaming.SimpleConsumer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.CustomWatermarkEmitter;

import java.util.concurrent.TimeUnit;

public class Test {

    private static final String outputPath = "data/out.csv";

    public static void main(String[] args) throws Exception {

        Tuple4<StreamExecutionEnvironment, StreamingFileSink<Double>, StreamingFileSink<Tuple2<String, Integer>>, FlinkKafkaConsumer<String>> tuple3 = Environment.getEnvironment();
        StreamExecutionEnvironment environment = tuple3.f0;
        StreamingFileSink<Tuple2<String, Integer>> sink = tuple3.f2;
        FlinkKafkaConsumer<String> consumer = tuple3.f3;

        DataStream<String[]> dataStream = environment.addSource(consumer)
                .flatMap(new Splitter0());
                //.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        DataStream<Tuple2<String, Integer>> stream = dataStream.flatMap(new Splitter()).keyBy(0).timeWindow(Time.seconds(5)).sum(1);
        stream.print();
        stream.addSink(sink);
        stream.writeAsCsv(outputPath);

        environment.execute();

    }
}
