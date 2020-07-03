package util;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SinkBuilder {

    public static StreamingFileSink<Tuple2<DateTime, ArrayList<Tuple2<String, Double>>>> buildSink1(String outputPath) {
        StreamingFileSink<Tuple2<DateTime, ArrayList<Tuple2<String, Double>>>> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple2<DateTime, ArrayList<Tuple2<String, Double>>>>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        return sink;
    }

    public static StreamingFileSink<Tuple2<DateTime, List<Tuple2<String, List<String>>>>> buildSink2(String outputPath) {
        StreamingFileSink<Tuple2<DateTime, List<Tuple2<String, List<String>>>>> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple2<DateTime, List<Tuple2<String, List<String>>>>>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        return sink;
    }
}
