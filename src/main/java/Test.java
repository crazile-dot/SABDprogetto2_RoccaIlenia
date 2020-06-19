import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.ZonedDateTime;

public class Test {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<Integer, Long>> windowed
                = env.fromElements(
                new Tuple2<>(16, ZonedDateTime.now().plusMinutes(25).toInstant().getEpochSecond()),
                new Tuple2<>(15, ZonedDateTime.now().plusMinutes(2).toInstant().getEpochSecond()))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <Tuple2<Integer, Long>>(Time.seconds(20)) {

                            @Override
                            public long extractTimestamp(Tuple2<Integer, Long> element) {
                                return element.f1 * 1000;
                            }
                        });

        SingleOutputStreamOperator<Tuple2<Integer, Long>> reduced = windowed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .maxBy(0, true);
        reduced.print();
        env.execute();
    }
}

