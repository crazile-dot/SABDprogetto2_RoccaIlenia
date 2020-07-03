import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import util.DateParser;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Query2MainClass {

    private static final String outputPath = "data/out.csv";

    public static void main(String[] args) {
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

        DataStream<Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, Character>> stream = environment.addSource(consumer)
                .filter(t -> t != null && !t.f1.equals("") && t.f2 != 0)
                .flatMap(new FlatMapFunction<Tuple6<Long, String, Integer, String, Integer, Integer>, Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, Character>>() {
                    @Override
                    public void flatMap(Tuple6<Long, String, Integer, String, Integer, Integer> failure, Collector<Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, Character>> collector) throws Exception {
                        char c;
                        if (new DateTime(failure.f0).toLocalTime().compareTo
                                (DateParser.dateTimeParser("1111-11-11T12:00:00.000").toLocalTime()) < 0) {
                            c = 'A';
                        } else {
                            c = 'B';
                        }
                        collector.collect(new Tuple2<>(failure, c));
                    }
                });
        stream.keyBy(1).window(TumblingEventTimeWindows.of(Time.days(1)))
                /** somma tutte le stesse cause di guasto per fascia oraria */
                .reduce(new ReduceFunction<Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, Character>>() {
            @Override
            public Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, Character> reduce(Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, Character> failure1, Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, Character> failure2) throws Exception {
                Tuple6<Long, String, Integer, String, Integer, Integer> res = null;
                if(failure1.f0.f3.equals(failure2.f0.f3)) {
                    res = new Tuple6<Long, String, Integer, String, Integer, Integer>(failure1.f0.f0, failure1.f0.f1, failure1.f0.f2, failure1.f0.f3, failure1.f0.f4 + failure2.f0.f4, 0);
                }
                return new Tuple2<>(res, failure1.f1);
            }
        }).filter(t -> t != null && t.f0 != null);
        /**Ora bisogna ordinare gli elementi di ogni gruppo A e B in base al flag (il numero di volte
         * in cui compare ogni tipologia di reason)
         */
    }



