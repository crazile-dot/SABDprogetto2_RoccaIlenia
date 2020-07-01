import Query1.Failure;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import util.CsvParser;
import util.DateParser;

public class Query2MainClass {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = Environment.getEnvironment().f0;
        StreamingFileSink<Double> sink = Environment.getEnvironment().f1;
        FlinkKafkaConsumer<String> consumer = Environment.getEnvironment().f3;

        DataStream<Tuple2<Failure, Character>> stream = environment.addSource(consumer).map(s -> CsvParser.customized2Parsing(s))
                .flatMap(new FlatMapFunction<Failure, Tuple2<Failure, Character>>() {
                    @Override
                    public void flatMap(Failure failure, Collector<Tuple2<Failure, Character>> collector) throws Exception {
                        char c;
                        if(failure.getOccurredOn().toLocalTime().compareTo
                                (DateParser.dateTimeParser("1111-11-11T12:00:00.000").toLocalTime()) < 0) {
                            c = 'A';
                        } else {
                            c = 'B';
                        }
                        collector.collect(new Tuple2<>(failure, c));
                    }
                });
        stream.keyBy(1).timeWindowAll(Time.seconds(5)).reduce(new ReduceFunction<Tuple2<Failure, Character>>() {
            @Override
            public Tuple2<Failure, Character> reduce(Tuple2<Failure, Character> failure1, Tuple2<Failure, Character> failure2) throws Exception {
                Failure res = null;
                if(failure1.f0.getReason().equals(failure2.f0.getReason())) {
                    res = new Failure(failure1.f0.getOccurredOn(), failure1.f0.getReason(), failure1.f0.getFlag() + failure2.f0.getFlag(), 0);
                }
                return new Tuple2<>(res, failure1.f1);
            }
        }).filter(t -> t != null && t.f0 != null);
        /**Ora bisogna ordinare gli elementi di ogni gruppo A e B in base al flag (il numero di volte
         * in cui compare ogni tipologia di reason.
         */
    }


}
