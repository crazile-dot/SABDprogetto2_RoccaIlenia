package Query2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import util.DateParser;

public class GroupMapper implements FlatMapFunction<Tuple6<Long, String, Integer, String, Integer, Integer>, Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, String>> {
    @Override
    public void flatMap(Tuple6<Long, String, Integer, String, Integer, Integer> failure, Collector<Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, String>> collector) throws Exception {
        String c;
        if (new DateTime(failure.f0).toLocalTime().compareTo
                (DateParser.dateTimeParser("1111-11-11T12:00:00.000").toLocalTime()) < 0) {
            c = "morning";
        } else {
            c = "afternoon";
        }
        collector.collect(new Tuple2<>(failure, c));
    }
}
