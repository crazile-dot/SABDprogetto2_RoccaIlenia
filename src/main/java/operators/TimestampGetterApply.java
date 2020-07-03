package operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import java.util.ArrayList;
import java.util.Comparator;

public class TimestampGetterApply implements AllWindowFunction<Tuple6<Long, String, Double, String, Integer, Integer>, Tuple2<DateTime, ArrayList<Tuple2<String, Double>>>, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple6<Long, String, Double, String, Integer, Integer>> iterable, Collector<Tuple2<DateTime, ArrayList<Tuple2<String, Double>>>> collector) throws Exception {
        ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
        iterable.iterator().forEachRemaining(t -> {
                Tuple2<String, Double> tuple2 = new Tuple2<>(t.f1, t.f2);
                list.add(tuple2);
        });
        list.sort(Comparator.comparing(t -> t.f0));
        collector.collect(new Tuple2<>(new DateTime(timeWindow.getStart()), list));
    }
}
