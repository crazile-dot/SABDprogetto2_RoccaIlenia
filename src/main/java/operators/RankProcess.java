package operators;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RankProcess extends ProcessAllWindowFunction<Tuple1<List<Tuple5<Long, String, Integer, Integer, String>>>, Tuple2<DateTime, List<Tuple2<String, List<String>>>>, TimeWindow> {

    @Override
    public void process(Context context, Iterable<Tuple1<List<Tuple5<Long, String, Integer, Integer, String>>>> iterable, Collector<Tuple2<DateTime, List<Tuple2<String, List<String>>>>> collector) throws Exception {
        List<Tuple5<Long, String, Integer, Integer, String>> list = new ArrayList<>();
        iterable.forEach(t -> t.f0.forEach(list::add));
        list.sort(Comparator.comparing(o -> -o.f2));
        List<Tuple5<Long, String, Integer, Integer, String>> l = new ArrayList<>(list.subList(0, Math.min(3, list.size())));
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        List<Tuple2<String, List<String>>> out = new ArrayList<>();
        Tuple2<String, List<String>> tuple21;
        Tuple2<String, List<String>> tuple22;
        for(Tuple5<Long, String, Integer, Integer, String> elem : l) {
            if(elem.f4.equals("morning")) {
                list1.add(elem.f1);
            } else {
                list2.add(elem.f1);
            }
        }
        tuple21 = new Tuple2<>("morning", list1);
        tuple22 = new Tuple2<>("afternoon", list2);
        out.add(tuple21);
        out.add(tuple22);
        collector.collect(new Tuple2<>(new DateTime(context.window().getStart()), out));
    }
}

