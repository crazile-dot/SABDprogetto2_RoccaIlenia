package operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AggregateRank implements AggregateFunction<Tuple7<Long, String, Integer, String, Integer, Integer, String>, List<Tuple5<Long, String, Integer, Integer, String>>, Tuple1<List<Tuple5<Long, String, Integer, Integer, String>>>> {
    @Override
    public List<Tuple5<Long, String, Integer, Integer, String>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Tuple5<Long, String, Integer, Integer, String>> add(Tuple7<Long, String, Integer, String, Integer, Integer, String> tuple7, List<Tuple5<Long, String, Integer, Integer, String>> list) {
        list.add(new Tuple5<>(tuple7.f0, tuple7.f3, tuple7.f4, tuple7.f5, tuple7.f6));
        return list;
    }

    @Override
    public Tuple1<List<Tuple5<Long, String, Integer, Integer, String>>> getResult(List<Tuple5<Long, String, Integer, Integer, String>> list) {
        list.sort(Comparator.comparing(o -> -o.f2));
        return new Tuple1<>(new ArrayList<>(list.subList(0, Math.min(3, list.size()))));
    }

    @Override
    public List<Tuple5<Long, String, Integer, Integer, String>> merge(List<Tuple5<Long, String, Integer, Integer, String>> list1, List<Tuple5<Long, String, Integer, Integer, String>> list2) {
        list1.forEach(t -> list2.add(t));
        return list2;
    }
}
