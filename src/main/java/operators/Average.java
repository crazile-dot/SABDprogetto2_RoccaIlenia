package operators;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;

public class Average implements AggregateFunction<Tuple6<Long, String, Double, String, Integer, Integer>, Tuple4<Long, String, Long, Long>, Tuple6<Long, String, Double, String, Integer, Integer>> {
    @Override
    public Tuple4<Long, String, Long, Long> createAccumulator() {
        return new Tuple4<>(0L, "", 0L, 0L);
    }

    @Override
    public Tuple4<Long, String, Long, Long> add(Tuple6<Long, String, Double, String, Integer, Integer> value, Tuple4<Long, String, Long, Long> accumulator) {
        return new Tuple4<Long, String, Long, Long>(value.f0, value.f1, accumulator.f2 + value.f2.longValue(), accumulator.f3 + 1L);
    }

    @Override
    public Tuple6<Long, String, Double, String, Integer, Integer> getResult(Tuple4<Long, String, Long, Long> accumulator) {
        return new Tuple6<>(accumulator.f0, accumulator.f1, Precision.round(((double) accumulator.f2)/accumulator.f3, 2), "", 0, 0);
    }

    @Override
    public Tuple4<Long, String, Long, Long> merge(Tuple4<Long, String, Long, Long> a, Tuple4<Long, String, Long, Long> b) {
        return new Tuple4<>(a.f0, a.f1, a.f2 + b.f2, a.f3 + b.f3);
    }
}
