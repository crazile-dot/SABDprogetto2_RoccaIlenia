package Query2;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

public class SumAggregate implements AggregateFunction<Tuple7<Long, String, Integer, String, Integer, Integer, String>, Tuple5<Long, String, String, Long, Long>, Tuple7<Long, String, Integer, String, Integer, Integer, String>> {
    @Override
    public Tuple5<Long, String, String, Long, Long> createAccumulator() {
        return new Tuple5<>(0L, "", "", 0L, 0L);
    }

    @Override
    public Tuple5<Long, String, String, Long, Long> add(Tuple7<Long, String, Integer, String, Integer, Integer, String> value, Tuple5<Long, String, String, Long, Long> accumulator) {
        return new Tuple5<Long, String, String, Long, Long>(value.f0, value.f3, value.f6, accumulator.f3 + value.f4, accumulator.f4 + 1L);
    }

    @Override
    public Tuple7<Long, String, Integer, String, Integer, Integer, String> getResult(Tuple5<Long, String, String, Long, Long> accumulator) {
        return new Tuple7<>(accumulator.f0, "", 0, accumulator.f1, Math.toIntExact(accumulator.f4), 0, accumulator.f2);
    }

    @Override
    public Tuple5<Long, String, String, Long, Long> merge(Tuple5<Long, String, String, Long, Long> a, Tuple5<Long, String, String, Long, Long> b) {
        return new Tuple5<>(a.f0, a.f1, a.f2, a.f3 + b.f3, a.f4 + b.f4);
    }
}
