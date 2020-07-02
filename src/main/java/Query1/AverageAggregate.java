package Query1;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.joda.time.DateTime;

import java.text.DecimalFormat;


public class AverageAggregate implements AggregateFunction<Tuple3<Long, String, Integer>, Tuple4<Long, String, Long, Long>, Tuple3<Long, String, Double>> {
    @Override
    public Tuple4<Long, String, Long, Long> createAccumulator() {
        return new Tuple4<>(0L, "", 0L, 0L);
    }

    @Override
    public Tuple4<Long, String, Long, Long> add(Tuple3<Long, String, Integer> value, Tuple4<Long, String, Long, Long> accumulator) {
        return new Tuple4<Long, String, Long, Long>(value.f0, value.f1, accumulator.f2 + value.f2, accumulator.f3 + 1L);
    }

    @Override
    public Tuple3<Long, String, Double> getResult(Tuple4<Long, String, Long, Long> accumulator) {
        return new Tuple3<>(accumulator.f0, accumulator.f1, Precision.round(((double) accumulator.f2)/accumulator.f3, 2));
    }

    @Override
    public Tuple4<Long, String, Long, Long> merge(Tuple4<Long, String, Long, Long> a, Tuple4<Long, String, Long, Long> b) {
        return new Tuple4<>(a.f0, a.f1, a.f2 + b.f2, a.f3 + b.f3);
    }
}


