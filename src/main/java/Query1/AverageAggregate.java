package Query1;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.joda.time.DateTime;

import java.text.DecimalFormat;


public class AverageAggregate implements AggregateFunction<Tuple3<DateTime, String, Integer>, Tuple2<Long, Long>, Double> {
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(Tuple3<DateTime, String, Integer> value, Tuple2<Long, Long> accumulator) {
        return new Tuple2<Long, Long>(accumulator.f0 + value.f2, accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return Precision.round(((double) accumulator.f0)/accumulator.f1, 2);
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}


