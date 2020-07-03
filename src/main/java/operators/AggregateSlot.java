package operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

public class AggregateSlot implements AggregateFunction<Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, String>, Tuple7<Long, String, Integer, String, Integer, Integer, String>, Tuple7<Long, String, Integer, String, Integer, Integer, String>> {
    @Override
    public Tuple7<Long, String, Integer, String, Integer, Integer, String> createAccumulator() {
        return new Tuple7<>();
    }

    @Override
    public Tuple7<Long, String, Integer, String, Integer, Integer, String> add(Tuple2<Tuple6<Long, String, Integer, String, Integer, Integer>, String> tuple2, Tuple7<Long, String, Integer, String, Integer, Integer, String> tuple7) {
        return new Tuple7<>(tuple2.f0.f0, tuple2.f0.f1, tuple2.f0.f2, tuple2.f0.f3, tuple2.f0.f4, tuple2.f0.f5, tuple2.f1);

    }

    @Override
    public Tuple7<Long, String, Integer, String, Integer, Integer, String> getResult(Tuple7<Long, String, Integer, String, Integer, Integer, String> tuple7) {
        return new Tuple7<>(tuple7.f0, tuple7.f1, tuple7.f2, tuple7.f3, tuple7.f4, tuple7.f5, tuple7.f6);
    }

    @Override
    public Tuple7<Long, String, Integer, String, Integer, Integer, String> merge(Tuple7<Long, String, Integer, String, Integer, Integer, String> tuple71, Tuple7<Long, String, Integer, String, Integer, Integer, String> tuple72) {
        return tuple71;
    }
}
