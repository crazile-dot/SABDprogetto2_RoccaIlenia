package Query2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

public class ReasonReduce implements ReduceFunction<Tuple7<Long, String, Integer, String, Integer, Integer, String>> {
    @Override
    public Tuple7<Long, String, Integer, String, Integer, Integer, String> reduce(Tuple7<Long, String, Integer, String, Integer, Integer, String> tuple71, Tuple7<Long, String, Integer, String, Integer, Integer, String> tuple72) throws Exception {
        return new Tuple7<>(tuple71.f0, tuple71.f1, tuple71.f2, tuple71.f3, tuple71.f4 + tuple72.f4, tuple71.f5, tuple71.f6);
    }
}
