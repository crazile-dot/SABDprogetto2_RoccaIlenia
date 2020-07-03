import Query2.AggregateRank;
import Query2.GroupMapper;
import Query2.RankProcess;
import Query2.AggregateSlot;
import Query2.SumAggregate;
import Query2.ReasonReduce;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query2 {

    public static void start(DataStream<Tuple6<Long, String, Integer, String, Integer, Integer>> stream) {

        DataStream<Tuple7<Long, String, Integer, String, Integer, Integer, String>> daily = stream
                .flatMap(new GroupMapper())
                .keyBy(1).window(TumblingEventTimeWindows.of(Time.days(1)))
                /** somma tutte le stesse cause di guasto per fascia oraria */
                .aggregate(new AggregateSlot())
                //.filter(t -> t != null && t.f0 != null);
        /**Ora bisogna ordinare gli elementi di ogni gruppo A e B in base al flag (il numero di volte
         * in cui compare ogni tipologia di reason)
         */
                .keyBy(3).window(TumblingEventTimeWindows.of(Time.days(1)))
                .sum(4);

                daily.keyBy(3, 6).window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateRank())
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new RankProcess());

        daily.keyBy(3).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new SumAggregate())
                .keyBy(3, 6).window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateRank())
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new RankProcess());

    }
}



