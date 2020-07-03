package pipeline;

import operators.AggregateRank;
import operators.GroupMapper;
import operators.RankProcess;
import operators.AggregateSlot;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.SinkBuilder;

public class Query2 {

    public static void start(DataStream<Tuple6<Long, String, Integer, String, Integer, Integer>> stream, String output) {

        DataStream<Tuple7<Long, String, Integer, String, Integer, Integer, String>> daily = stream
                /** divisione in due gruppi "morning" e "afternoon" */
                .flatMap(new GroupMapper())
                .keyBy(1).window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateSlot())
                .keyBy(3).window(TumblingEventTimeWindows.of(Time.days(1)))
                /** somma del campo "flag" per contare le cause di guasto dello stesso tipo */
                .sum(4);

        /** giornaliero */
        daily.keyBy(3, 6).window(TumblingEventTimeWindows.of(Time.days(1)))
                /** ricavo la classifica parziale delle cause di guasto */
                .aggregate(new AggregateRank())
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new RankProcess())
                .addSink(SinkBuilder.buildSink2(output + "Query2_daily_output.csv")).setParallelism(1);

        /** settimanale */
        daily.keyBy(3).window(TumblingEventTimeWindows.of(Time.days(1)))
                .sum(4)
                .keyBy(3, 6).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AggregateRank())
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .process(new RankProcess())
                .addSink(SinkBuilder.buildSink2(output + "Query2_weekly_output.csv")).setParallelism(1);

    }
}



