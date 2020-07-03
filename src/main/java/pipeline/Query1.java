package pipeline;

import operators.Average;
import operators.AverageAggregate;
import operators.TimestampGetterApply;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.SinkBuilder;

public class Query1 {

    public static void start(DataStream<Tuple6<Long, String, Integer, String, Integer, Integer>> stream, String output) {

        DataStream<Tuple6<Long, String, Double, String, Integer, Integer>> dailyStream = stream
                .keyBy(1).window(TumblingEventTimeWindows.of(Time.days(1)))
                /** calcolo della media */
                .aggregate(new AverageAggregate());

        /** giornaliero */
        dailyStream.windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new TimestampGetterApply())
                .addSink(SinkBuilder.buildSink1(output + "Query1_daily_output.csv")).setParallelism(1);

        /** settimanale */
        dailyStream.keyBy(1).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new Average())
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new TimestampGetterApply())
                .addSink(SinkBuilder.buildSink1(output + "Query1_weekly_output.csv")).setParallelism(1);

        /** mensile */
        dailyStream.keyBy(1).window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new Average())
                .windowAll(TumblingEventTimeWindows.of(Time.days(30)))
                .apply(new TimestampGetterApply())
                .addSink(SinkBuilder.buildSink1(output + "Query1_monthly_output.csv")).setParallelism(1);
    }
}
