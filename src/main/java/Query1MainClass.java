import Query1.AverageAggregate;
import Query1.Failure;

import Streaming.SimpleConsumer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.CsvParser;

public class Query1MainClass {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> consumer = SimpleConsumer.createConsumer();
        DataStream<Failure> parsed = environment.addSource(consumer).map(s -> CsvParser.customizedParsing(s));
        SingleOutputStreamOperator stream = parsed.keyBy(f -> f.getBoro()).timeWindow(Time.hours(24)).aggregate(new AverageAggregate());
        stream.print();
        environment.execute();



    }
}
