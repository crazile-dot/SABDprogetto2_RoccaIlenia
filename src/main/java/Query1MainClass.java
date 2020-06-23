import Query1.Failure;

import Streaming.SimpleConsumer;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.CsvParser;

public class Query1MainClass {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> consumer = SimpleConsumer.createConsumer();
        DataStream<String> stream = environment.addSource(consumer);

        DataStream<Failure> parsed = stream.map(s -> CsvParser.customizedParsing(s))
                .keyBy(f -> f.getBoro()).timeWindow(Time.hours(24)).reduce(
                        new ReduceFunction<Failure>() {
                            @Override
                            public Failure reduce(Failure failure1, Failure failure2) throws Exception {
                                int average = (failure1.getHowLongDelayed() + failure2.getHowLongDelayed())/2;
                                Failure newFailure = new Failure(failure1.getOccurredOn(), failure1.getBoro(), average);
                                return newFailure;
                            }
                        }
                );
        stream.print();
        environment.execute();

    }
}
