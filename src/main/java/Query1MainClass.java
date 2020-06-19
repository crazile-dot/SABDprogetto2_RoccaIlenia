import Query1.Failure;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.CsvParser;

public class Query1MainClass {

    /*public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Failure> stream = environment.readTextFile("data/bus-breakdown-and-delays.csv")
                .map(s -> CsvParser.parseCSV(s)).keyBy(f -> f.getBoro()).timeWindow(Time.hours(24))
                .aggregate();
        stream.print();
        environment.execute();

    }*/
}
