import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import pipeline.Query1;
import pipeline.Query2;
import util.Environment;

public class Main {

    public static void main(String[] args) throws Exception{

        final String outputPath = "s3://" + args[0];
        //final String outputPath = "Results/";


        Tuple2<StreamExecutionEnvironment, FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>>> tuple2 = Environment.getEnvironment();
        StreamExecutionEnvironment environment = tuple2.f0;
        FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>> consumer = tuple2.f1;

        DataStream<Tuple6<Long, String, Integer, String, Integer, Integer>> stream = environment.addSource(consumer)
                .filter(t -> t != null && !t.f1.equals("") && t.f2 != 0 && !t.f3.equals(""));

        Query1.start(stream, outputPath);
        Query2.start(stream, outputPath);

        environment.execute();
    }
}
