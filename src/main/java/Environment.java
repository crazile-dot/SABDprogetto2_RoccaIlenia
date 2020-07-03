import Streaming.SimpleConsumer;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.sun.org.apache.xml.internal.serializer.utils.SerializerMessages_sv;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import  org.apache.flink.api.common.*;
import org.apache.flink.api.java.typeutils.runtime.kryo.*;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Environment {

    private static final String outputPath = "data/out.csv";

    public static Tuple2<StreamExecutionEnvironment, FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>>> getEnvironment() {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setLatencyTrackingInterval(2000L);
        environment.registerTypeWithKryoSerializer(DateTime.class, JodaDateTimeSerializer.class);

        FlinkKafkaConsumer<Tuple6<Long, String, Integer, String, Integer, Integer>> consumer = SimpleConsumer.createConsumer();

        return new  Tuple2<>(environment, consumer);
    }
}
