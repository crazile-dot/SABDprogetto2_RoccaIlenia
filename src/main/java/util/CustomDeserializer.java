package util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;
import java.text.ParseException;

public class CustomDeserializer implements DeserializationSchema<Tuple3<Long, String, Integer>> {

    private static final long serialVersionUID = 6154188370181669758L;


    /**
     * Deserializer method
     * @param bytes: stream read by kafka consumer
     * @return the comment object in flink tuple format
     */
    @Override
    public Tuple3<Long, String, Integer> deserialize(byte[] bytes) throws IOException {
        String json = new String(bytes);
        Sentence comment = Sentence.parseJsonToObject(json);
        Tuple3<Long, String, Integer> res;
        try {
            res = comment.toTuple3();
        } catch (ParseException pe) {
            res = null;
        }
        return res;
    }

    @Override
    public boolean isEndOfStream(Tuple3<Long, String, Integer> tuple3) {
        return false;
    }

    /**
     * Return the type information of Comment class
     */
    @Override
    public TypeInformation<Tuple3<Long, String, Integer>> getProducedType() {
        return new TupleTypeInfo<>(
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class));
    }
}

