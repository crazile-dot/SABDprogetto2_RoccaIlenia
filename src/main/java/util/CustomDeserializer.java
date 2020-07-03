package util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;
import java.text.ParseException;

public class CustomDeserializer implements DeserializationSchema<Tuple6<Long, String, Integer, String, Integer, Integer>> {

    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public Tuple6<Long, String, Integer, String, Integer, Integer> deserialize(byte[] bytes) throws IOException {
        String json = new String(bytes);
        Sentence comment = Sentence.parseJsonToObject(json);
        Tuple6<Long, String, Integer, String, Integer, Integer> res;
        try {
            res = comment.toTuple6();
        } catch (ParseException pe) {
            res = null;
        }
        return res;
    }

    @Override
    public boolean isEndOfStream(Tuple6<Long, String, Integer, String, Integer, Integer> tuple6) {
        return false;
    }

    @Override
    public TypeInformation<Tuple6<Long, String, Integer, String, Integer, Integer>> getProducedType() {
        return new TupleTypeInfo<>(
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(Integer.class));
    }
}

