package util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple3;
import java.io.IOException;
import java.text.ParseException;

public class Sentence {

    private String occurredOn;
    private String boro;
    private String howLongDelayed;

    /**
     * Method to parse a json-formatted object to a Comment class
     * @param json: the json string to parse
     * @return: the comment object
     */
    public static Sentence parseJsonToObject(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(json, Sentence.class);
    }
    /**
     * @return the object in a Tuple15 flink format
     */
    public Tuple3<Long, String, Integer> toTuple3() throws NumberFormatException, ParseException {
        return new Tuple3<>(
                Long.valueOf(this.occurredOn),
                this.boro,
                Integer.valueOf(this.howLongDelayed)
        );
    }

    public Sentence(String dateTime, String boro, String delay) {
        this.occurredOn = dateTime;
        this.boro = boro;
        this.howLongDelayed = delay;
    }

    public String getOccurredOn() {
        return occurredOn;
    }

    public String getBoro() {
        return boro;
    }

    public String getHowLongDelayed() {
        return howLongDelayed;
    }

    public void setOccurredOn(String occurredOn) {
        this.occurredOn = occurredOn;
    }

    public void setBoro(String boro) {
        this.boro = boro;
    }

    public void setHowLongDelayed(String howLongDelayed) {
        this.howLongDelayed = howLongDelayed;
    }

    public Sentence() {
    }
}




