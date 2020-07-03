package util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;

import java.io.IOException;
import java.text.ParseException;

public class Sentence {

    private String occurredOn;
    private String boro;
    private String howLongDelayed;
    private String reason;
    private String flag;
    private String rank;

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
    public Tuple6<Long, String, Integer, String, Integer, Integer> toTuple6() throws NumberFormatException, ParseException {
        return new Tuple6<>(
                Long.valueOf(this.occurredOn),
                this.boro,
                Integer.valueOf(this.howLongDelayed),
                this.reason,
                Integer.valueOf(flag),
                Integer.valueOf(rank)
        );
    }

    public Sentence(String dateTime, String boro, String delay, String reason, String flag, String rank) {
        this.occurredOn = dateTime;
        this.boro = boro;
        this.howLongDelayed = delay;
        this.reason = reason;
        this.flag = flag;
        this.rank = rank;
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

    public String getReason() {
        return reason;
    }

    public String getFlag() {
        return flag;
    }

    public String getRank() {
        return rank;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public Sentence() {
    }
}




