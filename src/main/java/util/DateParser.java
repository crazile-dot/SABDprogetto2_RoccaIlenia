package util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DateParser {

    private final static String patternString = "yyyy-MM-dd'T'HH:mm:ss.sss";

    public static DateTime dateTimeParser(String date) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(patternString);
        DateTime dateTime = dtf.parseDateTime(date);
        return dateTime;
    }

    public static boolean isDateValid(String date) {
        try {
            dateTimeParser(date);
        } catch (IllegalArgumentException ia) {
            return false;
        }
        return true;
    }

}
