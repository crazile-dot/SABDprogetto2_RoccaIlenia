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

    public static void main(String[] args) {

       System.out.println(isDateValid("2012-2013"));


        /*Pattern pattern = Pattern.compile("\\d{4}\\S\\d{2}\\S\\d{2}T\\s{2}\\S\\d{2}\\S\\d{2}\\S\\d{3}",
                Pattern.CASE_INSENSITIVE);

        Pattern pattern = Pattern.compile(patternString);

        String input = "2015-09-01T07:30:00.000";

        // create a matcher that will match the given input against this pattern
        Matcher matcher = pattern.matcher(input);

        boolean matchFound = matcher.matches();
        System.out.println(input + " - matches: " + matchFound);

        input = "this passes the test";
        // reset the matcher with a new input sequence
        matcher.reset(input);
        matchFound = matcher.matches();
        System.out.println(input + " - matches: " + matchFound);

        // Attempts to match the input sequence, starting at the beginning
        // of the region, against the pattern
        matchFound = matcher.lookingAt();
        System.out.println(input + " - matches from the beginning: " + matchFound);*/

    }

}
