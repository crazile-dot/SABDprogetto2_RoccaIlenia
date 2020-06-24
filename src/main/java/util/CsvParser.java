package util;

import Query1.Failure;
import org.joda.time.DateTime;

public class CsvParser {

    public static Failure parseCSV(String csvLine) {

        Failure failure = null;
        String[] csvValues = csvLine.split(";");

        String value1 = csvValues[0];
        int value2 = Integer.parseInt(csvValues[1]);
        String value3 = csvValues[2];
        String value4 = csvValues[3];
        String value5 = csvValues[4];
        String value6 = csvValues[5];
        String value7 = csvValues[6];
        DateTime value8 = DateParser.dateTimeParser(csvValues[7]);
        DateTime value9 = DateParser.dateTimeParser(csvValues[8]);
        String value10 = csvValues[9];
        String value11 = csvValues[10];
        String value12 = csvValues[11];
        int value13 = Integer.parseInt(csvValues[12]);
        String value14 = csvValues[13];
        String value15 = csvValues[14];
        String value16 = csvValues[15];
        DateTime value17 = DateParser.dateTimeParser(csvValues[16]);
        String value18 = csvValues[17];
        String value19 = csvValues[18];
        String value20 = csvValues[19];
        String value21 = csvValues[20];

        failure = new Failure(
                value1, value2, value3, value4, value5, value6, value7,
                value8, value9, value10, value11, value12, value13, value14, value15,
                value16, value17, value18, value19, value20, value21);

        return failure;
    }

    public static Failure customizedParsing(String csvLine) {
        Failure failure = null;
        String[] csvValues = csvLine.split(";");
        String occurred = "";
        String boro = "";
        String delay = "";
        int intDelay = 0;

        for(int j = 0; j < csvValues.length; j++) {
            if(csvValues[j].contains("min")) {
                delay = csvValues[j].replace("min", "");
            } else if(csvValues[j].contains("mins")) {
                delay = csvValues[j].replace("mins", "");
            } else if(csvValues[j].contains("minutes")) {
                delay = csvValues[j].replace("minutes", "");
            } else if(csvValues[j].contains("MIN")) {
                delay = csvValues[j].replace("MIN", "");
            } else if(csvValues[j].contains("MINS")) {
                delay = csvValues[j].replace("MINS", "");
            } else if(csvValues[j].contains("MINUTES")) {
                delay = csvValues[j].replace("MINUTES", "");
            } else if(csvValues[j].contains("Min")) {
                delay = csvValues[j].replace("Min", "");
            } else if(csvValues[j].contains("Mins")) {
                delay = csvValues[j].replace("Mins", "");
            } else if(csvValues[j].contains("Minutes")) {
                delay = csvValues[j].replace("Minutes", "");
            } else {
                break;
            }
            if(delay.contains(" ")) {
                delay.replace(" ", "");
            }
            if(delay.contains("-")) {
                String[] time = delay.split("-");
                if (isIntegerValid(time[0]) && isIntegerValid(time[1])) {
                    intDelay = (Integer.valueOf(time[0]) + Integer.valueOf(time[1])) / 2;
                }
            } else {
                if(isIntegerValid(delay)) {
                    intDelay = Integer.valueOf(delay);
                }
            }
            break;
        }
        for(int j = 0; j < csvValues.length; j++) {
            if(DateParser.isDateValid(csvValues[j])) {
                occurred = csvValues[j];
                boro = csvValues[j+2];
                break;
            }
        }
        failure = new Failure(
                DateParser.dateTimeParser(occurred), boro, intDelay);
        return failure;
    }

    public static boolean isIntegerValid(String value) {
        try {
            Integer.valueOf(value);
        } catch (IllegalArgumentException ia) {
            return false;
        }
        return true;
    }

}
