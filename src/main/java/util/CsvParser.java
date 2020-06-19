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
}
