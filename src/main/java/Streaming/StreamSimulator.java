package Streaming;

import org.joda.time.Minutes;
import util.CsvReader;
import util.DateParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class StreamSimulator {

    private static final String path = "data/bus-breakdown-and-delays.csv";

    public static String simulateStream(ArrayList<String[]> arrayList, int i) throws InterruptedException{
        String tuple = "";
        int difference;
        //int N = CsvReader.getNumCsvLines(path);
        for(int j = 1; j < arrayList.get(i-1).length - 1; j++) {
            if(i == 1) {
                difference = 0;
                tuple = getTuple(arrayList.get(i-1), difference);
            } else {
                if(DateParser.isDateValid(arrayList.get(i-2)[j-1]) && DateParser.isDateValid(arrayList.get(i-2)[j])) {
                    if(DateParser.isDateValid(arrayList.get(i-1)[j-1]) && DateParser.isDateValid(arrayList.get(i-1)[j])) {
                        difference = Math.abs(Minutes.minutesBetween(DateParser.dateTimeParser(arrayList.get(i-2)[j]),
                                DateParser.dateTimeParser(arrayList.get(i-1)[j])).getMinutes());
                        tuple = getTuple(arrayList.get(i-1), difference);
                        break;
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }
        }
        return tuple;
    }

    public static String getTuple(String[] tuple, int time) throws InterruptedException{
        //Thread.sleep((time*60000)/43200); //trasformazione dei minuti in millisecondi
        TimeUnit.MILLISECONDS.sleep((time*60000)/43200);
        String s = String.join(";", tuple);
        return s;
    }

}
