package Streaming;

import org.joda.time.Minutes;
import util.DateParser;

import java.util.ArrayList;

public class StreamSimulator {

    private static final String path = "data/bus-breakdown-and-delays.csv";

    /*public static String simulateStream(ArrayList<String[]> arrayList, int i) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(5000);
        String s = String.join(",", arrayList.get(i-1));
        return s;
    }*/

    public static String simulateStream(ArrayList<String[]> arrayList, int i) throws InterruptedException{
        String tuple = "";
        int difference;
        //int N = CsvReader.getNumCsvLines(path);
        for(int j = 1; j < arrayList.get(i-1).length - 1; j++) {
            if(i == 2) {
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
        if((time*60000)/43200 > 0) {
            Thread.sleep((time*60000)/43200);
        }
        String s = String.join(";", tuple);
        return s;
    }

}
