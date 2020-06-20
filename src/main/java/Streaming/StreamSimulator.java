package Streaming;

import org.joda.time.Minutes;
import util.DateParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class StreamSimulator {

    public static String simulateStream(ArrayList<String[]> arrayList) throws InterruptedException{
        int difference = 0;
        String[] tuple = new String[]{};
        for(int i = 1; i < arrayList.size(); i++) { //righe del csv
            for(int j = 1; j < arrayList.get(i-1).length; j++) { //colonne del csv
                if(DateParser.isDateValid(arrayList.get(i-1)[j-1]) && DateParser.isDateValid(arrayList.get(i-1)[j])) {
                    if(DateParser.isDateValid(arrayList.get(i)[j-1]) && DateParser.isDateValid(arrayList.get(i)[j])) {
                        tuple = getTuples(arrayList.get(i-1), difference);
                        difference = Math.abs(Minutes.minutesBetween(DateParser.dateTimeParser(arrayList.get(i-1)[j]),
                                DateParser.dateTimeParser(arrayList.get(i)[j])).getMinutes());
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }
        }
        return tuple.toString();
    }

    public static String[] getTuples(String[] tuple, int time) throws InterruptedException{
        Thread.sleep((time*60000)/21700); //trasformazione dei minuti in millisecondi
        //TimeUnit.MILLISECONDS.sleep((time*60000)/86400);
        System.out.println(tuple);
        return tuple;

    }
}
