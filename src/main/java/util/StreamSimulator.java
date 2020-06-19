package util;

import org.joda.time.Minutes;
import java.util.ArrayList;

public class StreamSimulator {

    public static String[] simulateStream(ArrayList<String[]> arrayList) throws InterruptedException{
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
        return tuple;
    }

    public static String[] getTuples(String[] tuple, int time) throws InterruptedException{
        Thread.sleep((time*60000)/86400); //trasformazione dei minuti in millisecondi
        System.out.println(tuple);
        return tuple;

    }
}
