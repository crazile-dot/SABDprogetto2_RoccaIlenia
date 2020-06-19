package util;

import org.joda.time.Minutes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class CsvReader {

    private static final String path = "data/bus-breakdown-and-delays.csv";

    public static ArrayList<String[]> getCsvLines(String path) throws IOException {
        ArrayList<String[]> csvValues = new ArrayList<>();
        File csvFile = new File(path);
        String row = "";
        if (csvFile.isFile()) {
            BufferedReader csvReader = new BufferedReader(new FileReader(path));
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(";");
                csvValues.add(data);
            }
            csvReader.close();
        }
        return csvValues;
    }

    public static void main(String[] args) throws IOException, InterruptedException{
        /*ArrayList<String[]> arrayList = getCsvLines(path);
        for(String[] elem : arrayList) {
            for(String s : elem) {
                System.out.println(s);
            }
            System.out.println("\n");
        }*/
        //System.out.println(arrayList);
        /*int difference = Math.abs(Minutes.minutesBetween(DateParser.dateTimeParser("2015-09-04T16:35:00.000"),
                DateParser.dateTimeParser("2015-09-08T05:18:00.000")).getMinutes());*/
        StreamSimulator.simulateStream(getCsvLines(path));
        //System.out.println(difference);
    }
}
