package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class CsvReader {

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

}
