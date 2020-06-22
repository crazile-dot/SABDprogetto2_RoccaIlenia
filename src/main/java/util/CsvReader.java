package util;

import java.io.*;
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

    public static ArrayList<String[]> getCsvLinesTest(String path) throws IOException {
        ArrayList<String[]> csvValues = new ArrayList<>();
        File csvFile = new File(path);
        String row = "";
        if (csvFile.isFile()) {
            BufferedReader csvReader = new BufferedReader(new FileReader(path));
            while (csvValues.size() < 1000) {
                row = csvReader.readLine();
                String[] data = row.split(";");
                csvValues.add(data);
            }
            csvReader.close();
        }
        return csvValues;
    }

    public static int getNumCsvLines(String path) {
        File file = new File(path);
        int linenumber = 0;
        try{
            if(file.exists()){
                FileReader fr = new FileReader(file);
                LineNumberReader lnr = new LineNumberReader(fr);
                while (lnr.readLine() != null){
                    linenumber++;
                }
                //System.out.println("Total number of lines : " + linenumber);
                lnr.close();
            }else{
                System.out.println("File does not exists!");
            }
        }catch(IOException e){
            e.printStackTrace();
        }
        return linenumber;
    }
}
