package util;

public class CsvParser {

    public static Failure customizedParsing(String csvLine) {
        String[] csvValues = csvLine.split(";");
        Failure failure = null;
        String occurred = "";
        String bBoro = "";
        String delay = "";
        int intDelay = 0;

        for(int j = 0; j < csvValues.length; j++) {
            if(csvValues[j].contains("minutes")) {
                delay = csvValues[j].replace("minutes", "");
            } else if(csvValues[j].contains("mins")) {
                delay = csvValues[j].replace("mins", "");
            } else if(csvValues[j].contains("min")) {
                delay = csvValues[j].replace("min", "");
            } else if(csvValues[j].contains("MINUTES")) {
                delay = csvValues[j].replace("MINUTES", "");
            } else if(csvValues[j].contains("MINS")) {
                delay = csvValues[j].replace("MINS", "");
            } else if(csvValues[j].contains("MIN")) {
                delay = csvValues[j].replace("MIN", "");
            } else if(csvValues[j].contains("Minutes")) {
                delay = csvValues[j].replace("Minutes", "");
            } else if(csvValues[j].contains("Mins")) {
                delay = csvValues[j].replace("Mins", "");
            } else if(csvValues[j].contains("Min")) {
                delay = csvValues[j].replace("Min", "");
            }
            if(!delay.equals("") && delay.contains(".")) {
                delay = delay.replace(".", "");
            }
            if(!delay.equals("")) {
                delay = delay.trim();
            }
            if(!delay.equals("")) {
                if(delay.contains("-")) {
                    String[] time = delay.split("-");
                    if (time.length > 1 &&isIntegerValid(time[0]) && isIntegerValid(time[1])) {
                        intDelay = (Integer.valueOf(time[0]) + Integer.valueOf(time[1])) / 2;
                    }
                }else if(delay.contains("/")) {
                    String[] time = delay.split("/");
                    if (time.length > 1 && isIntegerValid(time[0]) && isIntegerValid(time[1])) {
                        intDelay = (Integer.valueOf(time[0]) + Integer.valueOf(time[1])) / 2;
                    }
                } else {
                    if(isIntegerValid(delay)) {
                        intDelay = Integer.valueOf(delay);
                    }
                }
            }
            if(!delay.equals("")) {
                break;
            }

        }
        for(int j = 2; j < csvValues.length; j++) {
            if(DateParser.isDateValid(csvValues[j-2])) {
                occurred = csvValues[j-2];
                bBoro = csvValues[j];
                failure = new Failure(
                        DateParser.dateTimeParserMillis(occurred), bBoro, intDelay, csvValues[5], 1, 0);
                break;
            }
        }
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
