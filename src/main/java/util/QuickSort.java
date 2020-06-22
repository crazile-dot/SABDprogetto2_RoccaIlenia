package util;

import java.util.ArrayList;
import java.util.Collections;

public class QuickSort {

    public static void quickSort(ArrayList<String[]> arr, int low, int high) {
        //check for empty or null array
        if (arr == null || arr.size() == 0){
            return;
        }
        if (low >= high){
            return;
        }
        //Get the pivot element from the middle of the list
        int middle = low + ((high - low) / 2);
        String pivot = "";
        while(true) {
            for (int value = 1; value < 21; value++) {
                if (DateParser.isDateValid(arr.get(middle)[value - 1]) && DateParser.isDateValid(arr.get(middle)[value])) {
                    pivot = arr.get(middle)[value];
                    break;
                }
            }
            if (pivot.equals("") && middle != 0) {
                middle--;
            } else {
                break;
            }
        }
        // make left < pivot and right > pivot
        int i = low, j = high;
        while (i <= j) {
            //Check until all values on left side array are lower than pivot
            for (int value = 1; value < 21; value++) {
                if(DateParser.isDateValid(arr.get(i)[value - 1]) && DateParser.isDateValid(arr.get(i)[value])) {
                    while(DateParser.dateTimeParser(arr.get(i)[value]).compareTo(DateParser.dateTimeParser(pivot)) < 0 && i<=j) {
                        i++;
                        while (true) {
                            if (DateParser.isDateValid(arr.get(i)[value - 1]) && DateParser.isDateValid(arr.get(i)[value])) {
                                break;
                            } else {
                                i++;
                            }
                        }
                    }
                    break;
                }

            }
            //Check until all values on left side array are greater than pivot
            for (int value = 1; value < 21; value++) {
                if(DateParser.isDateValid(arr.get(j)[value - 1]) && DateParser.isDateValid(arr.get(j)[value])) {
                    while (DateParser.dateTimeParser(arr.get(j)[value]).compareTo(DateParser.dateTimeParser(pivot)) > 0 && i <= j) {
                        j--;
                        while (true) {
                            if (DateParser.isDateValid(arr.get(j)[value - 1]) && DateParser.isDateValid(arr.get(j)[value])) {
                                break;
                            } else {
                                j--;
                            }
                        }
                    }
                    break;
                }
            }
            //Now compare values from both side of lists to see if they need swapping
            //After swapping move the iterator on both lists
            if (i <= j) {
                swap (arr, i, j);
                i++;
                j--;
            }
        }
        //Do same operation as above recursively to sort two sub arrays
        if (low < j){
            quickSort(arr, low, j);
        }
        if (high > i){
            quickSort(arr, i, high);
        }
    }

    public static void swap (ArrayList<String[]> array, int x, int y) {
        for (int value = 1; value < 21; value++) {
            if (DateParser.isDateValid(array.get(x)[value - 1]) && DateParser.isDateValid(array.get(x)[value])) {
                if (DateParser.isDateValid(array.get(y)[value - 1]) && DateParser.isDateValid(array.get(y)[value])) {
                    Collections.swap(array, array.indexOf(array.get(x)), array.indexOf(array.get(y)));
                    break;
                }
            }
        }

    }

}
