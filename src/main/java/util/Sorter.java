package util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Sorter {

    /*public static void sortTuples(ArrayList<String[]> arrayList) {
        arrayList.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] o1, String[] o2) {
                //for(int i = 1; i < arrayList.size(); i++) {
                for(int j = 1; j < (o1.length/2)+1; j++) {
                    if (DateParser.isDateValid(o1[j - 1]) && DateParser.isDateValid(o1[j])) {
                        if (DateParser.isDateValid(o2[j - 1]) && DateParser.isDateValid(o2[j])) {
                            return DateParser.dateTimeParser(o1[j]).compareTo(DateParser.dateTimeParser(o2[j]));
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                return 2;
            }
        });
    }*/

    public static ArrayList<String[]> sortTuples(ArrayList<String[]> arrayList) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("myApplication");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //List<String[]> mySource = arrayList;
        long startTime = System.nanoTime();
        JavaRDD<String[]> source = sc.parallelize(arrayList);
        JavaRDD<String[]> withoutFirstRow = source.zipWithIndex().filter(t -> t._2() > 0).map(t -> t._1());
        JavaRDD<String[]> tuples = withoutFirstRow.mapToPair(
                new PairFunction<String[], String, String[]>() {
                    @Override
                    public Tuple2<String, String[]> call(String[] strings) {
                        String date = "";
                        for(int j = 1; j < strings.length; j++) {
                            if(DateParser.isDateValid(strings[j-1]) && DateParser.isDateValid(strings[j])) {
                                date = strings[j];
                                break;
                            }
                        }
                        Tuple2<String, String[]> tuple2 = new Tuple2<>(date, strings);
                        return tuple2;
                    }
                }
        ).sortByKey().map(t -> t._2());
        System.out.println(System.nanoTime() - startTime);
        List<String[]> list = tuples.collect();
        ArrayList<String[]> array = new ArrayList<>(list);
        return array;
    }
}






                    /*@Override
                    public Tuple2<DateTime, String[]> call(String[] strings) throws Exception {

                            }
                        }

                    }*/

