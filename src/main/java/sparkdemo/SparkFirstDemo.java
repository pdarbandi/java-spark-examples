package sparkdemo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkFirstDemo {

    // A demonstration function to demonstrate how a custom mapping is just a regular java function
    public static int countWords(final String line) {
        List<String> words = new ArrayList<String>(Arrays.asList(line.split(" ")));
        words.remove("");
        return words.size();
    }

    public static void main(String[] args) {
        SparkSession sparkSession;

        if(Arrays.asList(args).contains("--cluster")) {
            sparkSession = SparkSession.builder().getOrCreate();
        }
        else {
            sparkSession = SparkSession.builder()
                                       .master("local[1]")
                                       .config("spark.hadoop.validateOutputSpecs", false)
                                       .getOrCreate();
        }

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        List<Integer> numbers = Arrays.asList(4, 5, 6, 7, 8);

        JavaRDD<Integer> numberRdd = sc.parallelize(numbers);

        System.out.println(numberRdd.count());

        System.out.println("Done");
    }
}
