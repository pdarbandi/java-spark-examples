package sparkdemo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkFilterDemo {

    public static void main(String[] args) {
        SparkSession sparkSession;

        if(Arrays.asList(args).contains("--cluster")) {
            sparkSession = SparkSession.builder().getOrCreate();
        }
        else {
            // the validateOutputSpecs=false option allows overwriting of output text files
            sparkSession = SparkSession.builder()
                                       .master("local[1]")
                                       .config("spark.hadoop.validateOutputSpecs", false)
                                       .getOrCreate();
        }

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaRDD<String> lines = sc.textFile("data/Macbeth.txt");

        // The number of lines that were read
        System.out.println("Total Lines:" + lines.count());

        // Filter by lines containing the word "foul"
        System.out.println("Foul Lines = " +
                           lines.filter(line -> line.contains("foul")).count());

        // How to filter by non-empty lines, split each line by word, AND
        // count the words in each line

        // Could we save our new RDD of numbers?

        // How would we get the total word count?

        // how many words in the longest line?

        // How would we find the text of the longest line? (reduce by line length?)

        System.out.println("Done");
    }
}
