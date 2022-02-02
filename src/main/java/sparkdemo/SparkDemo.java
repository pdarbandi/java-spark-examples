package sparkdemo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkDemo {

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

        JavaRDD<String> lines = sc.textFile("data/Macbeth.txt");

        System.out.println("Foul Lines = " +
                           lines.filter(line -> line.contains("foul")).count());

        System.out.println("Total Lines:" + lines.count());

        /*
        JavaRDD<Integer> numWords = lines.map(line -> Arrays.stream(line.split(" "))
                                                        .filter(value -> value.length() > 0)
                                                        .toArray().length);
        */
        JavaRDD<Integer> numWords = lines.map(line -> countWords(line));

        numWords.saveAsTextFile("data/numWords");

        System.out.println("totalWords:" + numWords.reduce( (prev, current) -> prev + current));

        System.out.println("max Words in a line:" + numWords.reduce( (prev, current) -> prev > current ? prev : current));

        System.out.println("Longest Line:\n" + lines.reduce( (prev, current) ->
                prev.split(" ").length > current.split(" ").length ? prev : current));

        System.out.println("Done");
    }
}
