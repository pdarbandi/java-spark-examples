package sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkDStreamStreamingDemo {

    public static void main(String[] args) {
        SparkConf sparkConf;

        if(Arrays.asList(args).contains("--cluster")) {
            sparkConf = new SparkConf();
        }
        else {
            sparkConf = new SparkConf().setMaster("local[*]")
                                       .setAppName("streaming-app");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                                                             Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch(InterruptedException ex) {
            System.out.println("Exiting");
        }
    }
}
