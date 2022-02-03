package sparkdemo;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

public class SparkStructuredStreamingDemo {

    public static void main(String[] args) {
        SparkSession sparkSession;

        if(Arrays.asList(args).contains("--cluster")) {
            sparkSession = SparkSession.builder().getOrCreate();
        }
        else {
            sparkSession = SparkSession.builder()
                    .master("local[*]")
                    .getOrCreate();
        }

        Dataset<Row> socketDF = sparkSession.readStream()
                                            .format("socket")
                                            .option("host", "localhost")
                                            .option("port", 9999)
                                            .load();

        socketDF.printSchema();
        Dataset<Row> wordsDF = socketDF.select(explode(split(socketDF.col("value")," ")).as("word"));

        wordsDF.printSchema();
        Dataset<Row> countDF = wordsDF.groupBy("word").count();

        countDF.printSchema();

        try {

            StreamingQuery query = countDF.writeStream()
                    .outputMode("complete")
                    .format("console")
                    .start();

            query.awaitTermination();
//// WORKS - ISH
////            wordsDF.writeStream().format("csv")
////                            .option("format", "append")
////                            .option("path", "data/stream/csv")
////                            .option("checkpointLocation", "data/stream/checkpoints")
////                            .outputMode("append")
////                            .start()
////                            .awaitTermination();
//
//            countDF.writeStream().outputMode("complete").foreachBatch((row, id) -> {
//                System.out.println("ID: " + id);
//                System.out.println(row.count());
//                System.out.println(row);
//            }).start().awaitTermination();
//
        } catch(TimeoutException ex) {
            System.out.println("Timeout: " + ex);
        } catch(StreamingQueryException ex) {
            System.out.println("Streaming Exception: " + ex);
        }

    }
}
