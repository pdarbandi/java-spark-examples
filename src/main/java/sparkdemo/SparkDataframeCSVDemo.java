package sparkdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkDataframeCSVDemo {

    public static void main(String[] args) {
        SparkSession sparkSession;

        if(Arrays.asList(args).contains("--cluster")) {
            sparkSession = SparkSession.builder().getOrCreate();
        }
        else {
            sparkSession = SparkSession.builder()
                                       .master("local[*]")
                                       .config("spark.hadoop.validateOutputSpecs", false)
                                       .getOrCreate();
        }

        Dataset<Row> airports = sparkSession.read().option("header", true).csv("data/airports.csv");

        // prints object info only
        //System.out.println(airports.take(5));

        airports.printSchema();
        airports.show(5);

        System.out.println("Empty IATA CODES: " + airports.filter( "iataCode = null").count());

        System.out.println("Distinct Countries: ");
        airports.select("Country").distinct().show();

        airports.col("latitude").cast("Double").as("latitude");
        airports.col("longitude").cast("Double").as("longitude");

        Dataset<Row> tropAirports = airports.filter("latitude > -23 AND latitude < 23");

        tropAirports.show();

        System.out.println("Trop Airports By Timezone");
        tropAirports.groupBy("timezoneOlsonFormat").count().show();

        tropAirports.write().parquet("data/tropAirports.parquet");
        airports.write().parquet("data/airports.parquet");
    }
}
