package sparkdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkDataframeCSVDemo_SQL {

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

        // Note how there is no sparkContext here - we use the sparkSession directly
        Dataset<Row> airports = sparkSession.read().option("header", true).csv("data/airports.csv");

        // How to know the inferred schema of the csv file and display the first few rows?
        airports.printSchema();
        airports.show(5);

        // How to show the number of rows with country="Papua New Guinea"?

        // OPTION A) - Create a View and call SQL on the sparkSession
        airports.createOrReplaceTempView("airports");
        sparkSession.sql("SELECT COUNT(*) FROM airports WHERE Country = \"Papua New Guinea\"").show();

        // OPTION B) - Using the API
        airports.filter("Country = \"Papua New Guinea\"").count();

        // How to print the list of distinct countries
        // Equivalent to: airports.select("Country").distinct().sort("Country").show();
        sparkSession.sql("SELECT DISTINCT(Country) FROM airports ORDER BY Country").show();

        // Countries with the word New
        sparkSession.sql("SELECT * FROM airports WHERE Country LIKE \"%New%\"").show();

        // cast latitude and longitude to Double
        // then filter by -23 < latitude < 23
        // these are airports in the tropics - display the results
        Dataset<Row> tropAirports = sparkSession.sql("SELECT * FROM airports WHERE Double(latitude) > -23 AND Double(latitude) < 23");

        tropAirports.show();

        tropAirports.createOrReplaceTempView("tropAirports");

        // Group tropical airports by timezone and show how many in each zone
        sparkSession.sql("SELECT timezoneOlsonFormat, COUNT(*) from tropAirports GROUP BY timezoneOlsonFormat").show();
        //airports.groupBy("timezoneOlsonFormat").count().show();

        // the list of tropical airports could be saved to any supported store - e.g. parquet on hdfs
    }
}
