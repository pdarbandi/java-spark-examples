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

        // Note how there is no sparkContext here - we use the sparkSession directly
        Dataset<Row> airports = sparkSession.read().option("header", true).csv("data/airports.csv");

        // How to know the inferred schema of the csv file and display the first few rows?
        airports.printSchema();
        airports.show(5);

        // How to show the number of rows with null in iataCode?
        System.out.println("PNG:" + airports.filter("Country = \"Papua New Guinea\"").count() );

        // How to print the list of distinct countries
        airports.select("Country").distinct().sort("Country").show();

        // Using SQL
        System.out.println("---- Using SQL to Query ----");
        airports.createOrReplaceTempView("airports");
        sparkSession.sql("SELECT distinct(Country) from airports order by Country").show();


        //airports.sqlContext().sql("select * from table where Country like %New%");
        // cast latitude and longitude to Double
        // then filter by -23 < latitude < 23
        // these are airports in the tropics - display the results

        // Group tropical airports by timezone and show how many in each zone

        airports.groupBy("timezoneOlsonFormat").count().show();

        // the list of tropical airports could be saved to any supported store - e.g. parquet on hdfs
    }
}
