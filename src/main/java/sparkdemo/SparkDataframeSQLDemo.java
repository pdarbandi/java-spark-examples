package sparkdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Properties;

public class SparkDataframeSQLDemo {

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

        // just some variable - these would come from configuration or command line arguments
        String jdbcQuery = "select * from country where Continent like \"North%\"";
        String username = "root";
        String password = "c0nygre";
        String jdbcUrl = "jdbc:mysql://" + username + "@msbigdata17.conygre.com:3306/world";


        // Option 1. Using the more verbose "load" API
        Dataset<Row> countries = sparkSession.read()
                                    .format("jdbc")
                                    .option("driver", "com.mysql.cj.jdbc.Driver")
                                    .option("url", jdbcUrl)
                                    .option("username", username)
                                    .option("password", password)
                                    .option("query", jdbcQuery)
                                    .load();

        // Option 2. An alternative is to use read().jdbc() - here we read the entire table
        Properties jdbcProps = new Properties();
        jdbcProps.put("password", password);
//        Dataset<Row> countries = sparkSession.read()
//                                .jdbc(jdbcUrl, "country", jdbcProps);


        // What did we just read in?
        // - check the schema - display the first few rows
        countries.show(5);

        // Experiment with filtering - e.g. which country has the greatest LifeExpectancy?

        // What's the average GNP per Region?
        Dataset<Row> regionalGnp = countries.groupBy("region").avg("GNP");
        regionalGnp.show();

//        regionalGnp.write()
//                .mode(SaveMode.ErrorIfExists)
//                .option("driver", "com.mysql.cj.jdbc.Driver")
//                .option("url", jdbcUrl)
//                .option("username", username)
//                .option("password", password)
//                .jdbc(jdbcUrl, "regionalGnp", jdbcProps);

        Dataset<Row> airports = sparkSession.read().option("header", true).csv("data/airports.csv");

        Dataset<Row> trimmedCountries = countries.select("Name", "Continent", "SurfaceArea");
        Dataset<Row> joined = airports.join(trimmedCountries, airports.col("Country").equalTo(trimmedCountries.col("Name")));

        joined.show();
    }
}
