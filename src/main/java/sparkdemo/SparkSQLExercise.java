package sparkdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Properties;

public class SparkSQLExercise {

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
        String jdbcQuery = "select * from city";
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


        // 1. What did we just read in?
        // - check the schema - display the first few rows

        // 2. Experiment with filtering

        // 3. Get the average city Population per country?

        // 4. Optional - read countries table, and left join the average city population

        // 5. Optional - build and run on cluster
    }
}