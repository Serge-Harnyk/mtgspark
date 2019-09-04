package mtg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EntryPoint {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[4]").getOrCreate();
        Dataset<String> mtgCards = spark.read().json("/Users/sharnyk/Projects/mtganalit/AllCards.json").toJSON();

//        mtgCards.select(mtgCards.col("Forest")).show();

        mtgCards.printSchema();

        System.out.println("Count: " + mtgCards.count());
        spark.stop();
    }
}
