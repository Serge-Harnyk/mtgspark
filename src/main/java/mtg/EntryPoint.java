package mtg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EntryPoint {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[4]").getOrCreate();
        Dataset<Row> mtgCards = spark.read().json("/Users/sharnyk/Projects/mtganalit/AllCardsSplitted.json");
        mtgCards.printSchema();

        mtgCards.createOrReplaceTempView("mtgdata");


        spark.sql("SELECT count(*), toughness FROM mtgdata WHERE array_contains(types, 'Creature') group by toughness ORDER BY toughness").show(100);
        spark.stop();
    }
}
