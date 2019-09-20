package mtg;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class EntryPoint {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[4]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> mtgCards = spark.read().json("/Users/sharnyk/Projects/MTG/mtganalit/src/main/resources/data/AllCardsCutted.json");
        mtgCards.printSchema();

        mtgCards.createOrReplaceTempView("mtgdata");


        Dataset<Row> ctExploded = spark.sql("SELECT name, explode(subtypes) as creatureType FROM mtgdata WHERE array_contains(types, 'Creature')");
        System.out.println(ctExploded.count());
        ctExploded.createOrReplaceTempView("exCr");

        ctExploded.show();

        spark.sql("SELECT creatureType, COUNT(*) as c FROM exCr group by creatureType ORDER by c desc").show(100);

        spark.sql("SELECT t.name, concat(t.creatureType, ' ', t2.creatureType) as types FROM exCr as t JOIN exCr as t2 ON t.name = t2.name " +
                "WHERE t.creatureType == 'Elf' AND t2.creatureType != 'Elf'")
//                .show();
                .createOrReplaceTempView("e2");
        spark.sql("SELECT COUNT(*) as C, types FROM e2 GROUP BY types ORDER BY C DESC").show(100);

        spark.sql("SELECT count(*) as c, subtypes FROM mtgdata WHERE size(subtypes) = 1 and array_contains(types, 'Creature')" +
                " group by subtypes ORDER BY c desc ").show(100);



        spark.stop();
    }

    private void generateSchema(Dataset<Row> mtgCards) {
        StructType my_schema = mtgCards.schema();
        String columns = Arrays.stream(my_schema.fields())
                .map(field -> field.name()+" "+field.dataType().typeName())
                .collect(Collectors.joining(","));

        System.out.println("create table my_table(" + columns + ") ");
    }
}
