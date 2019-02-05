package bdEvent;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.glassfish.hk2.api.ServiceLocator;

import scala.reflect.api.Trees.SelectExtractor;

public class AppJava {

    public static void main(String[] args) {
    	SparkSession spark = SparkSession.builder().master("local[*]").appName("event").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//        JavaRDD<String> inputFile = sc.textFile("src/main/resources/meteo_data.json.gz");
        Dataset<Row> json = spark.read().json("src/main/resources/meteo_data.json.gz");
//        JavaRDD<String> flatMap = inputFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//        List<String> take = flatMap.take(10);
//        System.out.println(take.get(0));
//		json.select(col("data")).show(10, false);
		Dataset<Row> select = json.select(explode(col("data")));//.withColumn("col", col(""));
		select.select(select.col("col").getField("date"), select.col("col").getField("long").as("longtitude"), select.col("col").getField("lat").as("latitude"), select.col("col").getField("tC").as("temp")).printSchema();
//		select.col("col").as(")
		select.printSchema();
		select.show(10, false);
//		select.groupBy(year(col("col.date"))).avg("col.tC").show();
//		select.select(explode(col("long"))).select("long");
//		select.show(10, false);
		
//        JavaRDD<String> inputFile = sc.textFile();
//		System.out.println(inputFile.first());
//		 Dataset<Row> ds = spark.read().json(inputFile);
//		 ds.createOrReplaceGlobalTempView("view");
//		 ds.select("data");
		 
//		 inputFile.DataTypes.createArrayType(DataType.fromJson(inputFile.first())).;
		 
//		 List<Row> collectAsList = ds.select("data").collectAsList();
//		 System.out.println(collectAsList.get(0).get(0));
//		 Object take = ds.take(10);
//		 long count = ds.select("data").count();
//		 System.out.println(count);//.as(Encoders.bean(Input.class));
//		 ds.printSchema();
		 sc.stop();
//		 ds.groupBy("year").agg(avg.col("temp"));
		 
    }
}
