package bdEvent;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppJava {

    public static void main(String[] args) {
    	SparkSession spark = SparkSession.builder().master("local[*]").appName("event").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> inputFile = sc.textFile("src/main/resources/meteo_data.json.gz");
		 Dataset<Row> ds = spark.read().json(inputFile);
		 Dataset<Input> dsData = ds.select("data").as(Encoders.bean(Input.class));
		 dsData.printSchema();
		 
    }
}
