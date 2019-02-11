import org.apache.spark.sql.SparkSession;

public class AppJava {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[2]").appName("meteo_data_processing").getOrCreate();
		MeteodataService service = new MeteodataService(spark);
		service.process();
	}
}