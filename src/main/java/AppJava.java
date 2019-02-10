

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import java.net.URISyntaxException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

public class AppJava {
	private static final String METEO_DATA_PATH = "meteo_data.json.gz";
	private static final String GPS_COUNTRY_CITY_PATH = "gps_country_city.csv";
	public static void main(String[] args) throws URISyntaxException {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("meteo_data").getOrCreate();
		//read data from JSON file
		Dataset<Row> firstTable = spark.read().json(AppJava.class.getClassLoader().getResource(METEO_DATA_PATH).getPath());
		firstTable = firstTable.select(explode(firstTable.col("data")));
		firstTable = firstTable.select(firstTable.col("col").getField("date").as("date"),
				firstTable.col("col").getField("long").as("longX"), firstTable.col("col").getField("lat").as("latX"),
				firstTable.col("col").getField("tC").as("temperature"));
		firstTable.show(10, false);
		
		// average temperature grouped by year
		Dataset<Row> averageGroupedByYear = firstTable.groupBy(functions.year(firstTable.col("date"))).avg("temperature").select(col("avg(temperature)"));
		averageGroupedByYear.show();
		//read data from CSV file
		Dataset<Row> secondTable = spark.read().option("header", "true").csv(AppJava.class.getClassLoader().getResource(GPS_COUNTRY_CITY_PATH).getPath());
		secondTable = secondTable.select(col("Longitude").cast(DataTypes.DoubleType).as("longY"), col("Latitude").as("latY").cast(DataTypes.DoubleType), col("Country").as("country"),
				col("City").as("city"));
		// cross join two tables as we don't have precise location coordinates from the second table 
		Dataset<Row> joined = firstTable.crossJoin(secondTable);
		//calculate minimum distance between points from both tables, so we can figure out location
		spark.sqlContext().udf().register("calculateDistance", calculateDistance, DataTypes.DoubleType);
		joined = joined.select(functions
				.callUDF("calculateDistance", col("latX"), col("longX"), col("latY"), col("longY")).as("distance"),
				col("date"), col("latX"), col("longX"), col("country"), col("city"), col("temperature"));
		// for each point from the first table find a minimum distance, so we can identify a city nearest to sensor coordinates
		Dataset<Row> grouped = joined.groupBy(col("latX"), col("longX")).min("distance");
		Dataset<Row> resultSet = joined.join(grouped)
				.where(joined.col("latX").equalTo(grouped.col("latX"))
						.and(joined.col("longX").equalTo(grouped.col("longX"))
								.and(joined.col("distance").equalTo(grouped.col("min(distance)"))))).select(joined.col("latX"), joined.col("longX"), joined.col("date"), joined.col("country"), joined.col("city"), joined.col("temperature"));
		resultSet.show(10, false);
		// average temperature in Germany in year 2011
		Dataset<Row> filtered = resultSet
				.filter(resultSet.col("country").equalTo("Germany").and(functions.year(col("date")).equalTo("2011")));
		filtered.agg(functions.avg("temperature")).show();
		
		
	}

	private static UDF4<Double, Double, Double, Double, Double> calculateDistance = (latX, longX, latY, longY) -> Math
			.hypot(latX - Double.valueOf(latY), longX - Double.valueOf(longY));
}
