
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import java.net.URISyntaxException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

public class MeteoDataProcessing {
	private static final String METEO_DATA_PATH = "meteo_data.json.gz";
	private static final String GPS_COUNTRY_CITY_PATH = "gps_country_city.csv";

	public static void main(String[] args) throws URISyntaxException {
		SparkSession spark = SparkSession.builder().master("local[2]").appName("meteo_data_processing").getOrCreate();
		Dataset<Row> meteoData = loadMeteoData(spark);
		meteoData.show(10, false);
		showAverageTemperatureGroupedByYear(meteoData);
		Dataset<Row> citiesLocation = loadCitiesLocation(spark);
		// cross join two tables as we don't have precise location coordinates from the
		// second table
		Dataset<Row> joined = meteoData.crossJoin(citiesLocation);
		// register udf in sqlContext
		spark.sqlContext().udf().register("calculateDistance", calculateDistance, DataTypes.DoubleType);
		// calculate distance between sensors and cities coordinates
		joined = joined.select(
				functions.callUDF("calculateDistance", col("latX"), col("longX"), col("latY"), col("longY"))
						.as("distance"),
				col("date"), col("latX"), col("longX"), col("country"), col("city"), col("temperature"));
		// for each point of sensor find a minimum distance
		Dataset<Row> sensorCoordinatesWithMinimumDistance = joined.groupBy(col("latX"), col("longX")).min("distance");
		// identify city nearest to sensor coordinates
		Dataset<Row> resultSet = joined.join(sensorCoordinatesWithMinimumDistance).where(joined.col("latX")
				.equalTo(sensorCoordinatesWithMinimumDistance.col("latX"))
				.and(joined.col("longX").equalTo(sensorCoordinatesWithMinimumDistance.col("longX")).and(
						joined.col("distance").equalTo(sensorCoordinatesWithMinimumDistance.col("min(distance)")))));
		//show samples of joined data
		resultSet.select(col("latX"), col("longX"), col("date"), col("country"), col("city"), col("temperature"))
				.show(10, false);
		showAverageTemperatureInGermanyIn2011(resultSet);
	}

	private static Dataset<Row> loadMeteoData(SparkSession spark) {
		Dataset<Row> firstTable = spark.read()
				.json(MeteoDataProcessing.class.getClassLoader().getResource(METEO_DATA_PATH).getPath());
		firstTable = firstTable.select(explode(firstTable.col("data")));
		firstTable = firstTable.select(firstTable.col("col").getField("date").as("date"),
				firstTable.col("col").getField("long").as("longX"), firstTable.col("col").getField("lat").as("latX"),
				firstTable.col("col").getField("tC").as("temperature"));
		return firstTable;
	}

	private static void showAverageTemperatureGroupedByYear(Dataset<Row> dataset) {
		Dataset<Row> averageGroupedByYear = dataset.groupBy(functions.year(dataset.col("date"))).avg("temperature")
				.select(col("avg(temperature)"));
		averageGroupedByYear.show();
	}

	private static Dataset<Row> loadCitiesLocation(SparkSession spark) {
		Dataset<Row> secondTable = spark.read().option("header", "true")
				.csv(MeteoDataProcessing.class.getClassLoader().getResource(GPS_COUNTRY_CITY_PATH).getPath());
		secondTable = secondTable.select(col("Longitude").cast(DataTypes.DoubleType).as("longY"),
				col("Latitude").as("latY").cast(DataTypes.DoubleType), col("Country").as("country"),
				col("City").as("city"));
		return secondTable;
	}

	private static UDF4<Double, Double, Double, Double, Double> calculateDistance = (latX, longX, latY, longY) -> Math
			.hypot(latX - latY, longX - longY);

	private static void showAverageTemperatureInGermanyIn2011(Dataset<Row> resultSet) {
		Dataset<Row> filtered = resultSet
				.filter(resultSet.col("country").equalTo("Germany").and(functions.year(col("date")).equalTo("2011")));
		filtered.agg(functions.avg("temperature")).show();
	}

}