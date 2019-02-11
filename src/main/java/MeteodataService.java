import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

public class MeteodataService {
	private static final String METEO_DATA_PATH = "meteo_data.json.gz";
	private static final String GPS_COUNTRY_CITY_PATH = "gps_country_city.csv";
	private static final String UDF_NAME = "calculateDistance";
	private static final String DATA = "data";
	private static final String LATITUDE = "Latitude";
	private static final String LAT = "lat";
	private static final String LONGTITUDE = "Longitude";
	private static final String LONG = "lat";
	private static final String LAT_X = "latX";
	private static final String LONG_X = "longX";
	private static final String LAT_Y = "latY";
	private static final String LONG_Y = "longY";
	private static final String DISTANCE = "distance";
	private static final String DATE = "date";
	private static final String COUNTRY = "country";
	private static final String CITY = "city";
	private static final String TEMPERATURE = "temperature";
	private static final String COL = "col";
	private static final String TC = "tC";
	private static final String HEADER_KEY = "header";
	private static final String YEAR_2011 = "2011";
	private static final String GERMANY = "Germany";

	private final SparkSession sparkSession;

	public MeteodataService(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	public void process() {
		Dataset<Row> meteoData = loadMeteoData();
		meteoData.show(10, false);
		getAverageTemperatureGroupedByYear(meteoData).show();
		Dataset<Row> citiesLocation = loadCitiesLocation();
		// cross join two tables as we don't have precise location coordinates from the
		// second table
		Dataset<Row> joined = meteoData.crossJoin(citiesLocation);
		registerUdf(calculateDistance, UDF_NAME);
		// calculate distance between sensors and cities coordinates
		joined = joined
				.select(functions.callUDF(UDF_NAME, col(LAT_X), col(LONG_X), col(LAT_Y), col(LONG_Y)).as(DISTANCE),
						col(DATE), col(LAT_X), col(LONG_X), col(COUNTRY), col(CITY), col(TEMPERATURE))
				.cache();
		// for each point of sensor find a minimum distance
		Dataset<Row> sensorCoordinatesWithMinimumDistance = joined.groupBy(col(LAT_X), col(LONG_X)).min(DISTANCE);
		// identify city nearest to sensor coordinates
		Dataset<Row> resultSet = joined
				.join(sensorCoordinatesWithMinimumDistance,
						joined.col(LAT_X).equalTo(sensorCoordinatesWithMinimumDistance.col(LAT_X))
								.and(joined.col(LONG_X).equalTo(sensorCoordinatesWithMinimumDistance.col(LONG_X))
										.and(joined.col(DISTANCE)
												.equalTo(sensorCoordinatesWithMinimumDistance.col("min(distance)")))))
				.select(joined.col(LAT_X), joined.col(LONG_X), col(DATE), col(COUNTRY), col(CITY), col(TEMPERATURE));
		resultSet.show(10, false);
		getAverageTemperatureInGermanyIn2011(resultSet).show();
	}

	private UDF4<Double, Double, Double, Double, Double> calculateDistance = (latX, longX, latY, longY) -> Math
			.hypot(latX - latY, longX - longY);

	private void registerUdf(UDF4<Double, Double, Double, Double, Double> udf, String udfName) {
		sparkSession.sqlContext().udf().register(udfName, udf, DataTypes.DoubleType);
	}

	private Dataset<Row> loadMeteoData() {
		Dataset<Row> dataset = sparkSession.read()
				.json(AppJava.class.getClassLoader().getResource(METEO_DATA_PATH).getPath());
		dataset = dataset.select(functions.explode(dataset.col(DATA)));
		dataset = dataset.select(dataset.col(COL).getField(DATE).as(DATE), dataset.col(COL).getField(LONG).as(LONG_X),
				dataset.col(COL).getField(LAT).as(LAT_X), dataset.col(COL).getField(TC).as(TEMPERATURE));
		return dataset;
	}

	private Dataset<Row> getAverageTemperatureGroupedByYear(Dataset<Row> dataset) {
		return dataset.groupBy(functions.year(dataset.col(DATE))).avg(TEMPERATURE).select(col("avg(temperature)"));
	}

	private Dataset<Row> loadCitiesLocation() {
		Dataset<Row> dataset = sparkSession.read().option(HEADER_KEY, Boolean.TRUE.toString())
				.csv(AppJava.class.getClassLoader().getResource(GPS_COUNTRY_CITY_PATH).getPath());
		dataset = dataset.select(col(LONGTITUDE).cast(DataTypes.DoubleType).as(LONG_Y),
				col(LATITUDE).as(LAT_Y).cast(DataTypes.DoubleType), col("Country").as(COUNTRY), col("City").as(CITY));
		return dataset;
	}

	private Dataset<Row> getAverageTemperatureInGermanyIn2011(Dataset<Row> dataset) {
		Dataset<Row> filtered = dataset
				.filter(dataset.col(COUNTRY).equalTo(GERMANY).and(functions.year(col(DATE)).equalTo(YEAR_2011)));
		return filtered.agg(functions.avg(TEMPERATURE));
	}

}