package bdEvent;

import java.io.Serializable;

import org.codehaus.jackson.annotate.JsonProperty;

public class Event implements Serializable {
	private static final long serialVersionUID = 1L;
	private String data;
	private Double lat;
	@JsonProperty("long")
	private Double longtitude;
	private Double tC;

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public Double getLat() {
		return lat;
	}

	public void setLat(Double lat) {
		this.lat = lat;
	}

	public Double getLongtitude() {
		return longtitude;
	}

	public void setLongtitude(Double longtitude) {
		this.longtitude = longtitude;
	}

	public Double gettC() {
		return tC;
	}

	public void settC(Double tC) {
		this.tC = tC;
	}

}
