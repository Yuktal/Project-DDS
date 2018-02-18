/**
 * Created by nikoo28 on 4/11/17.
 */
public class Constants {

  public static final double LATITUDE_UPPER = 40.50;
  public static final double LATITUDE_LOWER = 40.90;
  public static final double LONGITUDE_LEFT = -73.70;
  public static final double LONGITUDE_RIGHT = -74.25;

  public static final int LATITUDE_LOWER_BOUNDARY = (int)(LATITUDE_UPPER * 100);
  public static final int LATITUDE_UPPER_BOUNDARY = (int)(LATITUDE_LOWER * 100);
  public static final int LONGITUDE_LEFT_BOUNDARY = (int)(LONGITUDE_LEFT * -100);
  public static final int LONGITUDE_RIGHT_BOUNDARY = (int)(LONGITUDE_RIGHT * -100);
  public static final int NUMBER_OF_DAYS = 31;

  public static final int NEIGHBORS_ON_CORNERS = 7;
  public static final int NEIGHBORS_ON_EDGE = 11;
  public static final int NEIGHBORS_ON_FACE = 17;
  public static final int NEIGHBORS_ON_INSIDE = 26;

  public static final String SEPARATOR = "%";
  public static final String ROW_SPLITTER = ",";
  public static final String TIME_SPLITTER = " ";
  public static final String DATE_SEPARATOR = "-";
  public static final String OUTLIER = "Outlier";

  public static final String HEADER = "Ve";

  public static final int INDEX_DATE = 1;
  public static final int INDEX_LONGITUDE = 5;
  public static final int INDEX_LATITUDE = 6;


}
