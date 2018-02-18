import java.util.*;

/**
 * Created by nikoo28 on 4/10/17.
 */
public class HelperFunctions {

  public static final double ACCURACY_MEASURE = 0.01;

  public static int latitudeCordinateToIndex(Double cordinate) {

    return (getCeilOrFloor(cordinate) - Constants.LATITUDE_LOWER_BOUNDARY);
  }

  public static double xCordinateToLatitude(int xIndex) {

    return round((Constants.LATITUDE_LOWER_BOUNDARY + xIndex) * ACCURACY_MEASURE, 2);
  }

  public static int longitudeCordinateToIndex(Double cordinate) {

    return (getCeilOrFloor(cordinate) - Constants.LONGITUDE_LEFT_BOUNDARY);
  }

  public static double yCordinateToLongitude(int yIndex) {

    return round((Constants.LONGITUDE_LEFT_BOUNDARY + yIndex) * ACCURACY_MEASURE * -1, 2);
  }

  public static double round(double value, int places) {
    if (places < 0) throw new IllegalArgumentException();

    long factor = (long) Math.pow(10, places);
    value = value * factor;
    long tmp = Math.round(value);
    return (double) tmp / factor;
  }

  public static long getTotalEligiblePoints(int[][][] spaceTimeCube) {

    long sum = 0;
    for (int[][] ints : spaceTimeCube) {
      for (int[] anInt : ints) {
        for (int i : anInt) {
          sum += i;
        }
      }
    }

    return sum;
  }

  public static double getStandardDeviation(int[][][] spaceTimeCube, long totalNumberOfCells, double xBar) {

    long summationSquare = 0;
    for (int[][] ints : spaceTimeCube) {
      for (int[] anInt : ints) {
        for (int i : anInt) {
          summationSquare += Math.pow(i, 2.0);
        }
      }
    }

    double operandOne = (double) summationSquare / (double) totalNumberOfCells;
    double operandTwo = Math.pow(xBar, 2.0);
    double squaredSValue = operandOne - operandTwo;

    return Math.sqrt(squaredSValue);
  }

  private static int getCeilOrFloor(Double cordinate) {

    if (cordinate < 0) {
      cordinate = Math.abs(cordinate);
      return (int) Math.ceil(cordinate / ACCURACY_MEASURE);
    } else
      return (int) Math.floor(cordinate / ACCURACY_MEASURE);
  }

  public static double getGetisOrdStatistic(int zIndex, int xIndex, int yIndex, int[][][] spaceTimeCube,
                                            double mean, double standardDeviation, long totalEligiblePoints) {

    double numerator = getNumerator(zIndex, xIndex, yIndex, spaceTimeCube, mean);
    double denominator = getDenominator(zIndex, xIndex, yIndex, spaceTimeCube,
        standardDeviation, totalEligiblePoints);

    return numerator / denominator;
  }

  private static double getNumerator(int zIndex, int xIndex, int yIndex,
                                     int[][][] spaceTimeCube, double mean) {

    double operandOne = getWeightedSumOfNeighbors(zIndex, xIndex, yIndex, spaceTimeCube);
    double operandTwo = mean * findNumberOfNeighbors(zIndex, xIndex, yIndex, spaceTimeCube);

    return operandOne - operandTwo;
  }

  private static double getWeightedSumOfNeighbors(int i, int j, int k, int[][][] cube) {

    double sum = 0;
    int n1 = cube.length;
    int n2 = cube[0].length;
    int n3 = cube[0][0].length;
    for (int i1 = -1; i1 <= 1; ++i1) {
      for (int i2 = -1; i2 <= 1; ++i2) {
        for (int i3 = -1; i3 <= 1; ++i3) {

          int x1 = i + i1;
          int x2 = j + i2;
          int x3 = k + i3;

          if (x1 >= 0 && x1 < n1 &&
              x2 >= 0 && x2 < n2 &&
              x3 >= 0 && x3 < n3) {

            sum += cube[x1][x2][x3];
          }
        }
      }
    }

    return sum;
  }

  private static double getDenominator(int zIndex, int xIndex, int yIndex, int[][][] spaceTimeCube,
                                       double standardDeviation, long totalEligiblePoints) {

    int neighbors = findNumberOfNeighbors(zIndex, xIndex, yIndex, spaceTimeCube);
    double operandOne = totalEligiblePoints * neighbors;
    double operandTwo = Math.pow((double) neighbors, 2.0);
    double intermediateResult = operandOne - operandTwo;
    double squaredValue = intermediateResult / (double) totalEligiblePoints - 1;
    double squareRootValue = Math.sqrt(squaredValue);

    return standardDeviation * squareRootValue;
  }

  private static int findNumberOfNeighbors(int i, int j, int k, int[][][] cube) {

    int terminalIndexes = 0;
    int n1 = cube.length;
    int n2 = cube[0].length;
    int n3 = cube[0][0].length;

    if (i == 0 || i == n1 - 1)
      terminalIndexes++;
    if (j == 0 || j == n2 - 1)
      terminalIndexes++;
    if (k == 0 || k == n3 - 1)
      terminalIndexes++;

    if (terminalIndexes == 1)
      return Constants.NEIGHBORS_ON_FACE;
    if (terminalIndexes == 2)
      return Constants.NEIGHBORS_ON_EDGE;
    if (terminalIndexes == 3)
      return Constants.NEIGHBORS_ON_CORNERS;
    else
      return Constants.NEIGHBORS_ON_INSIDE;
  }

  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {

    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return -1 * (o1.getValue()).compareTo(o2.getValue());
      }
    });

    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
