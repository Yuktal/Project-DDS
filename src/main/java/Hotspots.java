import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by nikoo28 on 4/10/17.
 */
public class Hotspots implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Hotspots.class);

  public static void main(String[] args) {

    checkArgument(args.length >= 2,
        "Please provide the path of input file as first parameter and output file as second parameter");

    new Hotspots().run(args);
  }

  private void run(String[] runtimeArguments) {

    SparkConf sparkConf = new SparkConf()
        .setAppName(Hotspots.class.getName())
        .set("spark.driver.memory", "4500m")
        .set("spark.driver.cores", "2")
        .set("spark.driver.maxResultSize", "2g")
        .set("spark.executor.memory", "1500m")
        .set("spark.eventLog.enabled", "true");
    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

    try {
      findTop50Hotspots(javaSparkContext, runtimeArguments[0], runtimeArguments[1]);
    } catch (Exception e) {
      e.printStackTrace();
    }

    LOGGER.info("Over");
    TearDown(javaSparkContext);

  }

  private void findTop50Hotspots(JavaSparkContext javaSparkContext,
                                 String inputFilePath, String outputFilePath) throws FileNotFoundException {

    long start = System.currentTimeMillis();

    JavaRDD<String> inputTextFile = javaSparkContext.textFile(inputFilePath);
    PrintWriter printWriter = new PrintWriter(new File(outputFilePath));

    LOGGER.info("Reading CSV from: " + inputFilePath);

    JavaPairRDD<String, Integer> pairs = inputTextFile.mapToPair(new PairFunction<String, String, Integer>() {

      public Tuple2<String, Integer> call(String s) throws Exception {

        // Drop header
        if (s.startsWith(Constants.HEADER))
          return new Tuple2<String, Integer>(Constants.OUTLIER, 1);

        String[] tripData = s.split(Constants.ROW_SPLITTER);

        Double xCordinate = Double.parseDouble(tripData[Constants.INDEX_LATITUDE]);
        Double yCordinate = Double.parseDouble(tripData[Constants.INDEX_LONGITUDE]);

        // Removing the points outside of box
        if (!(xCordinate >= Constants.LATITUDE_UPPER && xCordinate <= Constants.LATITUDE_LOWER &&
            yCordinate >= Constants.LONGITUDE_RIGHT && yCordinate <= Constants.LONGITUDE_LEFT)) {
          return new Tuple2<String, Integer>(Constants.OUTLIER, 1);
        }

        int x = HelperFunctions.latitudeCordinateToIndex(xCordinate);
        int y = HelperFunctions.longitudeCordinateToIndex(yCordinate);
        int z = Integer.parseInt(tripData[Constants.INDEX_DATE].
            split(Constants.TIME_SPLITTER)[0].
            split(Constants.DATE_SEPARATOR)[2]) - 1;

        return new Tuple2<String, Integer>(z + Constants.SEPARATOR + x + Constants.SEPARATOR + y, 1);
      }
    });

    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });

    List<Tuple2<String, Integer>> data = counts.collect();
//    System.out.println("DATA SIZE = " + data.size());

    int[][][] spaceTimeCube = new int[Constants.NUMBER_OF_DAYS]
        [Constants.LATITUDE_UPPER_BOUNDARY - Constants.LATITUDE_LOWER_BOUNDARY + 1]
        [Constants.LONGITUDE_RIGHT_BOUNDARY - Constants.LONGITUDE_LEFT_BOUNDARY + 1];


    populateSpaceTimeCube(spaceTimeCube, data);
    long totalNumberOfCells = spaceTimeCube.length *
        spaceTimeCube[0].length *
        spaceTimeCube[0][0].length;
    LOGGER.info("Total number of cells in space time cube: " + totalNumberOfCells);

    // Get total Eligible points in the rectangle
    long totalEligiblePoints = HelperFunctions.getTotalEligiblePoints(spaceTimeCube);
    LOGGER.info("Total geo spatial points under consideration: " + totalEligiblePoints);

    double mean = (double) totalEligiblePoints / (double) totalNumberOfCells;
    LOGGER.info("Value of mean: " + mean);

    double standardDeviation = HelperFunctions.getStandardDeviation(spaceTimeCube, totalNumberOfCells, mean);
    LOGGER.info("Value of S: " + standardDeviation);

    Map<String, Double> coOrdinateZScoreMap = new HashMap<String, Double>();
    populateCoOrdinateZScoreMap(coOrdinateZScoreMap, spaceTimeCube, totalEligiblePoints, mean, standardDeviation);

    // Sort the map in reverse order
    Map<String, Double> reverseSortedCoOrdinateZScoreMap = HelperFunctions.sortByValue(coOrdinateZScoreMap);

    // Output the top 50 hotspots in file
    writeHotspotsToFile(reverseSortedCoOrdinateZScoreMap, printWriter);

    LOGGER.info("Top 50 Hotspots available at: " + outputFilePath);

    LOGGER.info("Over in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
  }

  private void populateSpaceTimeCube(int[][][] cube, List<Tuple2<String, Integer>> dataReducedByKey) {

    for (Tuple2<String, Integer> aDataReducedByKey : dataReducedByKey) {

      // Ignore the outliers
      if (aDataReducedByKey._1().equals(Constants.OUTLIER))
        continue;

      String[] splits = aDataReducedByKey._1.split(Constants.SEPARATOR);

      int zIndex = Integer.parseInt(splits[0]);
      int xIndex = Integer.parseInt(splits[1]);
      int yIndex = Integer.parseInt(splits[2]);

      // zIndex - time
      cube[zIndex][xIndex][yIndex] += aDataReducedByKey._2;
    }
  }

  private void populateCoOrdinateZScoreMap(Map<String, Double> coOrdinateZScoreMap, int[][][] spaceTimeCube,
                                           long totalEligiblePoints, double mean, double standardDeviation) {

    for (int time_step = 0; time_step < spaceTimeCube.length; time_step++) {

      for (int cell_x = 0; cell_x < spaceTimeCube[0].length; cell_x++) {

        for (int cell_y = 0; cell_y < spaceTimeCube[0][0].length; cell_y++) {

          double getisOrdStatistic = HelperFunctions.getGetisOrdStatistic(time_step, cell_x, cell_y,
              spaceTimeCube, mean, standardDeviation, totalEligiblePoints);

          StringBuilder spaceTime = new StringBuilder();
          String latitude = Double.toString(HelperFunctions.xCordinateToLatitude(cell_x));
          String longitude = Double.toString(HelperFunctions.yCordinateToLongitude(cell_y));
          spaceTime.append(latitude);
          spaceTime.append(",");
          spaceTime.append(longitude);
          spaceTime.append(",");
          spaceTime.append(time_step);

          coOrdinateZScoreMap.put(spaceTime.toString(), getisOrdStatistic);
        }
      }
    }
  }

  private void writeHotspotsToFile(Map<String, Double> reverseSortedCoOrdinateZScoreMap,
                                   PrintWriter printWriter) {

    int lines = 0;
    StringBuilder output = new StringBuilder();
    for (Map.Entry<String, Double> stringDoubleEntry : reverseSortedCoOrdinateZScoreMap.entrySet()) {

      output.append(stringDoubleEntry.getKey());
      output.append(",");
      output.append(stringDoubleEntry.getValue().toString());
      output.append("\n");

      lines++;
      if (lines == 50)
        break;
    }

    printWriter.write(output.toString());
    printWriter.close();
  }

  private void TearDown(JavaSparkContext javaSparkContext) {
    javaSparkContext.stop();
  }

}
