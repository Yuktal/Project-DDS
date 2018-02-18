import static org.junit.Assert.*;

import org.junit.*;

/**
 * Created by nikoo28 on 4/12/17.
 */
public class HelperFunctionsTest {

  @Test
  public void latitudeCordinateToIndex() throws Exception {

    assertEquals(25, HelperFunctions.latitudeCordinateToIndex(40.750110626220703));
    assertEquals(48, HelperFunctions.latitudeCordinateToIndex(40.98734));

//    System.out.println(HelperFunctions.xCordinateToLatitude(25));
//    System.out.println(HelperFunctions.xCordinateToLatitude(48));
  }

  @Test
  public void longitudeCordinateToIndex() throws Exception {

    assertEquals(30, HelperFunctions.longitudeCordinateToIndex(-73.993896484375));
    assertEquals(0, HelperFunctions.longitudeCordinateToIndex(-73.7));
    assertEquals(55, HelperFunctions.longitudeCordinateToIndex(-74.25));

//    System.out.println(HelperFunctions.yCordinateToLongitude(29));
//    System.out.println(HelperFunctions.yCordinateToLongitude(55));
  }

}