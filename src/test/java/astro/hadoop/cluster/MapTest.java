package astro.hadoop.cluster;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import healpix.essentials.HealpixBase;
import healpix.essentials.Pointing;

/**
 * Tests for Mapper. Takes several configurations for the points and tests
 * whether these are being placed correctly by the isInBorderArea function of
 * the mapper class.
 * 
 * @author caucau
 *
 */
@RunWith(Parameterized.class)
public class MapTest {

	private Map mapper;
	private Double inputRa;
	private Double inputDec;
	private Boolean expectedResult;

	@Before
	public void setUp() throws Exception {
		mapper = new Map();
	}

	public MapTest(Double ra, Double dec, Boolean expected) {
		this.inputRa = ra;
		this.inputDec = dec;
		this.expectedResult = expected;
	}

	@Parameterized.Parameters
	public static Collection coordinates() {
		return Arrays.asList(new Object[][] { { 13.1581441435464, -72.8003076548797, true },
				{ 13.1564393115068, -72.799209882416, false }, { 13.1605558685683, -72.7994491054888, false },
				{ 13.1558740647926, -72.7995433386488, true }, { 13.161998046998, -72.8010997554935, true }

		});
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testIsInBorderArea() throws Exception {
		Pointing pointing = Map.getPointingFromCoords(inputRa, inputDec);
		System.out.println("Coords should be in border: " + inputRa + ", " + inputDec + " " + expectedResult);
		Long healpixID = Map.hBase.ang2pix(pointing);

		assertEquals(expectedResult, Map.isInBorderArea(pointing, healpixID));
	}

}
