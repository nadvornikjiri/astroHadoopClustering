package astro.hadoop.cluster;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import healpix.essentials.HealpixBase;
import healpix.essentials.Pointing;
import healpix.essentials.Scheme;

public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

	public static HealpixBase hBase;
	public static HealpixBase hBaseBorder;

	static {
		try {
			hBase = new HealpixBase((long) Math.pow(2,17), Scheme.NESTED);
			hBaseBorder = new HealpixBase((long) Math.pow(2,20), Scheme.NESTED);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {

			String line = value.toString();
			String[] lineElems = line.split(",");
			
			Double ra = Double.parseDouble(lineElems[0]);
			Double dec = Double.parseDouble(lineElems[1]);
			String obsID = lineElems[2];
			String starNo = lineElems[3];

			Pointing healpixPointing = getPointingFromCoords(ra, dec);
			Long healpixID = hBase.ang2pix(healpixPointing);
			if (!isInBorderArea(healpixPointing, healpixID)) {
				context.write(new LongWritable(healpixID), new Text(obsID + starNo) );
			}

		} catch (Exception e) {
			System.err.println("Snapp, Something went wrong." );
			e.printStackTrace();
		}

	}

	/**
	 * Takes the Healpix base for the clusters and the resolution for declaring
	 * borders of clusters from the static variables and tells whether the healpixID
	 * parameter lies within the border range. If not, it lies within the center of
	 * the pixel.
	 * 
	 * @param pointBigPixID
	 * @return
	 * @throws Exception
	 */
	public static boolean isInBorderArea(Pointing point, Long pointBigPixID) throws Exception {
		long pointSmallPixID = hBaseBorder.ang2pix(point);
		long[] smallNeighbourPixIDs = hBaseBorder.neighbours(pointSmallPixID);

		for (long neighbourSmallPixelID : smallNeighbourPixIDs) {
			long neighbourbigPixelID = hBase.ang2pix(hBaseBorder.pix2ang(neighbourSmallPixelID));
			if (neighbourbigPixelID != pointBigPixID) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Transforms right ascension and declination doubles to Healpix Pointing.
	 * @param ra
	 * @param dec
	 * @return Pointing
	 */
	public static Pointing getPointingFromCoords(Double ra, Double dec) {
		Pointing healpixPointing = new Pointing();
		healpixPointing.theta = (90 - dec) * Math.PI / 180;
		healpixPointing.phi = ra * Math.PI / 180;

		return healpixPointing;
	}

}
