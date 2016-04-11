package org.sensorhub.aws.nexrad;

/**
 * <p>Title: DataBlockHeader.java</p>
 * <p>Description: </p>
 *
 * @author T
 * @date Mar 16, 2016
 */
public class DataHeader {
	public String siteId;
	public int msSinceMidnight;
	public short daysSince1970;
	public short azimuthNum;
	public float azimuthAngle;
	public int compression;
	public short radialLength;
	public int azimuthResolutionSpacing;
	public int radialStatus;
	public int elevationNum;
	public int cutStatusNum;
	public float elevationAngle;
	public int radialSpotBlankingStatus;
	public int azimuthIndexingMode;
	public short dataBlockCount;
	public int volumeBlockPointer;
	public int elevationBlockPointer;
	public int radialBlockPointer;
	public int reflectivityBlockPointer;
	public int velocityBlockPointer;
	public int spectrumWidthBlockPointer;
	public int zdrBlockPointer;
	public int phiBlockPointer;
	public int rhoBlockPointer;
}	
