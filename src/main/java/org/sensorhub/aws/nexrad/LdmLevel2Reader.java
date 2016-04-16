package org.sensorhub.aws.nexrad;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

/**
 * <p>Title: LdmLevel2Reader.java</p>
 * <p>Description: </p>
 *
 * @author T
 * @date Mar 9, 2016
 */
public class LdmLevel2Reader {

	//	S3Object s3object;
	byte [] b2 = new byte[2];
	byte [] b4 = new byte[4];

	public LdmLevel2Reader() {
	}

	public List<LdmRadial> read(File f) throws FileNotFoundException, IOException {
		String key = f.getName();
		try(InputStream is = new FileInputStream(f)) {
			if(key.endsWith("S")) {
				VolumeHeader hdr = readVolumeHeader(is);
				System.err.println(hdr);
				readMetadataRecord(is);
			} else if (key.endsWith("I")) {
				List<LdmRadial> radials = readMessage31(is);
				return radials;
				
			} else if (key.endsWith("E")) {
//				readE(is);
			}
		}
		
		return null;
	}

	public VolumeHeader readVolumeHeader(InputStream is) throws IOException {
		VolumeHeader hdr = new VolumeHeader();
		byte [] fn = new byte[12];
		int ok = is.read(fn);
		hdr.archive2filename = new String(fn, StandardCharsets.UTF_8);
		ok = is.read(b4);
		hdr.daysSince1970 = java.nio.ByteBuffer.wrap(b4).getInt();
		ok = is.read(b4);
		hdr.msSinceMidnight = java.nio.ByteBuffer.wrap(b4).getInt();
		ok = is.read(b4);
		hdr.siteId = new String(b4, StandardCharsets.UTF_8);
		return hdr;
	}

	public void readMessage2(InputStream is, int msgSize) throws IOException {
		byte [] bfull = new byte[msgSize];
		int ok = is.read(bfull);
		System.err.println("Read bytes: " + bfull.length);	
		byte[]bt = new byte[2356];
		is.read(bt);
		
//		int i=0;
//		while(true) {
//			int b = is.read();
//			String s = (b >= 32 && b < 125) ? (char)b + "" : "*";
//			System.err.println((i++) + ":  " + s + "  ==  " + Integer.toHexString(b));
//			if(b == -1) break;
//		}
	}
	
	public void readE(InputStream is) throws IOException {
//		dumpBytes(is);

		int ok = is.read(b2);
		ok = is.read(b2);
		short msgSize = java.nio.ByteBuffer.wrap(b2).getShort(); 
		System.err.println("Message31 size: " + msgSize);
//
		byte [] bfull = new byte[100];
		ok = is.read(bfull);

		ByteArrayInputStream bas = new ByteArrayInputStream(bfull);
		int radCnt = 1;
		try (BZip2CompressorInputStream bzis = new BZip2CompressorInputStream(bas) ) {
			dumpBytes(bzis);
		
		}
	}
	
	
	public List<LdmRadial> readMessage31(InputStream is) throws IOException {
		int ok = is.read(b4);
		int msgSize = java.nio.ByteBuffer.wrap(b4).getInt(); 
		System.err.println("Message31 size: " + msgSize);

		byte [] bfull = new byte[msgSize];
		ok = is.read(bfull);
//		System.err.println("Read bytes: " + bfull.length);
		
		List<LdmRadial> ldmRadials = new ArrayList<>();
		
		ByteArrayInputStream bas = new ByteArrayInputStream(bfull);
		int radCnt = 1;
		try (BZip2CompressorInputStream bzis = new BZip2CompressorInputStream(bas) ) {
			while(true) {

				MessageHeader msgHdr = readMessageHeader(bzis);
				if(msgHdr == null)  break;
				switch(msgHdr.messageType) {
				case 2:
					readMessage2(bzis, msgHdr.messageSize);
//					DataHeader dataHdr = readDataHeaderBlock(bzis);
//					VolumeDataBlock volumeBlock = readVolumeDataBlock(bzis);
//					readElevationDataBlock(bzis);
//					readRadialDataBlock(bzis);
//					for(int i=0; i<dataHdr.dataBlockCount - 3; i++) {
//
//						MomentDataBlock reflBlock = readMomentDataBlock(bzis);
//						System.err.println(radCnt + " + radials read. " + reflBlock.blockName);
//
//					}
//					radCnt++;
					break;
				case 31:
					LdmRadial ldmRadial = new LdmRadial();
					ldmRadial.dataHeader = readDataHeaderBlock(bzis);
					ldmRadial.volumeDataBlock = readVolumeDataBlock(bzis);
					readElevationDataBlock(bzis);
					readRadialDataBlock(bzis);
					for(int i=0; i<ldmRadial.dataHeader.dataBlockCount - 3; i++) {

						MomentDataBlock momentBlock = readMomentDataBlock(bzis);
						ldmRadial.momentData.add(momentBlock);
					}
					ldmRadials.add(ldmRadial);
					radCnt++;
//					System.err.println(radCnt + " + radials read. " );

					break;
				default:
					throw new IOException("Unknown MessageType = " + msgHdr.messageType);
				}
			}
			//			while(true) {
			//				int b = bzis.read();
			//				String s = (b >= 32 && b < 125) ? (char)b + "" : "*";
			//				System.err.println((i++) + ":  " + s + "  ==  " + Integer.toHexString(b));
			//				if(b == -1) break;
			//			}
		}
		
		return ldmRadials;
	}

	public MessageHeader readMessageHeader(InputStream is) throws IOException {
		MessageHeader hdr = new MessageHeader();

		byte [] b12 = new  byte[12];
		int ok = is.read(b12);  // skip 12 unused bytes
		if(ok == -1)  return null;

		ok = is.read(b2);
		hdr.messageSize = java.nio.ByteBuffer.wrap(b2).getShort();

		hdr.rdaByte = is.read();  
		hdr.messageType = is.read();

//		System.err.println("MsgType: " + hdr.messageType);
		
		ok = is.read(b2);  // id seqNum
		hdr.sequenceNum = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		hdr.daysSince1970 = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b4);
		hdr.msSinceMidnight = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b2);
		hdr.numSegments = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		hdr.segmentNum = java.nio.ByteBuffer.wrap(b2).getShort();

		return hdr;
	}

	public DataHeader readDataHeaderBlock(InputStream is) throws IOException {
		DataHeader hdr = new DataHeader();

		int ok = is.read(b4);
		hdr.siteId = new String(b4, StandardCharsets.UTF_8);

		ok = is.read(b4);
		hdr.msSinceMidnight = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b2);
		hdr.daysSince1970 = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		hdr.azimuthNum = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b4);
		hdr.azimuthAngle = java.nio.ByteBuffer.wrap(b4).getFloat();

		hdr.compression = is.read();
		is.read();  // spare byte

		ok = is.read(b2);
		hdr.radialLength = java.nio.ByteBuffer.wrap(b2).getShort();

		hdr.azimuthResolutionSpacing = is.read();  // Uncompressed length of the radial in bytes including the Data Header block length
		hdr.radialStatus = is.read();
		hdr.elevationNum = is.read();
		hdr.cutStatusNum = is.read();

		ok = is.read(b4);
		hdr.elevationAngle = java.nio.ByteBuffer.wrap(b4).getFloat();

		hdr.radialSpotBlankingStatus = is.read();
		hdr.azimuthIndexingMode = is.read();

		ok = is.read(b2);
		hdr.dataBlockCount = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b4);
		hdr.volumeBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.elevationBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.radialBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.reflectivityBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.velocityBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.spectrumWidthBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.zdrBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.phiBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		ok = is.read(b4);
		hdr.rhoBlockPointer = java.nio.ByteBuffer.wrap(b4).getInt();

		return hdr;
	}

	public VolumeDataBlock readVolumeDataBlock(InputStream is) throws IOException {
		VolumeDataBlock block = new VolumeDataBlock();

		int ok = is.read(b4);
		block.dataName = new String(b4, StandardCharsets.UTF_8);

		ok = is.read(b2);
		block.blockSize = java.nio.ByteBuffer.wrap(b2).getShort();

		block.majorVersionNum = is.read();
		block.minorVersionNum = is.read();

		ok = is.read(b4);
		block.latitude = java.nio.ByteBuffer.wrap(b4).getFloat();

		ok = is.read(b4);
		block.longitude = java.nio.ByteBuffer.wrap(b4).getFloat();

		ok = is.read(b2);
		block.siteHeightAboveSeaLevelMeters = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		block.feedhornHeightAboveGroundMeters = java.nio.ByteBuffer.wrap(b2).getShort();

		//  TODO - skipping some fields that we will prbbly never use
		for(int i=0; i<5; i++)
			ok = is.read(b4);

		ok = is.read(b2);
		block.volumeCoveragePattern = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		block.processingStatus = java.nio.ByteBuffer.wrap(b2).getShort();

		return block;
	}

	public ElevationDataBlock readElevationDataBlock(InputStream is) throws IOException {
		//  TODO - add if needed
		byte [] b12 = new byte [12];
		int ok = is.read(b12);

		return null;
	}

	public ElevationDataBlock readRadialDataBlock(InputStream is) throws IOException {
		//  TODO - add if needed
		byte [] b28 = new byte [28];
		int ok = is.read(b28);

		return null;
	}

	public MomentDataBlock readMomentDataBlock(InputStream is) throws IOException {
		MomentDataBlock block = new MomentDataBlock();

		block.blockType = (char)is.read();

		byte [] b3 = new byte[3];
		int ok = is.read(b3);
		block.blockName = new String(b3, StandardCharsets.UTF_8);

		is.read(b4);  // Reserved bytes

		ok = is.read(b2);
		block.numGates = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		block.rangeToCenterOfFirstGate = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		block.rangeSampleInterval = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		block.rangeFoldingThreshold = java.nio.ByteBuffer.wrap(b2).getShort();

		ok = is.read(b2);
		block.snrThreshold = java.nio.ByteBuffer.wrap(b2).getShort();

		block.controlFlags = is.read();
		block.gateSizeBits = is.read();
		assert block.gateSizeBits == 8 || block.gateSizeBits == 16;

		ok = is.read(b4);
		block.scale = java.nio.ByteBuffer.wrap(b4).getFloat();

		ok = is.read(b4);
		block.offset = java.nio.ByteBuffer.wrap(b4).getFloat();

		int gateSizeBytes = block.gateSizeBits / 8;
		block.bdata = new byte[block.numGates * gateSizeBytes];
		ok = is.read(block.bdata);

		return block;
	}

	//	public void countBytes(S3ObjectInputStream is) throws IOException { 
	public void countBytes(InputStream is) throws IOException { 
		int cnt = 0;
		while(true) {
			int b = is.read();
			if(b == -1)  break;
			cnt++;
		}
		System.err.println("Bytes counted: " + cnt);
	}
	
	public void dumpBytes(InputStream is ) throws IOException {
		int i=0;
		while(true) {
			int b = is.read();
			String s = (b >= 32 && b < 125) ? (char)b + "" : "*";
			System.err.println((i++) + ":  " + s + "  ==  " + Integer.toHexString(b));
			if(b == -1) break;
		}
	}

	public void readMetadataRecord(InputStream is) throws IOException { 
		int ok = is.read(b4);
		int recSize = Math.abs(java.nio.ByteBuffer.wrap(b4).getInt());
		byte[] bfull = new byte[recSize];
		ok = is.read(bfull);
		//		long l = s3object.getObjectMetadata().getInstanceLength();
		//		byte [] b = new byte[(int)l];
		//		int ok = is.read(b);
		try (BZip2CompressorInputStream bzis = new BZip2CompressorInputStream(new ByteArrayInputStream(bfull));) {
			int cnt = 0;
			for(int i=0; i<134; i++){
				byte [] bb = new byte[2432];
				ok = bzis.read(bb);
			}
			System.err.println(cnt + " compressed records read");
			countBytes(bzis);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void dumpChunkToFile(InputStream is, Path pout) throws IOException {
		try(FileOutputStream os = new FileOutputStream(pout.toFile())) {
			int cnt = 0;
			while(true) {
				int b = is.read();
				if(b == -1)  break;
				os.write(b);
				cnt++;
			}
//			System.err.println("Bytes counted: " + cnt);
		}
	}

	public static void main_(String[] args) throws IOException {
		LdmLevel2Reader r = new LdmLevel2Reader();
		File dir = new File("C:/Data/sensorhub/Level2/test");
		File [] files = dir.listFiles();
		for(File f: files) {
			if (f.getName().endsWith("I")) {
				System.err.println("reading " + f.getName());
				r.read(f);
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		LdmLevel2Reader reader = new LdmLevel2Reader();
		String p = "C:/Data/sensorhub/Level2/test/KSJT/KSJT_847_20160414-212848-001-S";
		List<LdmRadial> rads = reader.read(new File(p));
		for(LdmRadial r: rads)
			System.err.println(rads.size());

	}
}