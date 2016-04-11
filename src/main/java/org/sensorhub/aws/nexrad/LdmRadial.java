package org.sensorhub.aws.nexrad;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title: LdmRadisl.java</p>
 * <p>Description: </p>
 *
 * @author T
 * @date Apr 6, 2016
 */
public class LdmRadial
{	
	public DataHeader dataHeader;
	public VolumeDataBlock volumeDataBlock;
	public List<MomentDataBlock> momentData = new ArrayList<>();
	
}
