package org.sensorhub.aws.sqs;

import java.util.ArrayList;
import java.util.List;

import org.sensorhub.aws.nexrad.ProcessMessageThread;

import edu.emory.mathcs.backport.java.util.concurrent.ExecutorService;
import edu.emory.mathcs.backport.java.util.concurrent.Executors;

/**
 * <p>Title: SqsTest.java</p>
 * <p>Description: </p>
 *
 * @author T
 * @date Mar 2, 2016
 */
public class SqsTest {
	static final int NUM_THREADS = 5;
	public static void main(String[] args) {
		String topicArn = "arn:aws:sns:us-east-1:684042711724:NewNEXRADLevel2Object";
		//		String queueUrl = QueueFactory.createAndSubscribeQueue(topicArn, "NexradDynamicQueue");
		//		String queueUrl = "https://sqs.us-east-1.amazonaws.com/633354997535/QueueThis";
		//		String queueUrl = "https://sqs.us-west-2.amazonaws.com/384286541835/NexradDynamicQueue";
		String queueUrl  = QueueFactory.createAndSubscribeQueue(topicArn, "NexradDynamicQueueAMA");

		
		ExecutorService execService = Executors.newFixedThreadPool(NUM_THREADS);
		AwsSqsService sqsService = new AwsSqsService(queueUrl);
		List<String> sites = new ArrayList<>();
		sites.add("KAMA");
//		sites.add("KDGX");
//		sites.add("KGWX");

		for(int i=0; i<NUM_THREADS; i++) {
			execService.execute(new ProcessMessageThread(sqsService, sites));
		}

		execService.shutdown();
	}
}
