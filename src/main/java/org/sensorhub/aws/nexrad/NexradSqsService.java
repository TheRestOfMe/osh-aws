package org.sensorhub.aws.nexrad;

import java.util.ArrayList;
import java.util.List;

import org.sensorhub.aws.sqs.AwsSqsService;
import org.sensorhub.aws.sqs.QueueFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ExecutorService;
import edu.emory.mathcs.backport.java.util.concurrent.Executors;

/**
 * <p>Title: NexradSqsService.java</p>
 * <p>Description: </p>
 *
 * @author T
 * @date Apr 15, 2016
 */
public class NexradSqsService
{
	// allow to be configurable
	static final int NUM_THREADS = 5;
	static final String topicArn = "arn:aws:sns:us-east-1:684042711724:NewNEXRADLevel2Object";
	private String queueName;
	private String site;
	
	public NexradSqsService(String site) {
		this.site = site;
		queueName = "NexradQueue" + site;
	}
	
	public void start() {
		assert site.length() == 4;
		
		String queueUrl  = QueueFactory.createAndSubscribeQueue(topicArn, queueName);
		
		ExecutorService execService = Executors.newFixedThreadPool(NUM_THREADS);
		AwsSqsService sqsService = new AwsSqsService(queueUrl);
		List<String> sites = new ArrayList<>();
		sites.add(site);

		for(int i=0; i<NUM_THREADS; i++) {
			execService.execute(new ProcessMessageThread(sqsService, sites));
		}

		execService.shutdown();
	}
	
	public void stop() {
		QueueFactory.deleteQueue(queueName);
	}

}
