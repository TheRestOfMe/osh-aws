package org.sensorhub.aws.sqs;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

/**
 * <p>Title: AmazonSqsService.java</p>
 * <p>Description: </p>
 *
 * @author T
 * @date Mar 2, 2016
 */
public class AwsSqsService {

	private AmazonSQS sqs;
	AWSCredentials credentials;
//	private static final String QUEUE_URL = 	"https://sqs.us-east-1.amazonaws.com/633354997535/NexradRealtimeQueue";
	private String queueUrl; 
	
	public AwsSqsService(String queueUrl) {
		credentials = new ProfileCredentialsProvider().getCredentials();
		sqs = new AmazonSQSClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);
		sqs.setEndpoint("sdb.amazonaws.com");
		this.queueUrl = queueUrl;
	}
	
	
	public List<Message> receiveMessages() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl).
				withWaitTimeSeconds(0).withMaxNumberOfMessages(10);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		return messages;
	}
	
	public void deleteMessage(Message msg) {
		sqs.deleteMessage(new DeleteMessageRequest(queueUrl, msg.getReceiptHandle()));
	}
}
