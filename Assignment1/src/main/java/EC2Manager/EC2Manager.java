package EC2Manager;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import LocalApplication.LocalApplication;
import S3.S3;
import SQS.SimpleQueueService;

public class EC2Manager {

	public static void main(String[] args) throws IOException {

        AWSCredentials credentials = new PropertiesCredentials(EC2Manager.class.getResourceAsStream("AwsCredentials.properties"));
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        int numOfWorkers = 0;
        String localApp_Manager_QueueURL = null;
		
        
        /*	************** Check Local Application <--> Manager queue for messages **************	
    	************** If received a message from local application, act accordingly **************	*/
        
        
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));	// Declare SQS client
        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
        	String queueName = queueUrl.split("\\_")[0];			// Get queue name from beginning of queue URL up to '_' delimeter
            if (queueName == "localAppQueue"){
            	localApp_Manager_QueueURL = sqs.getQueueUrl(queueName).getQueueUrl();	// Get queue URL by queue name
            }
        	System.out.println("QueueUrl: " + queueUrl);
        }
        
        
        /*	************** Loop until termination message from local application **************	*/
        
        while(true){
        	System.out.println("Receiving messages from " + localApp_Manager_QueueURL + " SQS queue\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(localApp_Manager_QueueURL);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
                System.out.println("  *** Message ***");
                System.out.println("    MessageId:     " + message.getMessageId());
                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    System.out.println("  Attribute");
                    System.out.println("    Name:  " + entry.getKey());
                    System.out.println("    Value: " + entry.getValue());
                }
                System.out.println();
                if (message.getBody() == "newTask"){				// Received a "newTask" message from local application
                	
                	continue;
                }
                if (message.getBody() == "Terminate"){				// Received a termination message from local application
                	break;
                }
            }
        }
        
        
        		
        

        
        		
        /*	************** Get input file from S3 Storage **************	*/
        
        
        AmazonS3 s3 = new AmazonS3Client(credentials);
		System.out.println("Downloading input file from S3 storage");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));		// Provide object information with GetObjectRequest
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        //displayTextInputStream(object.getObjectContent());
        InputStream inp = object.getObjectContent();								// Write input file contents to input stream
        
        
        /*	************** Create Worker instances **************	*/
        
        
        try {
			System.out.println("Creating Worker instance...");
			RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1);
			request.setInstanceType(InstanceType.T1Micro.toString());
			
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			String instanceId = instances.get(0).getInstanceId();
			
			CreateTagsRequest tagRequest = new CreateTagsRequest();
			tagRequest = tagRequest.withResources(instanceId).withTags(new Tag("Worker", Integer.toString(numOfWorkers)));		// Set tag of a worker and a number value
			ec2.createTags(tagRequest);
			numOfWorkers++;
			  
			System.out.println("Launch instances: " + instances);
		} 
        catch (AmazonServiceException ase) {
	        System.out.println("Caught Exception: " + ase.getMessage());
	        System.out.println("Reponse Status Code: " + ase.getStatusCode());
	        System.out.println("Error Code: " + ase.getErrorCode());
	        System.out.println("Request ID: " + ase.getRequestId());
        }
        
        
        /*	************** Check for termination message  **************	*/
        
        
	}

}
