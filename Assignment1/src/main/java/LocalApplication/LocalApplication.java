package LocalApplication;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import SQS.SimpleQueueService;
import SQS.newTaskMessage;

public class LocalApplication {

    public static void main(String[] args) throws Exception {
    	
        AmazonEC2 ec2;
        AWSCredentials credentials = new PropertiesCredentials(LocalApplication.class.getResourceAsStream("AwsCredentials.properties"));
        ec2 = new AmazonEC2Client(credentials);

        
        /*	************** Checks if a Manager node is active on the EC2 cloud. Case not active, start the manager node **************	*/
        
        
		System.out.println("Checking if Manager instance exists...");
        try {
        	DescribeInstancesRequest listingRequest = new DescribeInstancesRequest();
        	List<String> valuesT1 = new ArrayList<String>();
        	valuesT1.add("0");
        	Filter filter1 = new Filter("tag:Manager", valuesT1);
        	DescribeInstancesResult result = ec2.describeInstances(listingRequest.withFilters(filter1));
        	List<Reservation> reservations = result.getReservations();
        	
        	if(reservations.size() == 0){				// Manager instace does not exist, create 'Manager' tagged instance
				System.out.println("Manager instance not found\nCreating Manager...");
				RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1);
				request.setInstanceType(InstanceType.T1Micro.toString());
				List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
				String instanceId = instances.get(0).getInstanceId();
				CreateTagsRequest tagRequest = new CreateTagsRequest();
				tagRequest = tagRequest.withResources(instanceId).withTags(new Tag("Manager", "0"));
				ec2.createTags(tagRequest);
				System.out.println("Launch instances: " + instances);
        	}
        	else{
        		System.out.println("Manager instance found: " + reservations);
        	}
 
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
        
        
        /*	************** Create S3 bucket and upload input file into it **************	*/
        
        
        AmazonS3 s3 = new AmazonS3Client(credentials);
        String directoryName = args[0];
        String bucketName = credentials.getAWSAccessKeyId() + "_" + directoryName.replace('\\', '_').replace('/','_').replace(':', '_');
        String key = null;
 
        try {
            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);
            
            // Uploading input file to bucket:
            System.out.println("Uploading input file to bucket " + bucketName);
            File inputFile = new File("inputFile.txt");
            key = inputFile.getName().replace('\\', '_').replace('/','_').replace(':', '_');
            PutObjectRequest req = new PutObjectRequest(bucketName, key, inputFile);
            s3.putObject(req);
            
        } catch (AmazonServiceException ase) {
        	
            System.out.println("Caught an AmazonServiceException,request made it to Amazon S3, but was rejected with an error response.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        	
            System.out.println("Caught an AmazonClientException, client encountered an internal problem while trying to communicate with S3");
            System.out.println("Error Message: " + ace.getMessage());
        }
        
        
        /*	************** Create Local Application <--> Manager 'newTask|Termination' and 'doneTask' queues **************	*/
        /* 'newTask|Termination' queue - Messages from Local Application to Manager
         * 'doneTask' queue - Messages from Manager  to Local Application
         */
        
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));
        System.out.println("Creating a Local Application <--> Manager 'newTask|Termination' SQS queue");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("newTaskQueue_" + UUID.randomUUID());
        String newTaskQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        
        System.out.println("Creating a Local Application <--> Manager 'doneTask' SQS queue");
        createQueueRequest = new CreateQueueRequest("doneTaskQueue_" + UUID.randomUUID());
        String doneTaskQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        
        
        
        /*	************** Sending a message to SQS queue, stating the location of the file on S3 **************	*/
        
        
        System.out.println("Sending 'new task' message to 'newTask' queue");
		SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(newTaskQueueURL).withMessageBody("newTask");
		send_msg_request.addMessageAttributesEntry("inputFileLocation", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
		sqs.sendMessage(send_msg_request);
        
        
        
        /*	************** Delete S3 bucket **************	*/
        
        System.out.println("Deleting bucket " + bucketName + "\n");
        s3.deleteBucket(bucketName);
    }
}
