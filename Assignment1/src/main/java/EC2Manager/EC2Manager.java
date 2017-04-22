package EC2Manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.sun.org.apache.xml.internal.utils.URI;

import LocalApplication.LocalApplication;
import S3.S3;
import SQS.SimpleQueueService;
import SQS.newTaskMessage;

public class EC2Manager {

	public static void main(String[] args) throws IOException {

        AWSCredentials credentials = new PropertiesCredentials(EC2Manager.class.getResourceAsStream("AwsCredentials.properties"));
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);	// EC2 client
        AmazonS3 s3 = new AmazonS3Client(credentials);		// S3 Storage client
        AmazonSQS sqs = new AmazonSQSClient(credentials);	// SQS client
        boolean terminateSignal = false;					// Termination signal indicator
        boolean newPDFTaskQueueExists = false;				// Boolean flag to indicate if need to create a new 'newPDFTask' queue
        boolean donePDFTaskQueueExists = false;				// Boolean flag to indicate if need to create a new 'donePDFTask' queue
        int numOfWorkers = 0;
        int numOfPDFperWorker = 10;					// An integer stating how many PDF files per worker, as determined by user
        int numOfMessages = 0;						// SQS message counter
        String newPDFTaskQueueURL = null;			// 'newPDFTask|Termination' Manager <--> Workers queue
        String donePDFTaskQueueURL = null;			// 'donePDFTask' Manager <--> Workers queue
        String newTaskQueueURL = null;				// 'newTask|Termination' Local Application <--> Manager queue
        String doneTaskQueueURL = null;				// 'doneTask' Local Application <--> Manager queue
        String inputFileBucket = null;				// S3 bucket of the input file with a list of PDF URLs and operations to perform	
        String inputFileKey = null;					// Key of the input file in the S3 Storage
        String newFileURL = null;					// URL of a new image file as given by Worker
        String operation = null;					// The operation performed by Worker for a given PDF URL
        String outputBucketName = "outputmanagerbucketamityoav";		// Manager's output files bucket name 
        String messageReceiptHandle = null;			// Message receipt handle, required when deleting queue messages
        //List<inputFile> inputFilesList = new ArrayList<inputFile>();		// Initialize empty list of input files
        Map<String, inputFile> inputFilesMap = new HashMap<String, inputFile>();
        Map<String, List<String>> outputFilesMap = new HashMap<String, List<String>>();		// Map from corresponding input file, to List of finished tasks (which we will write in the end to a summary output file)
       
		
        
        /*	************** Get 'newTask|Termination' and 'doneTask' Local Application <--> Manager SQS queues ************** */	
        

        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
        	URI uri = new URI(queueUrl);
        	String path = uri.getPath();
        	String queueName = path.substring(path.lastIndexOf('/') + 1); 
        	System.out.println("queuename: " + queueName);
            if (queueName.equals("newTaskQueue")){
            	newTaskQueueURL = queueUrl;
            }
            else if (queueName.equals("doneTaskQueue")){
            	doneTaskQueueURL = queueUrl;
            }
            else if(queueName.equals("newPDFTaskQueue")){
            	newPDFTaskQueueExists = true;
            	newPDFTaskQueueURL = queueUrl;
            }
            else if(queueName.equals("donePDFTaskQueue")){
            	donePDFTaskQueueExists = true;
            	donePDFTaskQueueURL = queueUrl;
            }
        	System.out.println("QueueUrl: " + queueUrl);
        }
        
   
        
        /*	************** Set 'newPDFTask|WorkerTermination' and 'donePDFTask' Manager <--> Workers SQS queues if doesn't exist ************** */	
        
        
        if (!newPDFTaskQueueExists){
        	 System.out.println("Creating a Manager <--> Workers 'newPDFTask|WorkerTermination' SQS queue");
             // CreateQueueRequest createQueueRequest = new CreateQueueRequest("newPDFTaskQueue" + UUID.randomUUID());
             CreateQueueRequest createQueueRequest = new CreateQueueRequest("newPDFTaskQueue");
             newPDFTaskQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        }
        if (!donePDFTaskQueueExists){
        	System.out.println("Creating a Manager <--> Workers 'donePDFTask' SQS queue");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("donePDFTaskQueue");
            donePDFTaskQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        }
        
        
        /*	************** Loop until termination message from local application **************	*/
        
        SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(newTaskQueueURL).withMessageBody("newTask");
        send_msg_request.addMessageAttributesEntry("inputFileBucket", new MessageAttributeValue().withDataType("String").withStringValue("inputbucketamitelran"));
        send_msg_request.addMessageAttributesEntry("inputFileKey", new MessageAttributeValue().withDataType("String").withStringValue("input.txt"));
		sqs.sendMessage(send_msg_request);
       
        
        while(true){
        	
        	/* ************** If terminated by Local App & All Workers finished their tasks **************
        	 * - Terminate all Workers
        	 * - Delete Manager <--> Workers queues
        	 * - Generate last output files
        	 * - Terminate Manager instance
        	 */
        	
        	if ((numOfMessages == 0) && (terminateSignal)){	
        		terminateWorkers(ec2);
        		System.out.println("Deleting queue: " + newPDFTaskQueueURL);
                sqs.deleteQueue(new DeleteQueueRequest(newPDFTaskQueueURL));
                System.out.println("Deleting queue: " + donePDFTaskQueueURL);
                sqs.deleteQueue(new DeleteQueueRequest(donePDFTaskQueueURL));
                terminateManager(ec2);
        		return;
        	}
        	
        	System.out.println("\nReceiving messages from " + newTaskQueueURL + " SQS queue\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(newTaskQueueURL).withMessageAttributeNames("All");
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
            	printMessage(message);
                
                /*	************** Received a "newTask" message from local application **************
                 *  - Download file from S3 Storage
                 *  - Count number of files lines, create worker per 'numOfPDFperWorker' integer
                 *  - Process the input file lines and send each line as a 'newPDFTask' message to Workers queue
              	*/
                
                if ((message.getBody().equals("newTask")) && (!terminateSignal)){				// Received a "newTask" message from local application (while not signaled to terminate)
                	for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                		if (entry.getKey().equals("inputFileBucket")){							// Get the input file bucket location
                			inputFileBucket = entry.getValue().getStringValue();
                			System.out.println(inputFileBucket);
                		}
                		if (entry.getKey().equals("inputFileKey")){								// Get the input file key
                			inputFileKey = entry.getValue().getStringValue();
                			System.out.println(inputFileKey);
                		}
                    }
                	System.out.println("\nDownloading input file object from S3 storage...\n");
                    S3Object inputFileObj = s3.getObject(new GetObjectRequest(inputFileBucket, inputFileKey));
                    System.out.println("Content-Type: "  + inputFileObj.getObjectMetadata().getContentType());
                    System.out.println();
                    outputFilesMap.put(inputFileKey, new ArrayList<String>());									// Add output file lines list to Map
                    int numOfTasks = sendNewPDFTask(inputFileObj, sqs, newPDFTaskQueueURL, inputFileKey);		// Send input file tasks to workers 
                    numOfMessages += numOfTasks;																// Send new PDF task to queue and increment 'newPDFTask' queue message counter
                    inputFilesMap.put(inputFileKey, new inputFile(inputFileBucket, inputFileKey, numOfTasks));		// Insert input file data to Map
                    
                    messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(newTaskQueueURL, messageReceiptHandle));		// Delete 'newTask' message from 'newTaskQueue'
                    /*
                    if (numOfWorkers == 0){								// Create a single worker to avoid dividing by zero
                    	createWorker(ec2, numOfWorkers);
                    	numOfWorkers++;
                    }
                    
                    // Stabilize ratio of <worker per 'n' number of messages> by creating new workers (not terminating running workers)
                    while ((numOfMessages / numOfWorkers) > numOfPDFperWorker){
                    	createWorker(ec2, numOfWorkers);
                    	numOfWorkers++;
                    }
                    */
                    SendMessageRequest send_msg_request1 = new SendMessageRequest().withQueueUrl(newTaskQueueURL).withMessageBody("Terminate");
            		sqs.sendMessage(send_msg_request1);
                	continue;
                }
                
                
                /*	************** Received a "Terminate" message from local application **************
                 *	- Don't accept any more tasks from Local Application
                 *	- Wait for all workers to finish their PDF tasks, then terminate them
                 *	- Create output summary file for tasks, store it in S3, and send message to Local Application with location of output file
                 *	- Terminate Manager instance
                 */
                
                
                if (message.getBody().equals("Terminate")){				// Received a termination message from local application
                	terminateSignal = true;
                	messageReceiptHandle = message.getReceiptHandle();
                	sqs.deleteMessage(new DeleteMessageRequest(newTaskQueueURL, messageReceiptHandle));		// Delete 'Terminate' message from 'newTaskQueue'
                	break;
                }
            }
            
            
            
            /*	************** Go through Manager|Workers queue messages ************** */
            
            
            System.out.println("\nReceiving messages from " + donePDFTaskQueueURL + " SQS queue\n");
            ReceiveMessageRequest receiveMessageRequest_fromWorkers = new ReceiveMessageRequest(donePDFTaskQueueURL).withMessageAttributeNames("All");
            List<Message> workers_messages = sqs.receiveMessage(receiveMessageRequest_fromWorkers).getMessages();
            for (Message message : workers_messages) {
            	printMessage(message);
            	
            	/*	************** Received a "donePDFTask" message from Worker **************
                 *  - Calculate response messages from workers for file
                 *  - If finished working on an input file: Create a summary output file accordingly
                 *  - Upload the output file to S3 Storage
                 *  - Sends a message to the user queue with the location of the file
              	*/
            	
            	if (message.getBody().equals("donePDFTask")){
            		for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
            			if (entry.getKey().equals("originalURL")){			// Get the input file bucket location
                			inputFileBucket = entry.getValue().getStringValue();
                		}
            			if (entry.getKey().equals("newFileURL")){				// Get the input file key
                			newFileURL = entry.getValue().getStringValue();
                		}
            			if (entry.getKey().equals("Operation")){				// Get the input file key
                			operation = entry.getValue().getStringValue();
                		}
                    }
            		String line = "<" + operation + ">:	 " + inputFileBucket + " 		" + newFileURL;
                	outputFilesMap.get(inputFileKey).add(line);
            		inputFilesMap.get(inputFileKey).incCompletedTasks();		// Increment no. of completed tasks for given input file identifier
            		messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(donePDFTaskQueueURL, messageReceiptHandle));		// Delete message
                    
                    // If all input file tasks are done, generate summary file, upload to S3, send Done Task message to Local Application
                    if (inputFilesMap.get(inputFileKey).isDone()){
                    	sendDoneTask(s3, sqs, doneTaskQueueURL, inputFileKey, outputBucketName, outputFilesMap);
                    	inputFilesMap.remove(inputFileKey);						// Remove input file from map
                    }
                    continue;
            	}
            }
        }
 
        
	}
	
	
	
	/**	************** Send new PDF task to queue **************	**/
	
	
	private static int sendNewPDFTask(S3Object inputFileObj, AmazonSQS sqs, String newPDFTaskQueueURL, String fileKey) throws IOException{
		int numOfMessages = 0;
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputFileObj.getObjectContent()));		// Read content of input file object
	    while (true) {
	        String line = reader.readLine();
	        if (line == null){
	        	break;
	        }
	        String operation = line.split("\\t")[0];		// Get operation to perform
	        String pdfURL = line.split("\\t")[1];			// Get PDF URL
	        System.out.println("Operation:    " + operation);
	        System.out.println("PDF URL:    " + pdfURL);
	        
	        // Send 'newPDFTask' message to queue
	        SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(newPDFTaskQueueURL).withMessageBody("newPDFTask");
	        send_msg_request.addMessageAttributesEntry("inputFileKey", new MessageAttributeValue().withDataType("String").withStringValue(fileKey));
	        send_msg_request.addMessageAttributesEntry("Operation", new MessageAttributeValue().withDataType("String").withStringValue(operation));
	        send_msg_request.addMessageAttributesEntry("PDF_URL", new MessageAttributeValue().withDataType("String").withStringValue(pdfURL));
	        
			sqs.sendMessage(send_msg_request);
			numOfMessages++;
	    }
	    return numOfMessages;
	}
	
	
	
	/**	************** Generate & upload summary file to S3, send done task to queue **************	**/
	
	
	private static void sendDoneTask(AmazonS3 s3, AmazonSQS sqs, String doneTaskQueueURL, String inputFileKey, String outputBucketName, Map<String, List<String>> outputFilesMap) throws IOException{
		String outputFileURL = generateSummaryFile(s3, outputFilesMap.get(inputFileKey), outputBucketName, inputFileKey);
		SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(doneTaskQueueURL).withMessageBody("doneTask");
        send_msg_request.addMessageAttributesEntry("outputFileLocation", new MessageAttributeValue().withDataType("String").withStringValue(outputFileURL));
        send_msg_request.addMessageAttributesEntry("outputFileKey", new MessageAttributeValue().withDataType("String").withStringValue("outputFileFor" + inputFileKey));
		sqs.sendMessage(send_msg_request);
	}
	
	
	
	
	/**	************** Create Worker instances **************	**/
    
	
    private static void createWorker(AmazonEC2 ec2, int numOfWorkers){
        try {
			System.out.println("Creating Worker instance...");
			RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1);
			request.setInstanceType(InstanceType.T1Micro.toString());
			
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			String instanceId = instances.get(0).getInstanceId();
			
			CreateTagsRequest tagRequest = new CreateTagsRequest();
			//tagRequest = tagRequest.withResources(instanceId).withTags(new Tag("Worker", Integer.toString(numOfWorkers)));		// Set tag of a worker and a number value
			tagRequest = tagRequest.withResources(instanceId).withTags(new Tag("Worker", "1"));
			ec2.createTags(tagRequest);
			  
			System.out.println("Launch instances: " + instances);
		} 
        catch (AmazonServiceException ase) {
	        System.out.println("Caught Exception: " + ase.getMessage());
	        System.out.println("Reponse Status Code: " + ase.getStatusCode());
	        System.out.println("Error Code: " + ase.getErrorCode());
	        System.out.println("Request ID: " + ase.getRequestId());
        }
    
    }
    
    
    
    /**	************** Terminate worker instances  **************	**/
    
    
    private static void terminateWorkers(AmazonEC2 ec2){
    	List<String> instanceIds = new ArrayList<String>();
    	DescribeInstancesRequest listingRequest = new DescribeInstancesRequest();
    	List<String> valuesT1 = new ArrayList<String>();
    	valuesT1.add("1");
    	Filter filter1 = new Filter("tag:Worker", valuesT1);
    	DescribeInstancesResult result = ec2.describeInstances(listingRequest.withFilters(filter1));
    	List<Reservation> reservations = result.getReservations();
    	for (Reservation reservation : reservations) {
    		List<Instance> instances = reservation.getInstances();
    		for (Instance instance : instances) {
    			instanceIds.add(instance.getInstanceId());
    			System.out.println("Terminating instance:  " + instance.getInstanceId());
    		}
    	}
		TerminateInstancesRequest terminate_request = new TerminateInstancesRequest(instanceIds);
		ec2.terminateInstances(terminate_request);		
    }
    
    
    
    /**	************** Terminate Manager instance  **************	**/
    
    
    private static void terminateManager(AmazonEC2 ec2){
    	List<String> instanceIds = new ArrayList<String>();
    	DescribeInstancesRequest listingRequest = new DescribeInstancesRequest();
    	List<String> valuesT1 = new ArrayList<String>();
    	valuesT1.add("0");
    	Filter filter1 = new Filter("tag:Manager", valuesT1);
    	DescribeInstancesResult result = ec2.describeInstances(listingRequest.withFilters(filter1));
    	List<Reservation> reservations = result.getReservations();
    	for (Reservation reservation : reservations) {
    		List<Instance> instances = reservation.getInstances();
    		for (Instance instance : instances) {
    			instanceIds.add(instance.getInstanceId());
    			System.out.println("Terminating instance:  " + instance.getInstanceId());
    		}
    	}
		TerminateInstancesRequest terminate_request = new TerminateInstancesRequest(instanceIds);
		ec2.terminateInstances(terminate_request);		
    }
	

    
    /**	************** Generate summary file and upload to s3 storage **************	**/
    
    
    private static String generateSummaryFile(AmazonS3 s3, List<String> outputFileLines, String bucket, String inputFileKey){
    	
    	/* *** Generate HTML summary file *** */
    	
    	File htmlFile = new File("outputFileFor" + inputFileKey);
    	try{
	    	FileWriter writer = new FileWriter(htmlFile);
	    	BufferedWriter out = new BufferedWriter(writer);
	    	out.write("<!DOCTYPE html>\n<html>\n<body>\n");
	    	for (int i = 0; i < outputFileLines.size(); i++) {
				out.write("<p> " + outputFileLines.get(i) + " </p>\n");
			}
	    	out.write("</body>\n</html>");
	    	out.close();
    	}
    	catch (Exception e){
    		System.err.println("Error: " + e.getMessage());
    	}
    	
    	/* *** Upload summary file to the Manager's S3 output bucket *** */
    	
    	System.out.println("Uploading summary file to S3 storage...\n");
        //String key = outputFile.getName().replace('\\', '_').replace('/','_').replace(':', '_');
    	String key = htmlFile.getName();
    	System.out.println("Output File Name: " + key);
        PutObjectRequest req = new PutObjectRequest(bucket, key, htmlFile);
        s3.putObject(req);
        URL outputFileURL = s3.getUrl(bucket, key);
        System.out.println("Output File URL: " + outputFileURL.toString());
        return outputFileURL.toString();
    }
    
   
    
    
    /**	************** Print message contents **************	**/
    
    
    private static void printMessage(Message message){
        System.out.println("  ******************** Message ********************");
        System.out.println("    MessageId:     " + message.getMessageId());
        System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
        System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
        System.out.println("    Body:          " + message.getBody());
        for (Entry<String, String> entry : message.getAttributes().entrySet()) {
            System.out.println("  Attribute");
            System.out.println("    Name:  " + entry.getKey());
            System.out.println("    Value: " + entry.getValue());
        }
        for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
            System.out.println("  Attribute (User made) ");
            System.out.println("    Name:  " + entry.getKey());
            System.out.println("    String Value: " + entry.getValue().getStringValue());
        }
        System.out.println("   **************************************************");
        System.out.println();
    }
    
    
}
