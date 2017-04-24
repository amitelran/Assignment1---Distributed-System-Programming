package EC2Manager;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.sun.org.apache.xml.internal.utils.URI;



/**	**************************	Manager Flow  **************************
 
 * If the message is that of a new task it:
		- Downloads the input file from S3.
		- Creates an SQS message for each URL in the input file together with the operation that should be performed on it.
		- Checks the SQS message count and starts Worker processes (nodes) accordingly.
				-- The manager should create a worker for every n messages, if there are no running workers.
				-- If there are k active workers, and the new job requires m workers, then the manager should create m-k new workers, if possible.
		- After the manger receives response messages from the workers on all the files on an input file, then it:
				-- Creates a summary output file accordingly,
				-- Uploads the output file to S3,
				-- Sends a message to the user queue with the location of the file.

 * If the message is a termination message, then the manager:
		- Does not accept any more input files from local applications.
		- Waits for all the workers to finish their job, and then terminates them.
		- Creates response messages for the jobs, if needed.
		- Terminates.

IMPORTANT:  the manager must process requests from local applications simultaneously; 
			meaning, it must not handle each request at a time, but rather work on all requests in parallel.

 * *********************************************************************
 */

public class EC2Manager {

	public static void main(String[] args) throws IOException {
		
		
        //AWSCredentials credentials = new PropertiesCredentials(EC2Manager.class.getResourceAsStream("AwsCredentials.properties"));
        /*globalVars.ec2 = new AmazonEC2Client(credentials);	// EC2 client
        globalVars.ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        globalVars.s3 = new AmazonS3Client(credentials);		// S3 Storage client
        globalVars.sqs = new AmazonSQSClient(credentials);	// SQS client */
        AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();			// Search and get credentials file in system
        globalVars.ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        globalVars.s3 = AmazonS3ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        globalVars.sqs = AmazonSQSClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        boolean newTaskQueueExists = false;					// Boolean flag to indicate if need to a create a 'newTask' queue
        boolean newPDFTaskQueueExists = false;				// Boolean flag to indicate if need to create a new 'newPDFTask' queue
        boolean donePDFTaskQueueExists = false;				// Boolean flag to indicate if need to create a new 'donePDFTask' queue
        String donePDFTaskQueueURL = null;					// 'donePDFTask' Manager <--> Workers queue
        String inputFileBucket = null;						// S3 bucket of the input file with a list of PDF URLs and operations to perform	
        String inputFileKey = null;							// Key of the input file in the S3 Storage
        String newFileURL = null;							// URL of a new image file as given by Worker
        String operation = null;							// The operation performed by Worker for a given PDF URL
        String messageReceiptHandle = null;					// Message receipt handle, required when deleting queue messages
        String outputBucketName = "outputmanagerbucketamityoav";
        Map<String, String> queueAttributes = new HashMap<String, String>();
       
       
		
        /*	************** Get 'n' stating number of messages per worker ************** */	
        /*
        if ((args.length < 1) || (Integer.parseInt(args[0]) < 1)){					
        	System.out.println("Integer stating how many PDFs per Worker has not been provided by Local Application");
        	return;
        }
        else{
        	try{
        		globalVars.numOfPDFperWorker = Integer.parseInt(args[0]);
            	System.out.println("Number of messages per Worker set to be: " + globalVars.numOfPDFperWorker + "\n");
        	}
        	catch(Exception e){
        		System.out.println("Error: " + e.getMessage());
        		return;
        	}   	
        }
        */
        globalVars.numOfPDFperWorker = 10;
        
        /*	************** Get 'newTask|Termination' Local Application <--> Manager SQS queue, and check if Manager <--> Workers queues exist ************** */	
        
        
        for (String queueUrl : globalVars.sqs.listQueues().getQueueUrls()) {
        	URI uri = new URI(queueUrl);
        	String path = uri.getPath();
        	String queueName = path.substring(path.lastIndexOf('/') + 1); 
            if (queueName.equals("newTaskQueue")){
            	newTaskQueueExists = true;
            	globalVars.newTaskQueueURL = queueUrl;
            }
            else if(queueName.equals("newPDFTaskQueue")){
            	newPDFTaskQueueExists = true;
            	globalVars.newPDFTaskQueueURL = queueUrl;
            }
            else if(queueName.equals("donePDFTaskQueue")){
            	donePDFTaskQueueExists = true;
            	donePDFTaskQueueURL = queueUrl;
            }
        	System.out.println("QueueUrl: " + queueUrl);
        }
        
   
        
        /*	************** Create 'newPDFTask|WorkerTermination' and 'donePDFTask' Manager <--> Workers SQS queues if doesn't exist ************** */	
        
        
        queueAttributes.put("VisibilityTimeout", Integer.toString(300));						// Set attribute VisibilityTimeout to be 5 minutes
        if (!newTaskQueueExists){
        	System.out.println("Creating a Local Applications <--> Manager 'newTask' SQS queue...");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("newTaskQueue").withAttributes(queueAttributes);
            globalVars.newTaskQueueURL = globalVars.sqs.createQueue(createQueueRequest).getQueueUrl();			// Storing the newly created queue URL
        }
        if (!newPDFTaskQueueExists){
        	 System.out.println("Creating a Manager <--> Workers 'newPDFTask|WorkerTermination' SQS queue...");
             // CreateQueueRequest createQueueRequest = new CreateQueueRequest("newPDFTaskQueue" + UUID.randomUUID());
             CreateQueueRequest createQueueRequest = new CreateQueueRequest("newPDFTaskQueue").withAttributes(queueAttributes);
             globalVars.newPDFTaskQueueURL = globalVars.sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        }
        if (!donePDFTaskQueueExists){
        	System.out.println("Creating a Manager <--> Workers 'donePDFTask' SQS queue...");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("donePDFTaskQueue").withAttributes(queueAttributes);
            donePDFTaskQueueURL = globalVars.sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        }
        
        
        /*	************** Run 'newTasksThread' Thread to loop over the 'newTaskQueue' SQS queue **************	*/
        
        newTasksThread managerThreadNewTasks = new newTasksThread();
        Thread managerThread = new Thread(managerThreadNewTasks);
        managerThread.start();
        
        
        /*	************** Loop until termination message from local application **************	*/
        
        SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(globalVars.newTaskQueueURL).withMessageBody("newTask");
        send_msg_request.addMessageAttributesEntry("inputFileBucket", new MessageAttributeValue().withDataType("String").withStringValue("inputbucketamitelran"));
        send_msg_request.addMessageAttributesEntry("inputFileKey", new MessageAttributeValue().withDataType("String").withStringValue("input.txt"));
        globalVars.sqs.sendMessage(send_msg_request);
       
        while(true){
        	
        	/* ************** If terminated by Local App & All Workers finished their tasks **************
        	 * - Does not accept any more input file from local application
        	 * - Terminate all Workers
        	 * - Delete Manager <--> Workers queues
        	 * - Generate last output files
        	 * - Terminate Manager instance
        	 */
        	
        	if ((globalVars.terminateSignal) && (globalVars.numOfMessages == 0)){	
        		terminateWorkers(globalVars.ec2);
        		System.out.println("Deleting queue: " + globalVars.newPDFTaskQueueURL);
        		globalVars.sqs.deleteQueue(new DeleteQueueRequest(globalVars.newPDFTaskQueueURL));
                System.out.println("Deleting queue: " + donePDFTaskQueueURL);
                globalVars.sqs.deleteQueue(new DeleteQueueRequest(donePDFTaskQueueURL));
                terminateManager(globalVars.ec2);
        		return;
        	}
        	
        	
        	
            /*	************** Go through Manager|Workers queue messages ************** */
            
            
            ReceiveMessageRequest receiveMessageRequest_fromWorkers = new ReceiveMessageRequest(donePDFTaskQueueURL).withMessageAttributeNames("All");
            List<Message> workers_messages = globalVars.sqs.receiveMessage(receiveMessageRequest_fromWorkers).getMessages();
            for (Message message : workers_messages) {
            	printMessage(message);
            	
            	/*	************** Received a "donePDFTask" message from Worker **************
                 *  - Calculate response messages from workers for file
                 *  - If finished working on an input file: Create a summary output file accordingly
                 *  - Upload the output file to S3 Storage
                 *  - Send a message to the 'doneTaskQueue' queue with the location of the file in the S3 storage
              	*/
            	
            	if (message.getBody().equals("donePDFTask")){
            		for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
            			if (entry.getKey().equals("originalURL")){			// Get the input file bucket location
                			inputFileBucket = entry.getValue().getStringValue();
                		}
            			else if (entry.getKey().equals("newFileURL")){				// Get the input file key
                			newFileURL = entry.getValue().getStringValue();
                		}
            			else if (entry.getKey().equals("Operation")){				// Get the input file key
                			operation = entry.getValue().getStringValue();
                		}
                    }
            		
            		// Ensure input file corresponding key in maps are initialized
            		if ((globalVars.inputFilesMap.get(inputFileKey) != null) && (globalVars.inputFilesMap.get(inputFileKey) != null)){
	            		globalVars.numOfMessages--;																		// Decrement number of 'online' uncompleted tasks
	            		String line = operation + "\t" + inputFileBucket + "\t" + newFileURL;
	            		globalVars.outputFilesMap.get(inputFileKey).add(line);											// Add completed task line to the List<String> of output file lines
	            		globalVars.inputFilesMap.get(inputFileKey).incCompletedTasks();									// Increment no. of completed tasks for given input file identifier
	            		messageReceiptHandle = message.getReceiptHandle();
	            		globalVars.sqs.deleteMessage(new DeleteMessageRequest(donePDFTaskQueueURL, messageReceiptHandle));		// Delete 'donePDFTask' message from queue after handling it
	            		
	            		// If all input file's tasks are done, generate summary file, upload to S3, send Done Task message to Local Application queue
	                    if (globalVars.inputFilesMap.get(inputFileKey).isDone()){
	                    	sendDoneTask(globalVars.s3, globalVars.sqs, globalVars.inputFilesMap.get(inputFileKey).getFileQueueURL(), inputFileKey, outputBucketName, globalVars.outputFilesMap);
	                    	globalVars.inputFilesMap.remove(inputFileKey);						// Remove input file from map
	                    	globalVars.outputFilesMap.remove(inputFileKey);						// Remove output file from map
	                    }
            		}
                    continue;
            	}
            }
        }

	}
	
	
	
	/**	************** Generate & upload summary file to S3, send done task to queue **************	**/
	
	
	private static void sendDoneTask(AmazonS3 s3, AmazonSQS sqs, String doneTaskQueueURL, String inputFileKey, String outputBucketName, Map<String, List<String>> outputFilesMap) throws IOException{
		String outputFileURL = generateSummaryFile(s3, outputFilesMap.get(inputFileKey), outputBucketName, inputFileKey);
		SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(doneTaskQueueURL).withMessageBody("taskDone");
        send_msg_request.addMessageAttributesEntry("outputFileBucket", new MessageAttributeValue().withDataType("String").withStringValue(outputFileURL));
        send_msg_request.addMessageAttributesEntry("outputFileKey", new MessageAttributeValue().withDataType("String").withStringValue("outputFileFor" + inputFileKey));
		sqs.sendMessage(send_msg_request);
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
    	if (instanceIds != null){
    		TerminateInstancesRequest terminate_request = new TerminateInstancesRequest(instanceIds);
    		ec2.terminateInstances(terminate_request);
    	}		
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
    	if (instanceIds != null){
			TerminateInstancesRequest terminate_request = new TerminateInstancesRequest(instanceIds);
			ec2.terminateInstances(terminate_request);		
    	}
    }
	

    
    /**	************** Generate summary file and upload to s3 storage **************	**/
    
    
    private static String generateSummaryFile(AmazonS3 s3, List<String> outputFileLines, String bucket, String inputFileKey){
    	
    	/* *** Generate summary file *** */

    	File summaryFile = null;
    	try{
    		int index = inputFileKey.lastIndexOf(".");					// Get rid of file suffix
    		summaryFile = new File("outputFileFor" + inputFileKey.substring(0, index) + ".txt");
	    	FileWriter writer = new FileWriter(summaryFile);
	    	BufferedWriter out = new BufferedWriter(writer);
	    	for (int i = 0; i < outputFileLines.size(); i++) {
				out.write(outputFileLines.get(i));
				out.newLine();
			}
	    	out.close();
    	}
    	catch (Exception e){
    		System.err.println("Error: " + e.getMessage());
    	}
    	
    	/* *** Upload summary file to the Manager's S3 output bucket *** */
    	
    	System.out.println("Uploading summary file to S3 storage...\n");
        //String key = outputFile.getName().replace('\\', '_').replace('/','_').replace(':', '_');
    	String key = summaryFile.getName();
    	System.out.println("Output File Name: " + key);
        PutObjectRequest req = new PutObjectRequest(bucket, key, summaryFile);
        s3.putObject(req);
        URL outputFileURL = s3.getUrl(bucket, key);
        System.out.println("Output File URL: " + outputFileURL.toString());
        return outputFileURL.toString();
    }
    
   
    
    
    /**	************** Print message contents **************	**/
    
    
    public static void printMessage(Message message){
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
