package EC2Manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
        boolean terminateSignal = false;					// Termination signal indicator
        int numOfWorkers = 0;
        int numOfPDFperWorker = 0;					// An integer stating how many PDF files per worker, as determined by user
        int numOfMessages = 0;						// SQS message counter
        String newTaskQueueURL = null;				// 'newTask|Termination' Local Application <--> Manager queue
        String doneTaskQueueURL = null;				// 'doneTask' Local Application <--> Manager queue
        String inputFileBucket = null;				// S3 bucket of the input file with a list of PDF URLs and operations to perform	
        String inputFileKey = null;					// Key of the input file in the S3 Storage
        //List<inputFile> inputFilesList = new ArrayList<inputFile>();		// Initialize empty list of input files
        Map<String, inputFile> inputFilesMap = new HashMap<String, inputFile>();
        Map<String, File> outputFilesMap = new HashMap<String, File>();
       
		
        
        /*	************** Get 'newTask|Termination' and 'doneTask' Local Application <--> Manager SQS queues ************** */	
        
        
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));	// Declare SQS client
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
        	System.out.println("QueueUrl: " + queueUrl);
        }
        
   
        
        /*	************** Set 'newPDFTask|WorkerTermination' and 'donePDFTask' Manager <--> Workers SQS queues ************** */	
        
        
        System.out.println("Creating a Manager <--> Workers 'newPDFTask|WorkerTermination' SQS queue");
       // CreateQueueRequest createQueueRequest = new CreateQueueRequest("newPDFTaskQueue" + UUID.randomUUID());
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("newPDFTaskQueue");
        String newPDFTaskQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        
        System.out.println("Creating a Manager <--> Workers 'donePDFTask' SQS queue");
        createQueueRequest = new CreateQueueRequest("donePDFTaskQueue");
        String donePDFTaskQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();		// Storing the newly created queue URL
        
        
        /*	************** Loop until termination message from local application **************	*/
        
       
        
        while(true){
        	
        	if (terminateSignal){				// If signaled to terminate & all workers finished their tasks: terminate workers, break loop
        		terminateWorkers(ec2);
        		break;
        	}
        	
        	System.out.println("\nReceiving messages from " + newTaskQueueURL + " SQS queue\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(newTaskQueueURL);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
            for (Message message : messages) {
            	printMessage(message);
                
                /*	************** Received a "newTask" message from local application **************
                 *  - Download file from S3 Storage
                 *  - Count number of files lines, create worker per 'numOfPDFperWorker' integer
                 *  - Process the input file lines and send each line as a 'newPDFTask' message to Workers queue
              	*/
                
                if ((message.getBody().equals("newTask")) && (!terminateSignal)){				// Received a "newTask" message from local application (while not signaled to terminate)
                	for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                		if (entry.getKey().equals("inputFileBucket")){		// Get the input file bucket location
                			inputFileBucket = entry.getValue();
                		}
                		if (entry.getKey().equals("inputFileKey")){			// Get the input file key
                			inputFileKey = entry.getValue();
                		}
                    }
                	System.out.println("Downloading input file object");
                    S3Object inputFileObj = s3.getObject(new GetObjectRequest(inputFileBucket, inputFileKey));
                    System.out.println("Content-Type: "  + inputFileObj.getObjectMetadata().getContentType());
                    System.out.println();
                    outputFilesMap.put(inputFileKey, new File(inputFileKey + "output"));				// Add output file element to Map
                    int numOfTasks = sendNewPDFTask(inputFileObj, sqs, newPDFTaskQueueURL, inputFileKey);
                    numOfMessages += numOfTasks;														// Send new PDF task to queue and increment 'newPDFTask' queue message counter
                    inputFilesMap.put(inputFileKey, new inputFile(inputFileBucket, inputFileKey, numOfTasks));		// Insert input file data to Map
                    
                    // Stabilize ratio of <worker per 'n' number of messages> by creating new workers (not terminating running workers)
                    while ((numOfMessages / numOfWorkers) > numOfPDFperWorker){
                    	createWorker(ec2, numOfWorkers);
                    	numOfWorkers++;
                    }
                	continue;
                }
                
                
                /*	************** Received a "Terminate" message from local application **************
                 *	- Don't accept any more tasks from Local Application
                 *	- Wait for all workers to finish their PDF tasks, then terminate them
                 *	- Create output summary file for tasks, store it in S3, and send message to Local Application with location of output file
                 *	- Terminate Manager instance
                 */
                
                
                if (message.getBody().equals("Terminate")){				// Received a termination message from local application
                	if (numOfMessages == 0){
                		terminateSignal = true;						// Indicate termination after all Workers completed their tasks
                		// Delete Manager <--> Workers queues
                		System.out.println("Deleting queue: " + newPDFTaskQueueURL);
                        sqs.deleteQueue(new DeleteQueueRequest(newPDFTaskQueueURL));
                        System.out.println("Deleting queue: " + donePDFTaskQueueURL);
                        sqs.deleteQueue(new DeleteQueueRequest(donePDFTaskQueueURL));
                	}
                	
                	break;
                }
            }
            
            
            /*	************** Go through Manager|Workers queue messages ************** */
            
            System.out.println("\nReceiving messages from " + donePDFTaskQueueURL + " SQS queue\n");
            ReceiveMessageRequest receiveMessageRequest_fromWorkers = new ReceiveMessageRequest(donePDFTaskQueueURL );
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
            		for (Entry<String, String> entry : message.getAttributes().entrySet()) {
            			if (entry.getKey().equals("inputFileBucket")){		// Get the input file bucket location
                			inputFileBucket = entry.getValue();
                		}
            			if (entry.getKey().equals("inputFileKey")){			// Get the input file key
                			inputFileKey = entry.getValue();
                		}
                    }
            		inputFilesMap.get(inputFileKey).incCompletedTasks();		// Increment no. of completed tasks for given input file identifier
            		String messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(doneTaskQueueURL, messageReceiptHandle));		// Delete message
                    
                    // If all input file tasks are done, generate summary file
                    if (inputFilesMap.get(inputFileKey).isDone()){
                    	generateSummaryFile(s3, outputFilesMap.get(inputFileKey), inputFileBucket);
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
	
    
    
    /**	************** Add line to summary file **************	**/
    
    
    
    private static void outputFileAddLine(){
    	
    }
    
    
    
    /**	************** Generate summary file and upload to s3 storage **************	**/
    
    
    private static void generateSummaryFile(AmazonS3 s3, File outputFile, String bucket){
    	
    	System.out.println("Uploading summary file to S3 storage...\n");
        String key = outputFile.getName().replace('\\', '_').replace('/','_').replace(':', '_');
        PutObjectRequest req = new PutObjectRequest(bucket, key, outputFile);
        s3.putObject(req);
    }
    
   
    
    
    /**	************** Print message contents **************	**/
    
    
    private static void printMessage(Message message){
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
    }
    
    
}
