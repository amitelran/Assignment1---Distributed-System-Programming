package EC2Manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class newTasksThread implements Runnable {
	String inputFileBucket = null;						// S3 bucket of the input file with a list of PDF URLs and operations to perform	
    String inputFileKey = null;							// Key of the input file in the S3 Storage
    String inputFileUniqueKey = null;					// Generating a unique input file key in order to tell different files with same name
    String doneTaskQueueURL = null;
    String messageReceiptHandle = null;
    
    
    
 
	public void run() {
		while(true){
	        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(globalVars.newTaskQueueURL).withMessageAttributeNames("All");
	        List<Message> messages = globalVars.sqs.receiveMessage(receiveMessageRequest).getMessages();
	        for (Message message : messages) {
	        	EC2Manager.printMessage(message);
	            
	            /*	************** Received a "newTask" message from local application **************
	             *  - Download file from S3 Storage
	             *  - Count number of files lines (which is number of tasks for file), create worker per 'numOfPDFperWorker' integer
	             *  - Process the input file lines and send each line as a 'newPDFTask' message to Workers queue
	          	*/
	            
	            if ((message.getBody().equals("newTask")) && (!globalVars.terminateSignal)){				// Received a "newTask" message from local application (while not signaled to terminate)
	            	for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
	            		if (entry.getKey().equals("inputFileBucket")){							// Get the input file bucket location
	            			inputFileBucket = entry.getValue().getStringValue();
	            			System.out.println(inputFileBucket);
	            		}
	            		else if (entry.getKey().equals("inputFileKey")){						// Get the input file key
	            			inputFileKey = entry.getValue().getStringValue();
	            			System.out.println(inputFileKey);
	            		}
	            		else if (entry.getKey().equals("taskDoneQueueUrl")){						// Get specific SQS queue to send 'doneTask' message
	            			doneTaskQueueURL = entry.getValue().getStringValue();
	            		}
	                }
	            	System.out.println("\nDownloading input file object from S3 storage...\n");
	                S3Object inputFileObj = globalVars.s3.getObject(new GetObjectRequest(inputFileBucket, inputFileKey));
	                System.out.println("Content-Type: "  + inputFileObj.getObjectMetadata().getContentType());
	                System.out.println();
	                
	                inputFileUniqueKey = inputFileKey + UUID.randomUUID();											// Generate unique key for input file
	                globalVars.outputFilesMap.put(inputFileUniqueKey, new ArrayList<String>());								// Add output file lines list to Map
	                int numOfTasks;
					try {
						numOfTasks = sendNewPDFTask(inputFileObj, globalVars.sqs, globalVars.newPDFTaskQueueURL, inputFileKey, inputFileUniqueKey);
						globalVars.numOfMessages += numOfTasks;																	// Increment 'newPDFTask' queue message counter
		                globalVars.inputFilesMap.put(inputFileUniqueKey, new inputFile(inputFileBucket, inputFileKey, inputFileUniqueKey, doneTaskQueueURL, numOfTasks));							// Insert input file data to Map by <uniqueKey, inputFile object>
		                
		                messageReceiptHandle = message.getReceiptHandle();
		                globalVars.sqs.deleteMessage(new DeleteMessageRequest(globalVars.newTaskQueueURL, messageReceiptHandle));				// Delete 'newTask' message from 'newTaskQueue'
		                /*
		                // Stabilize ratio of <worker per 'n' number of messages> by creating new workers (not terminating running workers)
		                if (numOfMessages > 0){
		                	if (numOfWorkers == 0){					// Create a new worker to avoid dividing by zero
		                		createWorker(ec2, numOfWorkers);
		                		numOfWorkers++;
		                	}
		                    while ((numOfMessages / numOfWorkers) > numOfPDFperWorker){
		                    	createWorker(ec2, numOfWorkers);
		                    	numOfWorkers++;
		                    }
		                }
		                */
					} 
					catch (IOException e) {
						e.printStackTrace();
					}		
	                
	                
	                SendMessageRequest send_msg_request1 = new SendMessageRequest().withQueueUrl(globalVars.newTaskQueueURL).withMessageBody("Terminate");
	                globalVars.sqs.sendMessage(send_msg_request1);
	            	continue;
	            }
	            
	            
	            /*	************** Received a "Terminate" message from local application **************
	             *	- Set 'terminateSignal' boolean flag on
	             *	- Delete message from queue
	             *	- Finish running (don't get more input messages from local applicationa)
	             */
	            
	            
	            if (message.getBody().equals("Terminate")){													// Received a termination message from local application
	            	globalVars.terminateSignal = true;																	// Set 'terminateSignal' flag 'on'
	            	messageReceiptHandle = message.getReceiptHandle();
	            	globalVars.sqs.deleteMessage(new DeleteMessageRequest(globalVars.newTaskQueueURL, messageReceiptHandle));		// Delete 'Terminate' message from 'newTaskQueue'
	            	return;
	            }
	        }
		}
		
	}
        
    
        
        
        
    /**	************** Send new PDF task to queue **************	**/
        
        
    private static int sendNewPDFTask(S3Object inputFileObj, AmazonSQS sqs, String newPDFTaskQueueURL, String fileKey, String fileUniqueKey) throws IOException{
		int numOfMessages = 0;
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputFileObj.getObjectContent()));		// Read content of input file object
	    while (true) {										// Read each file line, generate PDF task message to 'newPDFTaskQueue' Manager|Workers queue
	        String line = reader.readLine();				
	        if (line == null){
	        	break;
	        }
	        String operation = line.split("\\t")[0];		// Get operation to perform
	        String pdfURL = line.split("\\t")[1];			// Get PDF URL
	        System.out.println("Operation:    " + operation);
	        System.out.println("PDF URL:    " + pdfURL);
	        
	        // Send 'newPDFTask' message to queue, including attributes: input file name, input file unique key, operation to perform, PDF URL
	        SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(newPDFTaskQueueURL).withMessageBody("newPDFTask");
	        send_msg_request.addMessageAttributesEntry("inputFileKey", new MessageAttributeValue().withDataType("String").withStringValue(fileKey));
	        send_msg_request.addMessageAttributesEntry("inputFileUniqueKey", new MessageAttributeValue().withDataType("String").withStringValue(fileUniqueKey));
	        send_msg_request.addMessageAttributesEntry("Operation", new MessageAttributeValue().withDataType("String").withStringValue(operation));
	        send_msg_request.addMessageAttributesEntry("PDF_URL", new MessageAttributeValue().withDataType("String").withStringValue(pdfURL));
			sqs.sendMessage(send_msg_request);
			numOfMessages++;
	    }
	    return numOfMessages;								// Return number of messages sent (number of tasks generated from input file)
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
    
}
