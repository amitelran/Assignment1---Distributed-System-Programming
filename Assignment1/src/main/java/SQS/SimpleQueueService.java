package SQS;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
 
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SimpleQueueService {

	public static void main(String[] args) throws Exception {
 
        @SuppressWarnings("deprecation")
		AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));
        try {
        	// Create a Local Application <--> Manager queue
        	System.out.println("Creating a Local Application <--> Manager SQS queue");
            //CreateQueueRequest createLocalAppQueueRequest = new CreateQueueRequest("localAppQueue"+ UUID.randomUUID());
            //String localAppQueueUrl = sqs.createQueue(createLocalAppQueueRequest).getQueueUrl();
            String localAppQueueUrl = queueCreate(sqs, "localAppQueue");
 
            // Create a Manager <--> Workers queue
            System.out.println("Creating a Manager <--> Workers SQS queue");
            //CreateQueueRequest createInstancesQueueRequest = new CreateQueueRequest("instancesQueue"+ UUID.randomUUID());
            //String instancesQueueUrl = sqs.createQueue(createInstancesQueueRequest).getQueueUrl();
            String instancesQueueUrl = queueCreate(sqs, "instancesQueue");
 
            String queueURL = queueCreate(sqs, "myQueue");
            // List all queues in the account
            ListAllQueues(sqs);
            
            // Send message test
            donePDFTaskMessage donePDFTask = new donePDFTaskMessage("outputFilelocation", "operation", "URL");
            sendQueueMessage(sqs, instancesQueueUrl, MessageType.donePDFTask, donePDFTask);
            
            newPDFTaskMessage newpdfTSK = new newPDFTaskMessage("test2", "ToHTML");
            sendQueueMessage(sqs, instancesQueueUrl, MessageType.newPDFTask, newpdfTSK);
            
            // Receive message test
            List<Message> msgList = receiveMessages(sqs, localAppQueueUrl);
            msgList = receiveMessages(sqs, instancesQueueUrl);
            msgList = receiveMessages(sqs, instancesQueueUrl);
            
            // Deleting queues
            //deleteQueues(sqs);
            deleteSingleQueue(sqs, localAppQueueUrl);
            deleteSingleQueue(sqs, instancesQueueUrl);
            deleteSingleQueue(sqs, queueURL);
            
        } catch (AmazonServiceException ase) {
        	
            System.out.println("Caught an AmazonServiceException, request made it to Amazon SQS, but was rejected with an error.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            
        } catch (AmazonClientException ace) {
        	
            System.out.println("Caught an AmazonClientException, client encountered an internal problem while trying to communicate with SQS.");
            System.out.println("Error Message: " + ace.getMessage());

        }
    }
	
	/*************** Create new Simple Queue Service (SQS) ***************
	 * @throws IOException ***************/
	
	
	public static AmazonSQS createSQS() throws IOException{
		AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));
		return sqs;
	}
	
	/*
	/*************** AmazonSQS getter ***************
	 * @throws IOException ***************
	
	
	public static AmazonSQS getSQS() throws IOException{
		AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));
		return sqs;
	}*/
	
	/*************** Create new queue, returns queue URL ***************/
	
	
	public static String queueCreate(AmazonSQS sqs, String queueName){
		//System.out.println("Creating a Local Application <--> Manager SQS queue");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName + "_" + UUID.randomUUID());
        String queueURL = sqs.createQueue(createQueueRequest).getQueueUrl();
        return queueURL;
	}
	
	
	
	/*************** Delete a single queue ***************/
	
	
	public static void deleteSingleQueue(AmazonSQS sqs, String queueURL){
		System.out.println("Deleting queue: " + queueURL);
        sqs.deleteQueue(new DeleteQueueRequest(queueURL));
	}
	
	
	
	
	/*************** Delete all existing queues under current SQS ***************/
	
	
	public static void deleteQueues(AmazonSQS sqs){
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
            System.out.println("Deleting queue: " + queueUrl);
            sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
        }
	}
	
	
	
	/*************** List existing queues in given SQS ***************/
	
	
	public static void ListAllQueues(AmazonSQS sqs){
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
            System.out.println("QueueUrl: " + queueUrl);
        }
        System.out.println();
	}
	
	
	/*************** Send message from queue ***************/
	
	
	public static void sendQueueMessage(AmazonSQS sqs, String QueueURL, MessageType messageType, MessageInterface msg){
    	switch (messageType){
    		case newTask:
    			System.out.println("Sending 'new task' message to LocalApplication <--> Manager queue");
    			SendMessageRequest send_msg_request1 = new SendMessageRequest().withQueueUrl(QueueURL).withMessageBody("newTask");
    			send_msg_request1.addMessageAttributesEntry("inputFileLocation", new MessageAttributeValue().withDataType("String").withStringValue(((newTaskMessage) msg).getInputFileLocation()));
    			sqs.sendMessage(send_msg_request1);
    			break;
    			
    		case doneTask:
    			System.out.println("Sending 'done task' message to LocalApplication <--> Manager queue");
    			SendMessageRequest send_msg_request2 = new SendMessageRequest().withQueueUrl(QueueURL).withMessageBody("doneTask");
    			send_msg_request2.addMessageAttributesEntry("outputFileLocation", new MessageAttributeValue().withDataType("String").withStringValue(((doneTaskMessage) msg).getSummaryFileLocation()));
    			sqs.sendMessage(send_msg_request2);
    			break;
    			
    		case newPDFTask:
    			System.out.println("Sending 'new PDF task' message to Manager <--> Workers queue");
    			SendMessageRequest send_msg_request3 = new SendMessageRequest().withQueueUrl(QueueURL).withMessageBody("newPDFTask");
    			send_msg_request3.addMessageAttributesEntry("PDF_URL", new MessageAttributeValue().withDataType("String").
						withStringValue(((newPDFTaskMessage) msg).getpdfURL()));
    			send_msg_request3.addMessageAttributesEntry("Operation", new MessageAttributeValue().withDataType("String").
															withStringValue(((newPDFTaskMessage) msg).getOperation()));
    			sqs.sendMessage(send_msg_request3);
    			break;
    			
    		case donePDFTask:
    			System.out.println("Sending 'done PDF task' message to Manager <--> Workers queue");
    			SendMessageRequest send_msg_request4 = new SendMessageRequest().withQueueUrl(QueueURL).withMessageBody("donePDFTask");
    			send_msg_request4.addMessageAttributesEntry("PDF_URL", new MessageAttributeValue().withDataType("String").
															withStringValue(((donePDFTaskMessage) msg).getpdfURL()));
    			send_msg_request4.addMessageAttributesEntry("Operation", new MessageAttributeValue().withDataType("String").
															withStringValue(((donePDFTaskMessage) msg).getOperationToPeform()));
    			send_msg_request4.addMessageAttributesEntry("Output_File_Location", new MessageAttributeValue().withDataType("String").
															withStringValue(((donePDFTaskMessage) msg).getOutputLocation()));
    			sqs.sendMessage(send_msg_request4);
    			break;
    			
    		default:
    			break;
    	}
	}
	
	
	
	/*************** Receive messages from queue ***************/
	
	
	public static List<Message> receiveMessages(AmazonSQS sqs, String QueueURL){
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QueueURL);
        //ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QueueURL).withMaxNumberOfMessages(1);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
        for (Message message : messages) {
        	Map<String, MessageAttributeValue> msgAttributes = message.getMessageAttributes();
        	System.out.println("" + msgAttributes.size() + " attributes.");
            System.out.println("  ** Message **");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
        System.out.println();
        //return msgAttributes;
        return messages;
	}
	
	
	
	/*************** Delete messages from queue ***************/
	
	
	public static void deleteMessages(AmazonSQS sqs, String QueueURL, List<Message> messages){
    	String messageReceiptHandle = messages.get(0).getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(QueueURL, messageReceiptHandle));
	}
	
}
