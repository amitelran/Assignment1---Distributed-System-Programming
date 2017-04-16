package SQS;

import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;
 
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
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
 
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));
        try {
        	// Create a Local Application <--> Manager queue
        	System.out.println("Creating a Local Application <--> Manager SQS queue");
            CreateQueueRequest createLocalAppQueueRequest = new CreateQueueRequest("localAppQueue"+ UUID.randomUUID());
            String localAppQueueUrl = sqs.createQueue(createLocalAppQueueRequest).getQueueUrl();
 
            // Create a Manager <--> Workers queue
            System.out.println("Creating a Manager <--> Workers SQS queue");
            CreateQueueRequest createInstancesQueueRequest = new CreateQueueRequest("instancesQueue"+ UUID.randomUUID());
            String instancesQueueUrl = sqs.createQueue(createInstancesQueueRequest).getQueueUrl();
 
            // List all queues in the account
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("QueueUrl: " + queueUrl);
            }
            System.out.println();
 
            // Send a message
            System.out.println("Sending a message to MyQueue.\n");
            sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text."));
 
            // Receive messages
            System.out.println("Receiving messages from MyQueue.\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
                System.out.println("  Message");
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
 
            // Delete a message
            System.out.println("Deleting a message.\n");
            String messageRecieptHandle = messages.get(0).getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
 
            // Deleting queues
            System.out.println("Deleting the Manager <--> Workers queue");
            sqs.deleteQueue(new DeleteQueueRequest(instancesQueueUrl));
            System.out.println("Deleting the Local Application <--> Manager queue");
            sqs.deleteQueue(new DeleteQueueRequest(localAppQueueUrl));
            
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

}
