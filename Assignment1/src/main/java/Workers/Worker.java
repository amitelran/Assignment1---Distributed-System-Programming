package Workers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import SQS.SimpleQueueService;


public class Worker {

	public static void main(String[] args) throws IOException {

		AWSCredentials credentials = new PropertiesCredentials(Worker.class.getResourceAsStream("AwsCredentials.properties"));
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        String newPDFTaskQueueURL = null;					// 'newPDFTask|WorkerTermination' Manager <--> Workers queue
        String donePDFTaskQueueURL = null;					// 'donePDFTask' Manager <--> Workers queue
        PDFImageWriter pdf_Img_Writer = new PDFImageWriter();
        PDFText2HTML pdf_to_HTML_converter = new PDFText2HTML("UTF-8");
        PDFTextStripper pdf_to_text_converter = new PDFTextStripper("UTF-8");
        
        
        
        /*	************** Get 'newPDFTask|WorkerTermination' and 'donePDFTask' Manager <--> Workers SQS queues ************** */	
        
        
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SimpleQueueService.class.getResourceAsStream("AwsCredentials.properties")));	// Declare SQS client
        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
        	String queueName = queueUrl.split("\\_")[0];								// Get queue name from beginning of queue URL up to '_' delimeter
            if (queueName == "newPDFTaskQueue"){
            	newPDFTaskQueueURL = sqs.getQueueUrl(queueName).getQueueUrl();			// Get queue URL by queue name
            }
            else if (queueName == "donePDFTaskQueue"){
            	donePDFTaskQueueURL = sqs.getQueueUrl(queueName).getQueueUrl();			// Get queue URL by queue name
            }
        	System.out.println("QueueUrl: " + queueUrl);
        }
        
        
        
        /*	************** Loop until termination message from Manager **************	*/
        
        
        while(true){
        	System.out.println("Receiving messages from " + newPDFTaskQueueURL + " SQS queue\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(newPDFTaskQueueURL);
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
                if (message.getBody() == "newPDFTask"){				// Received a "newTask" message from local application
                	
                	
                	continue;
                }
            }
        }
	}

	
	
}
