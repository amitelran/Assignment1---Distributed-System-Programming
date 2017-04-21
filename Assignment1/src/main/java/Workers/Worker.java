package Workers;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.sun.org.apache.xml.internal.utils.URI;

import SQS.SimpleQueueService;


public class Worker {

	public static void main(String[] args) throws IOException {

		AWSCredentials credentials = new PropertiesCredentials(Worker.class.getResourceAsStream("AwsCredentials.properties"));
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        AmazonS3 s3 = new AmazonS3Client(credentials);		// S3 Storage client
        AmazonSQS sqs = new AmazonSQSClient(credentials);	// SQS client
        String newPDFTaskQueueURL = null;					// 'newPDFTask|WorkerTermination' Manager <--> Workers queue
        String donePDFTaskQueueURL = null;					// 'donePDFTask' Manager <--> Workers queue
        String operation = null;							// Operation to perform over given PDF URL
        String pdfURL = null;								// Given PDF URL
        String newFileURL = null;							// URL of a new file (after operating over input file)
        String inputFileKey = null;							// The key (name) of the input file originating the PDF task
        String outputBucketName = "outputworkersbucketamityoav";		// Workers output files bucket name
        
        
        
        /*	************** Get 'newPDFTask|WorkerTermination' and 'donePDFTask' Manager <--> Workers SQS queues ************** */	
        
        
        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
        	URI uri = new URI(queueUrl);
        	String path = uri.getPath();
        	String queueName = path.substring(path.lastIndexOf('/') + 1); 
        	System.out.println("queuename: " + queueName);
            if (queueName.equals("newPDFTaskQueue")){
            	newPDFTaskQueueURL = queueUrl;			
            }
            else if (queueName.equals("donePDFTaskQueue")){
            	donePDFTaskQueueURL = queueUrl;			
            }
        	System.out.println("QueueUrl: " + queueUrl);
        }
        
        
        
        /*	************** Loop until termination message from Manager **************	*/
        
        
        while(true){
        	
        	System.out.println("Retreiving messages from " + newPDFTaskQueueURL + " SQS queue\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(newPDFTaskQueueURL).withMessageAttributeNames("All");
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
                printMessage(message);
            	
            	/*	************** Received a "newPDFTask" message from Manager **************
                 *  - Download the PDF file indicated in the message.
                 *  - Perform the operation requested on the file.
                 *  - Upload the resulting output file to S3.
                 *  - Put a message in the 'donePDFTaskQueue' indicating:
                 *  	 -- the original URL of the PDF
                 *  	 -- the S3 url of the new image file, and the operation that was performed.
                 *  	 -- remove the processed message from the SQS queue.
              	*/
            	
            	
                if (message.getBody().equals("newPDFTask")){							// Received a "newPDFTask" message from Manager instance
                	for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                		if (entry.getKey().equals("inputFileKey")){						// Get the input file key
                			inputFileKey = entry.getValue().getStringValue();
                		}
                		else if (entry.getKey().equals("Operation")){					// Get the operation to perform on the PDF
                			operation = entry.getValue().getStringValue();
                		}
                		else if (entry.getKey().equals("PDF_URL")){						// Get the PDF's URL
                			pdfURL = entry.getValue().getStringValue();
                		}
                    }
                	newFileURL = handleTask(s3, operation, pdfURL);
                	continue;
                }
            }
        }
	}

	
	
	/**	************** Handle given task according to operation ************* ***/
       
	
	
    private static String handleTask(AmazonS3 s3, String operation, String pdfURL, String destination) throws IOException{
    	String newFileURL = null;
    	File inputPDFFile = null;
    	PDDocument pdfFile = null;
    	PDPage firstPage = null;
    	
    	/* *** Retrieve PDF file *** */
    	
    	try {
    		URL url = new URL(pdfURL);
    		FileUtils.copyURLToFile(url, inputPDFFile);				// Copy URL to file
    		pdfFile = PDDocument.load(inputPDFFile);				// Load PDF file
	        firstPage = pdfFile.getPage(0);							// Get first page of PDF file
    	}
    	catch (Exception e){
    		newFileURL = "<" + e.getMessage() + ">";
    		return newFileURL;
    	}
    		
    	
    	/*	*** Perform "ToImage" over PDF - convert the first page of the PDF file to a "png" image *** */
    	
    	if (operation.equals("ToImage")){
    		try {
    			PDFImageWriter pdf_Img_Writer = new PDFImageWriter();
    			pdf_Img_Writer.writeImage(pdfFile, "png", null, 1, 1, "picture");		// (pdfDocument, imageFormat, password, startPage, endPage, outputPrefix used to construct the filename for individual images)
    	        //PutObjectRequest req = new PutObjectRequest(destination, "picture" + pdfFile.getName(), pdfFile);
    	        //s3.putObject(req);
    		}
    		catch (Exception e){
    			System.err.println("Error: " + e.getMessage());
    			newFileURL = "<" + e.getMessage() + ">";
    		}
    	}
    	
    	/*	*** Perform "ToHTML" over PDF - convert the first page of the PDF file to an HTML file *** */
    	
    	else if (operation.equals("ToHTML")){
    		PDFText2HTML pdf_to_HTML_converter = new PDFText2HTML("UTF-8");
	    	try {
		    	
	    	}
	    	catch (Exception e){
	    		System.err.println("Error: " + e.getMessage());
	    		newFileURL = "<" + e.getMessage() + ">";
	    	}
    	}
    	
    	/*	*** Perform "ToText" over PDF - convert the first page of the PDF file to a text file *** */
    	
    	else if (operation.equals("ToText")){
    		PDFTextStripper pdf_to_text_converter = new PDFTextStripper("UTF-8");
    		Writer output = new OutputStreamWriter(new FileOutputStream(), "UTF-8");
	    	try {
	    		pdf_to_text_converter.writeText(pdfFile, outputStream);
	    	}
	    	catch (Exception e){
	    		System.err.println("Error: " + e.getMessage());
	    		newFileURL = "<" + e.getMessage() + ">";
	    	}
    	}
        return newFileURL;
    }
    
    
	
	/**	************** Send 'DonePDFTask' message to Manager <--> Workers 'donePDFTaskQueue' queue **************	**/
    
    
    private static void sendDonePDFTask(AmazonSQS sqs, String donePDFTaskQueueURL, String originalURL, String operation, String newFileURL){
    	SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(donePDFTaskQueueURL).withMessageBody("donePDFTask");
        send_msg_request.addMessageAttributesEntry("originalURL", new MessageAttributeValue().withDataType("String").withStringValue(originalURL));
        send_msg_request.addMessageAttributesEntry("Operation", new MessageAttributeValue().withDataType("String").withStringValue(operation));
        send_msg_request.addMessageAttributesEntry("newFileURL", new MessageAttributeValue().withDataType("String").withStringValue(newFileURL));
        
		sqs.sendMessage(send_msg_request);
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
        for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
            System.out.println("  * User made Attribute *");
            System.out.println("    Name:  " + entry.getKey());
            System.out.println("    String Value: " + entry.getValue().getStringValue());
        }
        System.out.println();
    }
	
}
