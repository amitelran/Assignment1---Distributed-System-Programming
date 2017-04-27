package Workers;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.util.ImageIOUtil;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.sun.org.apache.xml.internal.utils.URI;


/**	**************************  Worker Flow  **************************

 * Repeatedly:
		- Get a message from an SQS queue.
		- Download the PDF file indicated in the message.
		- Perform the operation requested on the file.
		- Upload the resulting output file to S3.
		- Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new image file, and the operation that was performed.
		- Remove the processed message from the SQS queue.

IMPORTANT:
	- If an exception occurs, then the worker should recover from it, send a message to the manager of the input message that caused the exception together with a short description of the exception, and continue working on the next message.
	- If a worker stops working unexpectedly before finishing its work on a message, then some other worker should be able to handle that message.
	
 * *********************************************************************
 */


public class Worker {

	public static void main(String[] args) throws IOException {
		
		AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();			// Search and get credentials file in system
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        String newPDFTaskQueueURL = null;					// 'newPDFTask|WorkerTermination' Manager <--> Workers queue
        String donePDFTaskQueueURL = null;					// 'donePDFTask' Manager <--> Workers queue
        String operation = null;							// Operation to perform over given PDF URL
        String pdfURL = null;								// Given PDF URL
        String newFileURL = null;							// URL of a new file (after operating over input file)
        String inputFileKey = null;							// The key (name) of the input file originating the PDF task
        String outputBucketName = "outputworkersbucketamityoav";
        
        
        /*	************** Get 'newPDFTask|WorkerTermination' and 'donePDFTask' Manager <--> Workers SQS queues ************** */	
        
        
        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
        	URI uri = new URI(queueUrl);
        	String path = uri.getPath();
        	String queueName = path.substring(path.lastIndexOf('/') + 1); 
            if (queueName.equals("newPDFTaskQueue")){
            	newPDFTaskQueueURL = queueUrl;			
            }
            else if (queueName.equals("donePDFTaskQueue")){
            	donePDFTaskQueueURL = queueUrl;			
            }
        }
        
        
        
        /*	************** Loop until termination message from Manager **************	*/
        
        
        while(true){
        	
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
                	newFileURL = handleTask(s3, outputBucketName, inputFileKey, operation, pdfURL);
                	sendDonePDFTask(sqs, donePDFTaskQueueURL, pdfURL, operation, newFileURL, inputFileKey);		// Send 'Done PDF Task' message to Manager <--> Workers queue
                	String messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(newPDFTaskQueueURL, messageReceiptHandle));		// Delete 'newPDFTask' message from 'newTaskQueue'
                	continue;
                }
            }
        }
	}

	
	
	/**	************** Handle given task according to operation ************* ***/
       
	
	
    private static String handleTask(AmazonS3 s3, String bucketName, String inputFileKey, String operation, String pdfURL) throws IOException{
    	String newFileURL = null;
    	PDDocument pdfFile = null;
    	String outputFileKey = null;
    	File outputFile = null;
    	
    	/* *** Retrieve PDF file *** */
    	
    	try {
    		String[] items = pdfURL.split("/");
    		outputFileKey = items[items.length - 1];									// Get PDF file name
    		int index = outputFileKey.lastIndexOf(".");
    		outputFileKey = outputFileKey.substring(0, index);							// Get PDF URL and rid of file suffix
    		pdfFile = getPDF(pdfURL, outputFileKey);
    	}
    	catch (Exception e){
    		if (e.getMessage().equals(pdfURL)){
    			newFileURL = "<Error 404: Web page not found>";
    			System.err.println("Error 404: Web page not found\n");
    		}
    		else{
    			newFileURL = "<" + e.getMessage() + ">";
    			System.err.println("Error: " + e.getMessage() + "\n");
    		}
    		if (pdfFile != null){
        		pdfFile.close();
        	}
    		return newFileURL;
    	}
    		
    	/*	*** Perform "ToImage" over PDF - convert the first page of the PDF file to a "png" image *** */
    	
    	if (operation.equals("ToImage")){
    		try {
    			System.out.println("Converting PDF file's first page to png Image...");
    			PDPage firstPage = (PDPage)pdfFile.getDocumentCatalog().getAllPages().get(0);			// Get PDF's first page	
    			BufferedImage bufferedImg = firstPage.convertToImage(BufferedImage.TYPE_INT_RGB, 300);	// Convert page to a buffered image (8-bit RGB color, 300 resolution)
    			outputFileKey += "_firstPage.png";														// Set file suffix as .png format
    			outputFile = new File(outputFileKey);
    			System.out.println("New File key with .png suffix: " + outputFileKey);
    	    	ImageIOUtil.writeImage(bufferedImg, outputFileKey, 300);								// (image to be written, filename with suffix as image format, resolution in dpi - dots per inch)
    	    	System.out.println();
    		}
    		catch (Exception e){
    			newFileURL = "<" + e.getMessage() + ">";
	    		System.err.println("Error: " + e.getMessage() + "\n");
	    		if (pdfFile != null){
	        		pdfFile.close();
	        	}
	    		return newFileURL;
    		}
    	}
    	
    	/*	*** Perform "ToHTML" over PDF - convert the first page of the PDF file to an HTML file *** */
    	
    	else if (operation.equals("ToHTML")){
	    	try {
	    		PDFText2HTML pdf_to_HTML_converter = new PDFText2HTML("UTF-8");
	    		System.out.println("Converting PDF file's first page to HTML file...");
	    		outputFileKey += ".html";
	    		pdf_to_HTML_converter.setStartPage(1);
	    		pdf_to_HTML_converter.setEndPage(1);
	    		String parsedText = pdf_to_HTML_converter.getText(pdfFile);
	    		outputFile = new File(outputFileKey);
	    		FileWriter writer = new FileWriter(outputFileKey);
	            BufferedWriter out = new BufferedWriter(writer);
	    		out.write(parsedText);
	    		out.close();						// Close the output stream
	    		System.out.println();
	    	}
	    	catch (Exception e){
	    		newFileURL = "<" + e.getMessage() + ">";
	    		System.err.println("Error: " + e.getMessage() + "\n");
	    		if (pdfFile != null){
	        		pdfFile.close();
	        	}
	    		return newFileURL;
	    	}
    	}
    	
    	/*	*** Perform "ToText" over PDF - convert the first page of the PDF file to a text file *** */
    	
    	else if (operation.equals("ToText")){
	    	try {
	    		PDFTextStripper pdf_to_text_converter = new PDFTextStripper("UTF-8");
	    		System.out.println("Converting PDF file's first page to a text file...");
	    		pdf_to_text_converter.setStartPage(1);
	    		pdf_to_text_converter.setEndPage(1);
	    		String parsedText = pdf_to_text_converter.getText(pdfFile);
	    		outputFileKey += ".txt";
	    		System.out.println("New File key with txt suffix: " + outputFileKey);
	    		outputFile = new File(outputFileKey);
	    		FileWriter writer = new FileWriter(outputFileKey);
	            BufferedWriter out = new BufferedWriter(writer);
	    		out.write(parsedText);
	    		out.close();						// Close the output stream
	    		System.out.println();
	    	}
	    	catch (Exception e){
	    		newFileURL = "<" + e.getMessage() + ">";
	    		System.err.println("Error: " + e.getMessage() + "\n");
	    		if (pdfFile != null){
	        		pdfFile.close();
	        	}
	    		return newFileURL;
	    	}
    	}
    	
    	/*	*** Close pdfFile, upload output file to S3 Storage, return newFileURL *** */
    	
    	if (pdfFile != null){
    		pdfFile.close();
    	}
    	if (outputFileKey != null){
    		PutObjectRequest req = new PutObjectRequest(bucketName, outputFileKey, outputFile);
            s3.putObject(req);
            s3.setObjectAcl(bucketName, outputFileKey, CannedAccessControlList.PublicRead);
            URL outputFileURL = s3.getUrl(bucketName, outputFileKey);
            newFileURL = outputFileURL.toString();
    	}
        System.out.println("New File URL: " + newFileURL + "\n");
        return newFileURL;
    }
    
    
    
    
    /**	************** Download PDF document given URL ************* 
     * @throws IOException 
     * @throws MalformedURLException ***/
    
    
    private static PDDocument getPDF(String pdfURL, String outputFileKey) throws MalformedURLException, IOException{
    	
    	// *****************************
    	
    	/*URLConnection connection = new URL(pdfURL).openConnection();
		connection.addRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
		connection.connect();
		
		BufferedReader r  = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
		File inputPDFFile = new File(outputFileKey + ".pdf");
        FileOutputStream fis = new FileOutputStream(inputPDFFile);
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = r.readLine()) != null) {
		    sb.append(line);
		    System.out.println(sb.toString());
		}
		fis.close();
		r.close();*/
		// *****************************
 		
    	
    	URL url = new URL(pdfURL);
    	File inputPDFFile = new File(outputFileKey + ".pdf");
		FileUtils.copyURLToFile(url, inputPDFFile);									// Copy URL to file
    	return PDDocument.loadNonSeq(inputPDFFile, null);							// Parse PDF with non sequential parser (checks valid objects and parse only these objects)
    }
    
    
	
	/**	************** Send 'DonePDFTask' message to Manager <--> Workers 'donePDFTaskQueue' queue **************	**/
    
    
    private static void sendDonePDFTask(AmazonSQS sqs, String donePDFTaskQueueURL, String originalURL, String operation, String newFileURL, String inputFileKey){
    	SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(donePDFTaskQueueURL).withMessageBody("donePDFTask");
        send_msg_request.addMessageAttributesEntry("originalURL", new MessageAttributeValue().withDataType("String").withStringValue(originalURL));
        send_msg_request.addMessageAttributesEntry("Operation", new MessageAttributeValue().withDataType("String").withStringValue(operation));
        send_msg_request.addMessageAttributesEntry("newFileURL", new MessageAttributeValue().withDataType("String").withStringValue(newFileURL));
        send_msg_request.addMessageAttributesEntry("inputFileKey", new MessageAttributeValue().withDataType("String").withStringValue(inputFileKey));
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
        System.out.println("   **************************************************");
        System.out.println();
    }
	
}
