package SQS;


/* Message from a worker to the manager 
 * (S3 location of the output file, the operation performed, and the URL of the PDF file)
 */
public class donePDFTaskMessage implements MessageInterface {
	private String outputFileLocation;
	private String operationToPerform;
	private String pdfURL;
	
	
	// Constructor params: output file location in S3 storage, the URL of the PDF, the operation performed on pdf
	public donePDFTaskMessage(String outputFileLoc, String opToPerform, String URL){
		this.outputFileLocation = outputFileLoc;
		this.operationToPerform = opToPerform;
		this.pdfURL = URL;
	}
	
	// Output file location getter
	public String getOutputLocation(){
		return this.outputFileLocation;
	}
	
	// PDF URL getter
	public String getpdfURL(){
		return this.pdfURL;
	}
	
	// Operation on PDF getter
	public String getOperationToPeform(){
		return this.operationToPerform;
	}
}
