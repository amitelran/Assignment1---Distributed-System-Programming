package SQS;


/* Message from the manager to the workers 
 * (URL of a specific PDF file together with an operation to perform on it)
 */
public class newPDFTaskMessage implements MessageInterface {
	private String pdfURL;
	private String operation;
	
	
	// Constructor params: PDF URL, operation to perform on PDF
	public newPDFTaskMessage(String URL, String op){
		this.pdfURL = URL;
		this.operation = op;
	}
	
	
	// PDF URL getter
	public String getpdfURL(){
		return this.pdfURL;
	}
	
	
	// Operation getter
	public String getOperation(){
		return this.operation;
	}
}
