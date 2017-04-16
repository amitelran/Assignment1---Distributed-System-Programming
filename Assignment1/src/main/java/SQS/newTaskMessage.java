package SQS;


/* Message from the application to the manager 
 * (location of an input file with a list of PDF URLs and opertaions to perform) 
 * */
public class newTaskMessage implements MessageInterface {
	private String inputFileLocation;
	
	
	// Constructor params: input file location in S3
	public newTaskMessage(String inpFileLocation){
		this.inputFileLocation = inpFileLocation;
	}
	
	// Getter of input file location
	public String getInputFileLocation(){
		return this.inputFileLocation;
	}
}
