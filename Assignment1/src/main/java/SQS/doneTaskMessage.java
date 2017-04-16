package SQS;


/* Message from the manager to the application 
 * (S3 location of the output summary file)
 */
public class doneTaskMessage implements MessageInterface {
	private String summaryFileLocation;
	
	
	public doneTaskMessage(String summFileLoc){
		this.summaryFileLocation = summFileLoc;
	}
	
	public String getSummaryFileLocation(){
		return this.summaryFileLocation;
	}
}
