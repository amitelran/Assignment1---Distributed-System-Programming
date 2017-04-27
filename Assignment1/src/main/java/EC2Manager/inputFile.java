package EC2Manager;


/*	************** Input file class **************	*/


public class inputFile {
	private String file_Bucket;
	private String file_Key;
	private String file_QueueURL;
	private int num_of_tasks;
	private int num_of_completed_tasks;
	
	public inputFile(String fileBucket, String fileKey, String queueURL, int numTasks){
		this.file_Bucket = fileBucket;
		this.file_Key = fileKey;
		this.file_QueueURL = queueURL;
		this.num_of_tasks = numTasks;
		this.num_of_completed_tasks = 0;
	}
	
	public int getNumOfTasks(){
		return this.num_of_tasks;
	}
	
	public String getFileBucket(){
		return this.file_Bucket;
	}
	
	public String getFileKey(){
		return this.file_Key;
	}
	
	public String getFileQueueURL(){
		return this.file_QueueURL;
	}
	
	public void incCompletedTasks(){
		System.out.println(++this.num_of_completed_tasks);
	}
	
	public boolean isDone(){
		return this.num_of_tasks == this.num_of_completed_tasks;
	}
}
