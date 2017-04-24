package EC2Manager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;

public class globalVars {
    public static AmazonEC2 ec2;									// EC2 client
    public static AmazonS3 s3;										// S3 Storage client
    public static AmazonSQS sqs;									// SQS client
    public static int numOfWorkers = 0;								// Number of created Worker instances
    public static int numOfPDFperWorker = 0;						// An integer stating how many PDF files per worker, as determined by user
    public static int numOfMessages = 0;							// SQS message counter
    public static boolean terminateSignal = false;					// Termination signal indicator
    public static String newPDFTaskQueueURL = null;					// 'newPDFTask|Termination' Manager <--> Workers queue
    public static String newTaskQueueURL = null;					// 'newTask|Termination' Local Applications <--> Manager queue
    public static Map<String, inputFile> inputFilesMap = new HashMap<String, inputFile>();			// <input file unique key, inputFile object> Map
    public static Map<String, List<String>> outputFilesMap = new HashMap<String, List<String>>();		// Map from corresponding input file, to List of finished tasks (which we will write in the end to a summary output file)
        
}
