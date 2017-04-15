package LocalApplication;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;


public class LocalApplication {

	public static void main(String[] args) {
		System.out.println("Check");
		
    }
	
	
	
	/* Passing user-data shell-scripts to the remote machine, to be run when the machine starts up.
	 * The script being passed is encoded to base64 
	 */
	public static void EC2Launch(){
        AWSCredentials credentials = new BasicAWSCredentials("access-key","secret-access-key");
        AmazonEC2Client ec2 = (AmazonEC2Client) AmazonEC2ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        //AmazonEC2Client ec2 = new AmazonEC2Client(credentials); --> Deprecated method
        RunInstancesRequest request = new RunInstancesRequest();
        request.setInstanceType(InstanceType.M1Small.toString());		// Set instance type to m1small in string representation (general purpose instance - resource balanced in terms of CPU, nmemory and network resources)
        request.setMinCount(1);											// Set minimum number of instances to launch
        request.setMaxCount(1);											// Set maximum number of instances to launch
        request.setImageId("ami-51792c38"); 							// Choosing an image which supports user-data scripts 'ami-51792c38' 
        request.setKeyName("linux-keypair");							// Name of the key pair
        request.setUserData(getUserDataScript());						// The user data to make available to the instance
        ec2.runInstances(request);    									// Run the instance with the request we constructed
	}

	
	
	/* Get data script from user */
    private static String getUserDataScript(){
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");				// User data starting with '#!' getting the instance to run as the root user on the first boot
        lines.add("curl http://www.google.com > google.html");
        lines.add("shutdown -h 0");
        String str = new String(Base64.getEncoder().encode(join(lines, "\n").getBytes()));
        return str;
    }

    
    
    /* Join lines of Base 64 text and return the stringified text*/
    public static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

}
