package EC2Manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import LocalApplication.LocalApplication;
import S3.S3;

public class EC2Manager {

	public static void main(String[] args) throws IOException {

        AWSCredentials credentials = new PropertiesCredentials(EC2Manager.class.getResourceAsStream("AwsCredentials.properties"));
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        int numOfWorkers = 0;
		
        AmazonS3 s3 = new AmazonS3Client(credentials);
        File inputFile = s3.S3downloadObject
        try {
			System.out.println("Creating Worker instance...");
			RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1);
			request.setInstanceType(InstanceType.T1Micro.toString());
			
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			String instanceId = instances.get(0).getInstanceId();
			
			CreateTagsRequest tagRequest = new CreateTagsRequest();
			tagRequest = tagRequest.withResources(instanceId).withTags(new Tag("Worker", Integer.toString(numOfWorkers)));		// Set tag of a worker and a number value
			ec2.createTags(tagRequest);
			numOfWorkers++;
			  
			System.out.println("Launch instances: " + instances);
		} 
        catch (AmazonServiceException ase) {
	        System.out.println("Caught Exception: " + ase.getMessage());
	        System.out.println("Reponse Status Code: " + ase.getStatusCode());
	        System.out.println("Error Code: " + ase.getErrorCode());
	        System.out.println("Request ID: " + ase.getRequestId());
        }
	}

}
