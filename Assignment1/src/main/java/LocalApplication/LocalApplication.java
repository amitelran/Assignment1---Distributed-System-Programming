package LocalApplication;

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

public class LocalApplication {

    public static void main(String[] args) throws Exception {
        AmazonEC2 ec2;
        
        AWSCredentials credentials = new PropertiesCredentials(EC2Launch.class.getResourceAsStream("AwsCredentials.properties"));
        ec2 = new AmazonEC2Client(credentials);
 
        try {
        	DescribeInstancesRequest listingRequest = new DescribeInstancesRequest();

        	List<String> valuesT1 = new ArrayList<String>();
        	valuesT1.add("0");
        	Filter filter1 = new Filter("tag:Manager", valuesT1);

        	DescribeInstancesResult result = ec2.describeInstances(listingRequest.withFilters(filter1));
        	List<Reservation> reservations = result.getReservations();

        	if(reservations.size() == 0){
              RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1);
              request.setInstanceType(InstanceType.T1Micro.toString());
              
              List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
              
              String instanceId = instances.get(0).getInstanceId();
              
              
              CreateTagsRequest tagRequest = new CreateTagsRequest();
              tagRequest = tagRequest.withResources(instanceId)
                               .withTags(new Tag("Manager", "0"));
              ec2.createTags(tagRequest);
              
              System.out.println("Launch instances: " + instances);
        	}
        	else
        		System.out.println("Manager already exists!");

 
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
 
    }
}
