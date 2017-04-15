package LocalApplication;

import java.io.File;
import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
 
public class EC2Launch {
    public static void main(String[] args) throws Exception {
        AmazonEC2 ec2;
        
        AWSCredentials credentials = new PropertiesCredentials(EC2Launch.class.getResourceAsStream("AwsCredentials.properties"));
        ec2 = new AmazonEC2Client(credentials);
 
        try {
            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
            RunInstancesRequest request = new RunInstancesRequest("ami-08728661", 1, 1);
            request.setInstanceType(InstanceType.T1Micro.toString());
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            System.out.println("Launch instances: " + instances);
 
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
 
    }
}