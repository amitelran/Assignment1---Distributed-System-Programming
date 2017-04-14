package com.amazonaws.samples;

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


public class Main {

	public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials("access-key","secret-access-key");
        AmazonEC2Client ec2 = (AmazonEC2Client) AmazonEC2ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        //AmazonEC2Client ec2 = new AmazonEC2Client(credentials); --> Deprecated method
        RunInstancesRequest request = new RunInstancesRequest();
        request.setInstanceType(InstanceType.M1Small.toString());
        request.setMinCount(1);
        request.setMaxCount(1);
        request.setImageId("ami-51792c38"); 	// Choosing an image which supports user-data scripts 'ami-51792c38' 
        request.setKeyName("linux-keypair");
        request.setUserData(getUserDataScript());
        ec2.runInstances(request);    			// Run the instance with the request we constructed
    }

	
	/* Get data script from user */
    private static String getUserDataScript(){
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");
        lines.add("curl http://www.google.com > google.html");
        lines.add("shutdown -h 0");
        String str = new String(Base64.getEncoder().encode(join(lines, "\n").getBytes()));
        return str;
    }

    
    /* Join lines of Base 64 text and return the stringified text*/
    static String join(Collection<String> s, String delimiter) {
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
