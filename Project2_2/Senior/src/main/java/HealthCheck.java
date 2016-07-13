import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * Created by Zhangwei on 2/12/16.
 */
public class HealthCheck implements Runnable {
    private final DataCenterInstance[] instances;

    public HealthCheck(DataCenterInstance[] instances) {
        this.instances = instances;
    }

    public void run() {
        while (true) {
            try {
                //health check interval
                Thread.sleep(1000);
                for (int i = 0; i < 3; i++) {
                    String dnsUrl = instances[i].getUrl();
                    // when the dns is not empty
                    if (!dnsUrl.isEmpty()) {
                        String url = dnsUrl + "/lookup/random";
                        //if the instance is unhealthy
                        if (!isHealthy(url)) {
                            //set url to empty
                            instances[i].setUrl("");
                            System.out.println("set " + i + ": " +instances[i].getUrl());
                            //create a new data center
                            String dataCenterDNS = CreateDataCenter();
                            //add new data center to instances
                            instances[i].setUrl("http://" + dataCenterDNS);
                            System.out.println("instance "+ i + ": " + instances[i].getUrl());
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Send 5 requests to the url, if non of them has a 200 respond code, then the instance is unhealthy
     *
     * @param urlString the url to send request ro
     * @return whether is healthy or not
     */
    public static boolean isHealthy(String urlString) {
        for (int i = 0; i < 5; i++) {
            try {
                URL url = new URL(urlString);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                //set timeout limit
                connection.setConnectTimeout(1500);
                if (connection.getResponseCode() == 200) {
                    return true;
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
        return false;
    }

    /**
     * Create a new data center.
     *
     * @return new data center DNS
     * @throws IOException
     * @throws InterruptedException
     */
    public static String CreateDataCenter() throws IOException, InterruptedException {
        //Load the Properties File with AWS Credentials
        Properties properties = new Properties();
        properties.load(HealthCheck.class.getResourceAsStream("/AwsCredentials.properties"));

        BasicAWSCredentials bawsc =
        new BasicAWSCredentials(properties.getProperty("accessKey"),
            properties.getProperty("secretKey"));

        //Create an Amazon EC2 Client
        AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);

        Instance dataCenter = createInstance(ec2, "ami-6f486605", "m3.medium", "myaws", "all");
        String dataCenterID = dataCenter.getInstanceId();

        String dataCenterDNS = null;

        //Get public DNS of load generator and data center when they are in running state
        while (dataCenterDNS == null || dataCenterDNS.isEmpty()) {
            Thread.sleep(60000);
            dataCenterDNS = getDnsName(ec2, dataCenterID);
        }
        System.out.println("dc-dns: " + dataCenterDNS);
        //wait for the data center to be in service
        Thread.sleep(50000);
        return dataCenterDNS;
    }

    /**
     * Create a new instance.
     *
     * @param ec2     AmazonEC2Client
     * @param imgID   image ID
     * @param type    instance type
     * @param keyName key name
     * @param group   security group id
     * @return instance that created
     */
    public static Instance createInstance(AmazonEC2 ec2, String imgID, String type, String keyName, String group) {

        //Create Instance Request
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

        //Configure Instance Request
        runInstancesRequest.withImageId(imgID)
        .withInstanceType(type)
        .withMinCount(1)
        .withMaxCount(1)
        .withKeyName(keyName)
        .withSecurityGroups(group);

        //Launch Instance
        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);


        //Return the Object Reference of the Instance just Launched, get Instance ID
        Instance instance = runInstancesResult.getReservation().getInstances().get(0);

        //Add a Tag to the Instance
        CreateTagsRequest createTagsRequest = new CreateTagsRequest();
        createTagsRequest.withResources(instance.getInstanceId())
        .withTags(new Tag("Project", "2.2"));

        ec2.createTags(createTagsRequest);

        //Print Instance ID
        System.out.println("Just launched an Instance with ID: " + instance.getInstanceId());

        return instance;
    }

    /**
     * Get Instance DNS name
     *
     * @param ec2        AmazonEC2Client
     * @param instanceId instance id
     * @return return the instance DNS name
     */
    public static String getDnsName(AmazonEC2Client ec2, String instanceId) {

        DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.getReservations();

        for (Reservation reservation : reservations) {
            for (Instance instance : reservation.getInstances()) {
                if (instance.getInstanceId().equals(instanceId))
                    return instance.getPublicDnsName();
            }
        }
        return null;
    }
}
