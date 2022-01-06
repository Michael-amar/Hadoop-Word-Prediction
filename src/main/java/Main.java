import com.amazonaws.regions.Regions;
import org.apache.hadoop.mapreduce.TestMapCollection.StepFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Main {

    private static final Regions REGION = Regions.US_EAST_1;
    private static final String OUTPUT = "output";
    private static final String S3_BUCKET_NAME = "distributed-systems-assignment-2";
    private static final String S3_BUCKET_URL = "S3://" + S3_BUCKET_NAME + "/";
    private static final String LOGS = S3_BUCKET_URL + "logs";
    StepFactory stepFactory = new StepFactory();

    private static String jarOfStep(int stepNum) {
        return String.format("s3://%s/step%s.jar", S3_BUCKET_NAME, stepNum);
    }

    private static String ngramDatasetFor(int n) {
        return String.format("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/%sgram/data", n);
    }

    private static StepConfig getStep(int step) {
        HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig().withJar(jarOfStep(step));
        switch (step) {
            case 1:
            case 2:
            case 3:
                hadoopJarStepConfig = hadoopJarStepConfig.withArgs(ngramDatasetFor(step), null);
                break;
            case 4:
            case 5:
                hadoopJarStepConfig = hadoopJarStepConfig.withArgs();
                break;
            case 6:
                hadoopJarStepConfig = hadoopJarStepConfig.withArgs(null, S3_BUCKET_URL + OUTPUT);
                break;
        }

        return new StepConfig()
                .withName("step" + step)
                .withHadoopJarStep(hadoopJarStepConfig)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }

    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
        AmazonS3 S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
        ObjectListing filesInBucket = S3.listObjects(S3_BUCKET_NAME, OUTPUT);
        for (S3ObjectSummary s3ObjectSummary : filesInBucket.getObjectSummaries())
            S3.deleteObject(S3_BUCKET_NAME, s3ObjectSummary.getKey());

        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("temp") //TODO
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));


        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("HW2")
                .withInstances(instances)
                .withSteps(getStep(1), getStep(2), getStep(3), getStep(4), getStep(5), getStep(6))
                .withLogUri(LOGS);

        emr.runJobFlow(runFlowRequest);
    }
}
