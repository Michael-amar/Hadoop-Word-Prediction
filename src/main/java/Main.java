import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
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
    private static final String S3_BUCKET_NAME = "distributed-systems-assignment-2";
    private static final String S3_BUCKET_URL = "s3n://" + S3_BUCKET_NAME + "/";
    private static final String OUTPUT_FOLDER_NAME = "output";
    private static final String FINAL_OUTPUT_FOLDER = S3_BUCKET_URL + OUTPUT_FOLDER_NAME;
    private static final String LOGS = S3_BUCKET_URL + "logs";
    private static final String _3_GRAM_DATASET = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
//    private static final String _3_GRAM_DATASET = "s3n://distributed-systems-assignment-2/3_grams.txt";
    private static final String READ_ONLY_FILE_EXTENSION = "-r-00000";

    public static String jarOfStep(int stepNum) {
        return String.format("s3n://%s/step%s.jar", S3_BUCKET_NAME, stepNum);
    }

    private static String outputFolderForStep(int step) {
        return "/Step" + step;
    }

    private static String outputFolderNameForStep(int step) {
        return outputFolderForStep(step) + outputFolderForStep(step) + READ_ONLY_FILE_EXTENSION;
    }

    /*
Step1:
hadoop jar /Users/michaelamar/Desktop/Hadoop-Word-Prediction/out/artifacts/Step1_jar/Hadoop-Word-Prediction.jar /hebrew-grams/3-grams /Step1

Step2:
hadoop jar /Users/michaelamar/Desktop/Hadoop-Word-Prediction/out/artifacts/Step2_jar/Hadoop-Word-Prediction.jar /Step1/Step1-r-00000 /Step2/

Step3:
hadoop jar /Users/michaelamar/Desktop/Hadoop-Word-Prediction/out/artifacts/Step3_jar/Hadoop-Word-Prediction.jar /Step1/Step1-r-00000 /Step3/

Step4:
hadoop jar /Users/michaelamar/Desktop/Hadoop-Word-Prediction/out/artifacts/Step4_jar/Hadoop-Word-Prediction.jar /Step1/Step1-r-00000 /Step4/

Step5:
hadoop jar /Users/michaelamar/Desktop/Hadoop-Word-Prediction/out/artifacts/Step5_jar/Hadoop-Word-Prediction.jar /Step3/Step3-r-00000 /Step4/Step4-r-00000 /Step5/

Step6:
hadoop jar /Users/michaelamar/Desktop/Hadoop-Word-Prediction/out/artifacts/Step6_jar/Hadoop-Word-Prediction.jar /Step5/Step5-r-00000 /Step6/
 */

    private static StepConfig getStep(int step) {
        HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig().withJar(jarOfStep(step));
        switch (step) {
            case 1:
                hadoopJarStepConfig = hadoopJarStepConfig.withArgs(_3_GRAM_DATASET, outputFolderForStep(step));
                break;
            case 2:
            case 3:
            case 4:
                hadoopJarStepConfig = hadoopJarStepConfig.withArgs(outputFolderNameForStep(1), outputFolderForStep(step));
                break;
            case 5:
                hadoopJarStepConfig = hadoopJarStepConfig.withArgs(outputFolderNameForStep(3), outputFolderNameForStep(4), outputFolderForStep(step));
                break;
            case 6:
                hadoopJarStepConfig = hadoopJarStepConfig.withArgs(outputFolderNameForStep(5), FINAL_OUTPUT_FOLDER);
                break;
        }

        return new StepConfig()
                .withName("step" + step)
                .withHadoopJarStep(hadoopJarStepConfig)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }

    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AmazonS3 S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
        ObjectListing filesInBucket = S3.listObjects(S3_BUCKET_NAME, OUTPUT_FOLDER_NAME);
        for (S3ObjectSummary s3ObjectSummary : filesInBucket.getObjectSummaries())
            S3.deleteObject(S3_BUCKET_NAME, s3ObjectSummary.getKey());

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("hw2")
                .withPlacement(new PlacementType("us-east-1a"))
                .withKeepJobFlowAliveWhenNoSteps(false);

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("HW2")
                .withInstances(instances)
                .withSteps(getStep(1), getStep(2), getStep(3), getStep(4), getStep(5), getStep(6))
                .withLogUri(LOGS)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult result = emr.runJobFlow(runFlowRequest);
        String id = result.getJobFlowId();
        System.out.println("our cluster id: " + id);
    }
}
