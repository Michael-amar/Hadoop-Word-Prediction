import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Step4
{
    public static class MapperClass extends Mapper<LongWritable, Text, CombinedKey, IntWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
        {
            String[] fields = value.toString().split("\t");
            String[] three_gram = fields[0].split(" ");
            IntWritable count = new IntWritable(Integer.parseInt(fields[1]));
            context.write(new CombinedKey(three_gram[1], three_gram[2], Gram.oneGram("*")), count);
            context.write(new CombinedKey(three_gram[1], three_gram[2], Gram.threeGram(three_gram[0], three_gram[1], three_gram[2])), count);
        }
    }

    public static class CombinerClass extends Reducer<CombinedKey, IntWritable, CombinedKey, IntWritable>
    {
        @Override
        public void reduce(CombinedKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum=0;
            for(IntWritable value : values)
                sum += value.get();
            context.write(key,new IntWritable(sum));
        }
    }

    public static class ReducerClass extends Reducer<CombinedKey,IntWritable, Gram, DoubleWritable>
    {
        public HashMap<String, Integer> one_grams= new HashMap<String,Integer>();
        int N2 = 0;

        /* This function was taken from stack overflow*/
        public void setup(Reducer.Context context) throws IOException {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            RemoteIterator<LocatedFileStatus> it=fileSystem.listFiles(new Path("/Step2"),false);
            while(it.hasNext()){
                LocatedFileStatus fileStatus=it.next();
                if (fileStatus.getPath().getName().startsWith("Step2")){
                    FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, "UTF-8"));
                    String line=null;
                    while ((line = reader.readLine()) != null)
                    {
                        String[] fields = line.split("\t");
                        one_grams.put(fields[0], Integer.parseInt(fields[1]));
                    }
                    reader.close();
                }
            }
        }

        @Override
        public void reduce(CombinedKey key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException
        {
            int sum = 0;
            for(IntWritable value : values)
                sum += value.get();

            // if key.gram is one-gram then the key is: "<w2,w3>, * " so the sum is N2
            if (key.getGram().getTag().toString().equals(Gram.oneGramString))
                N2 = sum;

            // the key is: "<w2,w3>, <w1,w2,w3>" and the sum is  N3
            else{
                double N3 = sum;
                double k2 = (Math.log(N2 +1) + 1) / (Math.log(N2+1) + 2);
                double k3 = (Math.log(N3 + 1) + 1 ) / (Math.log(N3+1) + 2);
                double C1 = one_grams.get(key.getKey1().toString());
                double N1 = one_grams.get(key.getKey2().toString());
                double C0 = one_grams.get("*");
                context.write(key.getGram(),new DoubleWritable(
                        ((1-k3)*k2*N2) / C1 +
                              ((1-k3)*(1-k2)*(N1)) / C0
                        ));
            }

        }
    }

    public static class PartitionerClass extends Partitioner<CombinedKey, IntWritable>
    {
        @Override
        public int getPartition(CombinedKey key, IntWritable value, int numPartitions)
        {
            return (key.getKey1().toString() + key.getKey2().toString()).hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step4");

        job.setJarByClass(Step4.class);

        // Mapper
        job.setMapperClass(Step4.MapperClass.class);
        job.setMapOutputKeyClass(CombinedKey.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setPartitionerClass(Step4.PartitionerClass.class);

        // Reducer
        job.setReducerClass(Step4.ReducerClass.class);
        job.setOutputKeyClass(CombinedKey.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(Step4.CombinerClass.class);

        // renaming output file
        job.getConfiguration().set("mapreduce.output.basename", "Step4");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}