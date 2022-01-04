import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.Math;

public class Step3
{
    public static class MapperClass extends Mapper<LongWritable, Text, CombinedKey, IntWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
        {
            String[] fields = value.toString().split("\t");
            String[] three_gram = fields[0].split(" ");
            IntWritable count = new IntWritable(Integer.parseInt(fields[1]));
            context.write(new CombinedKey(three_gram[0], three_gram[1], Gram.oneGram("*")), count);
            context.write(new CombinedKey(three_gram[0], three_gram[1], Gram.threeGram(three_gram[0], three_gram[1], three_gram[2])), count);
        }
    }

    public static class ReducerClass extends Reducer<CombinedKey,IntWritable, Gram, DoubleWritable>
    {
        int C2 = 0;

        @Override
        public void reduce(CombinedKey key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException
        {
            System.out.println(key.toString());
            int sum = 0;
            for(IntWritable value : values)
                sum += value.get();

            // if key.gram is one-gram then the key is: "<w1,w2>, * " so the sum is C2
            if (key.getGram().getTag().toString().equals(Gram.oneGramString))
            {
                C2 = sum;
            }
            // the key is: "<w1,w2>, <w1,w2,w3>" and the sum is  N3
            else{
                double N3 = sum;
                double k3 = (Math.log(N3 + 1) + 1 ) / (Math.log(N3+1) + 2);
                context.write(key.getGram(), new DoubleWritable(k3*(N3/C2)));
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
        Job job = Job.getInstance(conf, "Step3");

        job.setJarByClass(Step3.class);

        // Mapper
        job.setMapperClass(Step3.MapperClass.class);
        job.setMapOutputKeyClass(CombinedKey.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setPartitionerClass(Step3.PartitionerClass.class);

        // Reducer
        job.setReducerClass(Step3.ReducerClass.class);
        job.setOutputKeyClass(Gram.class);
        job.setOutputValueClass(DoubleWritable.class);

        // renaming output file
        job.getConfiguration().set("mapreduce.output.basename", "Step3");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
