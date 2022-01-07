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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Step5
{
    public static class MapperClass extends Mapper<LongWritable, Text, Gram, DoubleWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
        {
            String[] fields = value.toString().split("\t");
            String[] three_gram = fields[0].split(" ");
            DoubleWritable res = new DoubleWritable(Double.parseDouble(fields[1]));
            context.write(Gram.threeGram(three_gram[0],three_gram[1],three_gram[2]), res);
        }
    }

    public static class CombinerClass extends Reducer<Gram, DoubleWritable, Gram, DoubleWritable>
    {
        @Override
        public void reduce(Gram key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double sum=0;
            for(DoubleWritable value : values)
                sum += value.get();
            context.write(key,new DoubleWritable(sum));
        }
    }

    public static class ReducerClass extends Reducer<Gram,DoubleWritable, Gram, DoubleWritable>
    {

        @Override
        public void reduce(Gram key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException
        {
            double sum = 0;
            for(DoubleWritable value : values)
                sum += value.get();

            context.write(key,new DoubleWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Gram, DoubleWritable>
    {
        @Override
        public int getPartition(Gram key, DoubleWritable value, int numPartitions)
        {
            return key.toString().hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step5");

        job.setJarByClass(Step5.class);

        // Mapper
        job.setMapperClass(Step5.MapperClass.class);
        job.setMapOutputKeyClass(Gram.class);
        job.setMapOutputValueClass(DoubleWritable.class);


        job.setPartitionerClass(Step5.PartitionerClass.class);

        // Reducer
        job.setReducerClass(Step5.ReducerClass.class);
        job.setOutputKeyClass(Gram.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(Step5.CombinerClass.class);

        // renaming output file
        job.getConfiguration().set("mapreduce.output.basename", "Step5");

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
