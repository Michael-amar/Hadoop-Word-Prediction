import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class ThreeGramsStatistics
{

    public static class MapperClass extends Mapper<LongWritable, Text, Gram, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text w1 = new Text();
        private Text w2 = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
        {

            String[] fields = value.toString().split("\t");
            String[] three_gram = fields[0].split(" ");
            IntWritable count = new IntWritable(Integer.parseInt(fields[2]));
            Gram w1 = Gram.oneGram(new Text(three_gram[0]));
            Gram w2 = Gram.oneGram(new Text(three_gram[1]));
            Gram w3 = Gram.oneGram(new Text(three_gram[2]));
            Gram w1w2 = Gram.twoGram(new Text(three_gram[0]), new Text(three_gram[1]));
            Gram w2w3 = Gram.twoGram(new Text(three_gram[1]), new Text(three_gram[2]));
            Gram w1w2w3 = Gram.threeGram(new Text(three_gram[0]), new Text(three_gram[1]), new Text(three_gram[2]));
            context.write(w1,count);
            context.write(w2,count);
            context.write(w3,count);
            context.write(w1w2,count);
            context.write(w2w3,count);
            context.write(w1w2w3,count);
        }
    }

    public static class ReducerClass extends Reducer<Gram,IntWritable, Gram,IntWritable>
    {
        private static float den=0;

        @Override
        public void reduce(Gram key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException
        {
            int sum = 0 ;
            for(IntWritable value : values)
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Gram, IntWritable>
    {
        @Override
        public int getPartition(Gram key, IntWritable value, int numPartitions)
        {
            return 1;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "three grams");

        job.setJarByClass(WordCount.class);

        // Mapper
        job.setMapperClass(ThreeGramsStatistics.MapperClass.class);
        job.setMapOutputKeyClass(Gram.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setPartitionerClass(ThreeGramsStatistics.PartitionerClass.class);

        // Reducer
        job.setReducerClass(ThreeGramsStatistics.ReducerClass.class);
        job.setOutputKeyClass(Gram.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String IterableToString(Iterable<IntWritable> itr)
    {
        String a = "[ ";
        for(IntWritable d : itr)
        {
            a += d + " ";
        }
        a += "]";
        return a;
    }
}
