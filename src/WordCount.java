import java.io.IOException;


import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{
    public static ArrayList<String> MyStringTokenizer(String line)
    {
        StringTokenizer itr = new StringTokenizer(line.toString());
        ArrayList<String> split = new ArrayList<>();
        while(itr.hasMoreTokens())
        {
            split.add(itr.nextToken());
        }
        return split;
    }

    public static class MapperClass extends Mapper<LongWritable, Text, PairAsKey, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text w1 = new Text();
        private Text w2 = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
        {
            ArrayList<String> line = MyStringTokenizer(value.toString());
            for(int i=0; i<line.size()-1; i++)
            {
                w1 = new Text(line.get(i));
                w2 = new Text(line.get(i+1));
                context.write(new PairAsKey(w1, w2),one);
                context.write(new PairAsKey(w1, new Text("*")), one);
            }
        }
    }

    public static class ReducerClass extends Reducer<PairAsKey,IntWritable, PairAsKey,FloatWritable>
    {
        private static float den=0;

        @Override
        public void reduce(PairAsKey key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException
        {
            System.out.println(key.toString());
            int sum = 0;
            float res = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            Text w2 = key.getW2();
            if (w2.toString().equals("*")) {
                den = sum;
            }
            else {
                res = (float) (sum / den);
                System.out.println("res:" + res + " den:" + den + " " + key);
                context.write(key, new FloatWritable(res));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<PairAsKey, IntWritable>
    {
        @Override
        public int getPartition(PairAsKey key, IntWritable value, int numPartitions)
        {
            Text w1 = (Text) key.getW1();
            return w1.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);

        // Mapper
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(PairAsKey.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setPartitionerClass(PartitionerClass.class);

        // Reducer
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(PairAsKey.class);
        job.setOutputValueClass(FloatWritable.class);

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