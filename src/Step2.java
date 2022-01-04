import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Step2
{
    public static class MapperClass extends Mapper<LongWritable, Text, Gram, IntWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
        {
            String[] fields = value.toString().split("\t");
            String[] three_gram = fields[0].split(" ");
            IntWritable count = new IntWritable(Integer.parseInt(fields[1]));
            context.write(Gram.oneGram("*"), new IntWritable(count.get()*3));
            context.write(Gram.oneGram(three_gram[0]), count);
            context.write(Gram.oneGram(three_gram[1]), count);
            context.write(Gram.oneGram(three_gram[2]), count);
        }
    }

    public static class ReducerClass extends Reducer<Gram,IntWritable, Gram,IntWritable>
    {
        @Override
        public void reduce(Gram key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException
        {
            int sum = 0;
            for(IntWritable value : values){
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Gram, IntWritable>
    {
        @Override
        public int getPartition(Gram key, IntWritable value, int numPartitions)
        {
            return key.getW1().toString().hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "one grams count");

        job.setJarByClass(Step2.class);

        // Mapper
        job.setMapperClass(Step2.MapperClass.class);
        job.setMapOutputKeyClass(Gram.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setPartitionerClass(Step2.PartitionerClass.class);

        // Reducer
        job.setReducerClass(Step2.ReducerClass.class);
        job.setOutputKeyClass(Gram.class);
        job.setOutputValueClass(IntWritable.class);

        job.getConfiguration().set("mapreduce.output.basename", "1-grams_count");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
