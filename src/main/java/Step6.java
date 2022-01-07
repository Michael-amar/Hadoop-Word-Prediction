import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Step6
{
    public static class MapperClass extends Mapper<LongWritable, Text, Custom3Gram, DoubleWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
        {
            String[] fields = value.toString().split("\t");
            String[] three_gram = fields[0].split(" ");
            DoubleWritable res = new DoubleWritable(Double.parseDouble(fields[1]));
            Gram tri_gram = Gram.threeGram(three_gram[0],three_gram[1],three_gram[2]);
            context.write(new Custom3Gram(tri_gram,res), res);
        }
    }

    public static class ReducerClass extends Reducer<Custom3Gram,DoubleWritable, Gram, DoubleWritable>
    {

        @Override
        public void reduce(Custom3Gram key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException
        {
            context.write(key.getThree_gram(), key.getValue());
        }
    }

    public static class PartitionerClass extends Partitioner<Gram, DoubleWritable>
    {
        @Override
        public int getPartition(Gram key, DoubleWritable value, int numPartitions)
        {
            return 1;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step6");

        job.setJarByClass(Step6.class);

        // Mapper
        job.setMapperClass(Step6.MapperClass.class);
        job.setMapOutputKeyClass(Custom3Gram.class);
        job.setMapOutputValueClass(DoubleWritable.class);


        job.setPartitionerClass(Step6.PartitionerClass.class);

        // Reducer
        job.setReducerClass(Step6.ReducerClass.class);
        job.setOutputKeyClass(Gram.class);
        job.setOutputValueClass(DoubleWritable.class);


        // renaming output file
        job.getConfiguration().set("mapreduce.output.basename", "Step6");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
