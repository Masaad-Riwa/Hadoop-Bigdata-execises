package epita.fr.Exercise2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver for the Word Count MapReduce job
 */
public class DriverBigData extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        int exitCode;

        // Parse input arguments
        int numberOfReducers = Integer.parseInt(args[0]);
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // Configuration and job setup
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Word Count - Collection of Textual Files");
        job.setJarByClass(DriverBigData.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Set the Mapper and Reducer classes
        job.setMapperClass(MapperBigData.class);
        job.setReducerClass(ReducerBigData.class);

        // Set input and output key-value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the number of reducers
        job.setNumReduceTasks(numberOfReducers);

        // Execute the job
        exitCode = job.waitForCompletion(true) ? 0 : 1;

        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
        System.exit(res);
    }
}