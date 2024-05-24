package epita.fr.Exercise7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce program to create an inverted index
 */
public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    int exitCode;

    int numberOfReducers = Integer.parseInt(args[0]);
    Path inputPath = new Path(args[1]);
    Path outputDir = new Path(args[2]);
    
    Configuration conf = this.getConf();

    Job job = Job.getInstance(conf);

    job.setJobName("Exercise #7 - Inverted Index");

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputDir);

    job.setJarByClass(DriverBigData.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
       
    job.setMapperClass(MapperBigData.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(ReducerBigData.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(numberOfReducers);

    exitCode = job.waitForCompletion(true) ? 0 : 1;

    return exitCode;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new DriverBigData(), args);
    System.exit(exitCode);
  }
}
