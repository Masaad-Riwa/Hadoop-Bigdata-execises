package epita.fr.Exercise3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Word Count - Mapper
 */
class MapperBigData extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line by the tab delimiter
        String[] parts = value.toString().split("\t");

        if (parts.length == 2) {
            String sensorId = parts[0];
            double pm10Value = Double.parseDouble(parts[1]);

            // Get the threshold from the job configuration
            int threshold = context.getConfiguration().getInt("threshold", 0);

            if (pm10Value > threshold) {
                context.write(new Text(sensorId), ONE);
            }
        }
    }
}