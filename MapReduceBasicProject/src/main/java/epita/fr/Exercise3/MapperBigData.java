package epita.fr.Exercise3;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.log4j.Logger;

public class MapperBigData extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final double PM10_THRESHOLD = 50.0; // Threshold for PM10 value

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into sensorId, date, and PM10 value
        String[] parts = value.toString().split("\\t");
        String sensorId = parts[0].split(",")[0];
        double pm10Value = Double.parseDouble(parts[1]);
        
        //System.out.println("Processing line: " + value.toString());
        //System.out.println("PM10 value: " + pm10Value);
        //System.out.println("Threshold: " + PM10_THRESHOLD);
        
        // Emit the sensorId if PM10 value exceeds the threshold
        try {
            if (pm10Value > PM10_THRESHOLD) {
                context.write(new Text(sensorId), new Text("exceeded"));
            }
        } catch (Exception e) {
            System.err.println("Error during context.write: " + e.getMessage());
            e.printStackTrace(); // This will log the stack trace
        }
    }
}
