package epita.fr.Exercise5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for calculating the average PM10 value for each sensor
 */
public class MapperBigData extends Mapper<
                    Object, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    DoubleWritable> {  // Output value type

    @Override
    protected void map(
            Object key,   		// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split the CSV line into parts
            String[] parts = value.toString().split(",");
            if (parts.length != 3) {
                return;  // Skip lines with unexpected format
            }

            String sensorId = parts[0];
            double pm10Value = Double.parseDouble(parts[2]);

            // Emit the sensor ID and the PM10 value
            context.write(new Text(sensorId), new DoubleWritable(pm10Value));
    }
}
