package epita.fr.Exercise6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for computing the maximum and minimum PM10 values for each sensor
 */
public class MapperBigData extends Mapper<
                    Object, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {        // Output value type
    
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
            String pm10Value = parts[2];

            // Emit the sensor ID and the PM10 value
            context.write(new Text(sensorId), new Text(pm10Value));
    }
}
