package epita.fr.Exercise6;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for computing the maximum and minimum PM10 values for each sensor
 */
public class ReducerBigData extends Reducer<
    Text, // Input key type
    Text, // Input value type
    Text, // Output key type
    Text> { // Output value type

    @Override
    protected void reduce(
            Text key,  // Input key type
            Iterable<Text> values,  // Input value type
            Context context) throws IOException, InterruptedException {

        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;

        // Find the maximum and minimum PM10 values for the sensor
        for (Text value : values) {
            double pm10Value = Double.parseDouble(value.toString());
            if (pm10Value > max) {
                max = pm10Value;
            }
            if (pm10Value < min) {
                min = pm10Value;
            }
        }

        // Emit the sensor ID with max and min PM10 values
        context.write(key, new Text("max=" + max + "_min=" + min));
    }
}
