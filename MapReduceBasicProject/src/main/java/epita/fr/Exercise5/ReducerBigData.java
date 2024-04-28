package epita.fr.Exercise5;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for calculating the average PM10 value for each sensor
 */
public class ReducerBigData extends Reducer<
    Text, // Input key type
    DoubleWritable, // Input value type
    Text, // Output key type
    DoubleWritable> { // Output value type

    @Override
    protected void reduce(
            Text key,  // Input key type
            Iterable<DoubleWritable> values,  // Input value type
            Context context) throws IOException, InterruptedException {

        int count = 0;
        double total = 0.0;

        // Iterate over all PM10 values for the sensor to compute the average
        for (DoubleWritable value : values) {
            total += value.get();
            count++;
        }

        // Calculate the average PM10 value for the sensor
        double average = total / count;

        // Emit the sensor ID and the average PM10 value
        context.write(key, new DoubleWritable(average));
    }
}
