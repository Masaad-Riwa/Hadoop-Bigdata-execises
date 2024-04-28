package epita.fr.Exercise4;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for PM10 pollution analysis per city zone
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

        ArrayList<String> highPM10Dates = new ArrayList<>();

        // Add each date with PM10 above threshold to the list
        for (Text value : values) {
            highPM10Dates.add(value.toString());
        }

        // Emit the zone and the list of dates
        context.write(key, new Text(highPM10Dates.toString()));
    }
}
