package epita.fr.Exercise4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for PM10 pollution analysis per city zone
 */
public class MapperBigData extends Mapper<
                    Object, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {        // Output value type
    
    private static final double PM10Threshold = 50.0;

    @Override
    protected void map(
            Object key,   		// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Extract zone and date from the input line
            String[] fields = value.toString().split(",");
            if (fields.length != 2) {
                return;
            }

            String zoneId = fields[0];
            String[] dateValue = fields[1].split("\t");

            if (dateValue.length != 2) {
                return;
            }

            String date = dateValue[0];
            double PM10Level = Double.parseDouble(dateValue[1]);

            // Check if PM10 level exceeds the threshold
            if (PM10Level > PM10Threshold) {
                // Emit the pair (zoneId, date)
                context.write(new Text(zoneId), new Text(date));
            }
    }
}
