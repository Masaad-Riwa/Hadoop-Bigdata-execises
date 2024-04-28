package epita.fr.Exercise8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for calculating total monthly income
 */
public class MapperBigData extends Mapper<
                    Object, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    IntWritable> {  // Output value type
    
    @Override
    protected void map(
            Object key,   		// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split the input line into date and daily income
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) {
                return;  // Skip lines with unexpected format
            }

            String date = parts[0];
            int dailyIncome = Integer.parseInt(parts[1]);

            // Extract year and month from the date
            String month = date.substring(0, 7);  // Year-Month format

            // Emit the month and the daily income
            context.write(new Text(month), new IntWritable(dailyIncome));
    }
}
