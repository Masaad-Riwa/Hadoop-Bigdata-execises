package epita.fr.Exercise8;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for calculating total monthly income
 */
public class ReducerBigData extends Reducer<
    Text, // Input key type
    IntWritable, // Input value type
    Text, // Output key type
    IntWritable> { // Output value type

    @Override
    protected void reduce(
            Text key,  // Input key type
            Iterable<IntWritable> values,  // Input value type
            Context context) throws IOException, InterruptedException {

        int totalIncome = 0;

        // Sum all the daily incomes for the month
        for (IntWritable value : values) {
            totalIncome += value.get();
        }

        // Emit the month and the total income
        context.write(key, new IntWritable(totalIncome));
    }
}
