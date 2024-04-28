package epita.fr.Exercise2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for the Word Count problem
 */
public class ReducerBigData extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Sum the counts for each word
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Write the word and its total count
        context.write(key, new IntWritable(sum));
    }
}