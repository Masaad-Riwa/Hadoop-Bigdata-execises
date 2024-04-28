package epita.fr.Exercise7;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for creating an inverted index
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

        HashSet<String> sentenceIds = new HashSet<>();

        // Collect unique sentence IDs for each word
        for (Text value : values) {
            sentenceIds.add(value.toString());
        }

        // Create a formatted output with the word and the list of unique sentence IDs
        context.write(key, new Text(sentenceIds.toString()));
    }
}
