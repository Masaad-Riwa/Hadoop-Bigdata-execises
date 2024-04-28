package epita.fr.Exercise7;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for creating an inverted index
 */
public class MapperBigData extends Mapper<
                    Object, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {        // Output value type

    private static final Pattern SPLIT_REGEX = Pattern.compile("\\W+");
    private static final String[] STOPWORDS = {"and", "or", "not"};
    
    @Override
    protected void map(
            Object key,   		// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split the input line into sentence ID and text
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) {
                return;  // Skip lines with unexpected format
            }

            String sentenceId = parts[0];
            String sentence = parts[1].toLowerCase();  // Lowercase to avoid case sensitivity

            // Split the sentence into words and remove empty strings
            String[] words = SPLIT_REGEX.split(sentence);

            for (String word : words) {
                if (word.isEmpty()) {
                    continue;
                }
                boolean isStopword = false;
                for (String stopword : STOPWORDS) {
                    if (word.equals(stopword)) {
                        isStopword = true;
                        break;
                    }
                }
                if (!isStopword) {
                    // Emit the word and the sentence ID
                    context.write(new Text(word), new Text(sentenceId));
                }
            }
    }
}
