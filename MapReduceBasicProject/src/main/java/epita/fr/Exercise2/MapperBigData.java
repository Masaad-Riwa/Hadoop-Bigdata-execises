package epita.fr.Exercise2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for the Word Count problem
 */
public class MapperBigData extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into words (non-word characters as delimiters)
        String[] words = value.toString().toLowerCase().split("\\W+");

        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(new Text(word), ONE);
            }
        }
    }
}
