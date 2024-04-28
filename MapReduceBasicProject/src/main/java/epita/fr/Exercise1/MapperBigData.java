package epita.fr.Exercise1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Word Count - Mapper
 */
class MapperBigData extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().toLowerCase().split("\\W+"); // Split by non-word characters

        for (String word : words) {
            if (!word.isEmpty()) { // Check if the word is not empty
                context.write(new Text(word), ONE);
            }
        }
    }
}
