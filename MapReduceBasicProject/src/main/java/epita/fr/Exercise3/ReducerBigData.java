package epita.fr.Exercise3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ReducerBigData extends Reducer<Text, Text, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // If there's at least one record for this sensor, write it to the output
        Set<String> uniqueValues = new HashSet<>();
        
        for (Text value : values) {
            uniqueValues.add(value.toString());
            System.out.println("reducer:" + value.toString());
        }
        
        if (uniqueValues.contains("exceeded")) {
            context.write(key, new LongWritable(1));
        }
    }
}
