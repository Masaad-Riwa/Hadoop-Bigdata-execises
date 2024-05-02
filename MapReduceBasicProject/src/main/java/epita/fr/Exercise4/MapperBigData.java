package epita.fr.Exercise4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

/**
 * Exercise 4 - Mapper
 */
class MapperBigData extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try{
		System.out.println(value.toString());
		String[] parts = value.toString().split("\\t");
		System.out.println("parts" + parts);
        String zoneId = parts[0].split(",")[0];
		String date = parts[0].split(",")[1];
        double pm10Value = Double.parseDouble(parts[1]);

		System.out.println(pm10Value + " " + date);
        // Emit key-value pair with zoneId as key and date as value
		if (pm10Value > 50.0) { // Check if PM10 value exceeds threshold
            context.write(new Text(zoneId), new Text(date));
			System.out.println("aaaaa");
		}
	 } catch (Exception e) {
		System.err.println("Error during context.write: " + e.getMessage());
		e.printStackTrace(); 
		}
    }
}
