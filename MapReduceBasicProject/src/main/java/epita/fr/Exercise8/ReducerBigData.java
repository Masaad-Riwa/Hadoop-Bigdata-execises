package epita.fr.Exercise8;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 8 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		MonthIncome, // Input value type
		Text, // Output key type
		DoubleWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<MonthIncome> values, // Input value type
			Context context) throws IOException, InterruptedException {

		// Store in the hashmap 
		HashMap<String, Double> totalMonthIncome = new HashMap<String, Double>();

		String year = key.toString();
 
		double totalYearlyIncome = 0;
		int countMonths = 0;

		for (MonthIncome value : values) {
			Double income = totalMonthIncome.get(value.getMonthID());

			if (income != null) {
				totalMonthIncome.put(new String(value.getMonthID()), new Double(value.getIncome() + income));
			} else {
				totalMonthIncome.put(new String(value.getMonthID()), new Double(value.getIncome()));

				countMonths++;
			}
			totalYearlyIncome = totalYearlyIncome + value.getIncome();
		}

		for (Entry<String, Double> pair : totalMonthIncome.entrySet()) {
			context.write(new Text(year + "-" + pair.getKey()), new DoubleWritable(pair.getValue()));
		}
		context.write(new Text(year), new DoubleWritable(totalYearlyIncome / countMonths));

	}
}