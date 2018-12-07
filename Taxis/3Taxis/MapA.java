package TreeBusyDays;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class MapA extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static String emptyWords[] = { "" };
	private final static IntWritable one = new IntWritable(1);
    public static boolean isValidDate(String isDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setLenient(false);
        try {
            dateFormat.parse(isDate.trim());
        } catch (ParseException pe) {
            return false;
        }
        return true;
    }
    
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String oneCsvEntry = value.toString();
		String[] oneCsvEntryTokens = oneCsvEntry.split("\\,");

		if (Arrays.equals(oneCsvEntryTokens, emptyWords))
			return;
		
		if (isValidDate(oneCsvEntryTokens[1])){
			String [] splitDateDT = oneCsvEntryTokens[1].split("\\s+");
			context.write(new Text(splitDateDT[0]), one);
		}		
	}
}
