package DepartureFreq;

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
	
	public boolean isPosition( String str ){
		  try{
		    Integer.parseInt( str );
		    return true;}
		  catch( Exception e ){
		    return false;
		  }
		}
    
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String oneCsvEntry = value.toString();
		String[] oneCsvEntryTokens = oneCsvEntry.split("\\,");

		if (Arrays.equals(oneCsvEntryTokens, emptyWords))
			return;
		
		if (isPosition(oneCsvEntryTokens[7])){
			context.write(new Text(oneCsvEntryTokens[7]), one);
		}		
	}
}
