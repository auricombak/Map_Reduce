package ReverseTopK;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class MapA extends Mapper<LongWritable, Text, Text, Text> {

	public static boolean isDouble( String str ){
		  try{
		    Double.parseDouble( str );
		    return true;}
		  catch( Exception e ){
		    return false;
		  }
		}

	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String oneCsvEntry = value.toString();
		String[] oneCsvEntryTokens = oneCsvEntry.split("\\,");

		if (Arrays.equals(oneCsvEntryTokens, emptyWords))
			return;

		if (isDouble(oneCsvEntryTokens[20])){
			context.write(new Text(oneCsvEntryTokens[20]), new Text(oneCsvEntry));
		}		
	}
}