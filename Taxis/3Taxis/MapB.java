package TreeBusyDays;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class MapB extends Mapper<LongWritable, Text, IntWritable, Text> {

	public boolean isInteger( String str ){
		  try{
		    Integer.parseInt( str );
		    return true;}
		  catch( Exception e ){
		    return false;
		  }
		}

	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String Entry = value.toString();
		String[] Tokens = Entry.split("\\t");
//		System.out.println(Tokens[0]);
//		System.out.println(Tokens[1]);
		if (Arrays.equals(Tokens, emptyWords))
			return;

		if (isInteger(Tokens[1])){
			context.write(new IntWritable(Integer.parseInt(Tokens[1])), new Text (Tokens[0]));
		}		
	}
}
