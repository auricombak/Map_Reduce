package Amazon_Huge_Comments;

import java.io.IOException;
import java.io.PrintStream;

import org.json.*;


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

	private final static IntWritable one = new IntWritable(1);
    
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			JSONObject obj = new JSONObject(value.toString());
			String name = obj.getString("reviewerName");
			String comment = obj.getString("reviewText");
			int commentSize = comment.split("\\s+").length;
			context.write(new Text(name), new IntWritable(commentSize));

		}catch(JSONException e) {
			// Do nothing
		}
		//
		
	}
}
