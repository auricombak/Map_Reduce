package Amazon_Best_Reviewer;

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

class MapA extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private final static IntWritable one = new IntWritable(1);
	
    
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			JSONObject obj = new JSONObject(value.toString());
			String name = obj.getString("reviewerName");
			int helpful_rate = obj.getJSONArray("helpful").getInt(0);
			int helpful_rate_on = obj.getJSONArray("helpful").getInt(1);
			if(helpful_rate_on == 0 || helpful_rate_on == 0) {
				context.write(new Text(name), new DoubleWritable(0.0));
			}else {
				context.write(new Text(name), new DoubleWritable(helpful_rate/helpful_rate_on));
			}

		}catch(JSONException e) {
			// Do nothing
		}
		//
		
	}
}
