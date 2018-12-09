package Amazon_Huge_Comments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

class ReduceA extends Reducer<Text, IntWritable, Text, Text> {



	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Text keyCopy = new Text(key);
		
		int sum = 0;
		int count = 0;
		double averageRate = 0.0;
		for (IntWritable val : values) {
			sum+= val.get();
			count ++;
		}
		averageRate = sum / count;
		if(sum>10) {
			context.write(new Text(keyCopy), new Text(""+averageRate));
		}


			
	}


}
