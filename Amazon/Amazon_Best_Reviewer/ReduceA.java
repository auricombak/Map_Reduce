package Amazon_Best_Reviewer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

class ReduceA extends Reducer<Text, DoubleWritable, Text, Text> {



	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Text keyCopy = new Text(key);
		
		double sum = 0.0;
		int count = 0;
		double averageRate = 0.0;
		for (DoubleWritable val : values) {
			sum+= val.get();
			count ++;
		}
		averageRate = sum / count;
		if(averageRate > 0.0) {
			context.write(new Text(keyCopy), new Text(""+averageRate));
		}

			
	}


}
