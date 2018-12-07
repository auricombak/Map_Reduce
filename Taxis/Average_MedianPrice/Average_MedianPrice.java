package Average_MedianPrice;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



	class Map extends Mapper<LongWritable, Text, DoubleWritable, IntWritable> {

		private final static String emptyWords[] = { "" };
		private final static IntWritable one = new IntWritable(1);
		
		public static boolean isDouble( String str ){
			  try{
			    Double.parseDouble( str );
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
			
			if(isDouble(oneCsvEntryTokens[4])) {
				Double price = Double.parseDouble(oneCsvEntryTokens[16]);

				context.write(new DoubleWritable(price), one);

			}		
		}
	}

	class Reduce extends Reducer<DoubleWritable, IntWritable, Text, Text> {

		private ArrayList <Double> allPrices = new ArrayList<>();

		@Override
		public void setup(Context context) {}
		
		@Override
		public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			Double keyCopy = key.get();
						
			for (IntWritable val : values)
				allPrices.add(keyCopy);

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
					Double totalPrices = 0.0;
					for(Double price: allPrices ) {
						totalPrices+= price;
					}
					
					Double averagePrices = totalPrices/allPrices.size();
					double medianPrice;
					// Get the middle value of the median, if size of array is peer, we get the n/2 and n/2+ which we divide by 2
					if(allPrices.size() % 2 ==0 ) {
						//peer
						medianPrice = (allPrices.get(allPrices.size()/2) + allPrices.get(allPrices.size()/2+1))/2;
						
					}else {
						//odd
						medianPrice = allPrices.get(allPrices.size()/2+1);
					}
					context.write(new Text(" Median Price : " + String.format( "%.2f",medianPrice) + " Dollars") , new Text(" Average Price : " + String.format( "%.2f",averagePrices) + " Dollars"));
			
		}
	}
	
	public class Average_MedianPrice {
		private static final String INPUT_PATH = "input-Taxi/";
		private static final String OUTPUT_PATH = "output/Average_MedianPrice-";
		private static final Logger LOG = Logger.getLogger(Average_MedianPrice.class.getName());

		static {
			System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

			try {
				FileHandler fh = new FileHandler("out.log");
				fh.setFormatter(new SimpleFormatter());
				LOG.addHandler(fh);
			} catch (SecurityException | IOException e) {
				System.exit(1);
			}
		}
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();


		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);

	}
}