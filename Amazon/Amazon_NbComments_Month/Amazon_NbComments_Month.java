package Amazon_NbComments_Month;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
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
import org.json.JSONException;
import org.json.JSONObject;



	class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				JSONObject obj = new JSONObject(value.toString());
				String date = obj.getString("reviewTime");
				String [] splitDateDT = date.split(" ");
				String[] splitDate = splitDateDT[0].split(",");
				String month = splitDate[0];
				//System.out.println(month);
				context.write(new Text(month), one);
				

			}catch(JSONException e) {
				// Do nothing
			}	
	}
	}


	class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		private TreeMap<Integer , List<Text>> reviewerPerNbCmt = new TreeMap<>();
		private int nbReviewers = 0;
		private int k;
		
		@Override
		public void setup(Context context) {
			// On charge k
			k = context.getConfiguration().getInt("k", 1);
		}
		
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			Integer sum = 0;
			Text KeyCopy = new Text(key);

			for (IntWritable val : values)
				sum ++;

			String month = "";
		        switch (Integer.parseInt(KeyCopy.toString())) {
	            case 1:  month = "January";
	                     break;
	            case 2:  month = "February";
	                     break;
	            case 3:  month = "March";
	                     break;
	            case 4:  month = "April";
	                     break;
	            case 5:  month = "May";
	                     break;
	            case 6:  month = "June";
	                     break;
	            case 7:  month = "July";
	                     break;
	            case 8:  month = "August";
	                     break;
	            case 9:  month = "September";
	                     break;
	            case 10: month = "October";
	                     break;
	            case 11: month = "November";
	                     break;
	            case 12: month = "December";
	                     break;
	            default: month = "Invalid month";
	                     break;
		        }
		        context.write(new Text("In " + month + " : "), new Text(sum + " comments written"));
				
		}
		

	}
	
	
	public class Amazon_NbComments_Month {
		private static final String INPUT_PATH = "input-amazon/";
		private static final String OUTPUT_PATH = "output/Month/ComOnth-";
		private static final Logger LOG = Logger.getLogger(Amazon_NbComments_Month.class.getName());

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

		// Borne 'k' du topk
		int k = 10;

		try {
			// Passage du k en argument ?
			if (args.length > 0) {
				k = Integer.parseInt(args[0]);

				// On contraint k à valoir au moins 1
				if (k <= 0) {
					LOG.warning("k must be at least 1, " + k + " given");
					k = 1;
				}
			}
		} catch (NumberFormatException e) {
			LOG.severe("Error for the k argument: " + e.getMessage());
			System.exit(1);
		}

		Configuration conf = new Configuration();
		conf.setInt("k", k);



		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
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


