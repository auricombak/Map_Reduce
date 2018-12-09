package AverageTips;

import java.io.IOException;
import java.time.Instant;
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
				Double distance = Double.parseDouble(oneCsvEntryTokens[4]);

				context.write(new DoubleWritable(distance), one);

			}
		}
	}

	class Reduce extends Reducer<DoubleWritable, IntWritable, Text, Text> {

		private double allRatios = 0.0;
		private int totalBill = 0;

		@Override
		public void setup(Context context) {}

		@Override
		public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {




			// On copie car l'objet key reste le même entre chaque appel du reducer
			Double keyCopy = key.get();


			int sum = 0;
			for (IntWritable val : values)
				sum ++;


			allRatios += keyCopy*sum;
			totalBill += sum;

		}


		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
					System.out.println(allRatios + "&&" + totalBill);
					context.write(new Text("Average trip distance in january = ") , new Text(String.format( "%.2f",allRatios/totalBill) + " Miles"));

		}
	}

	public class AverageDistance {
		private static final String INPUT_PATH = "input-Taxi/";
		private static final String OUTPUT_PATH = "output/AvDistance-";
		private static final Logger LOG = Logger.getLogger(AverageDistance.class.getName());

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
