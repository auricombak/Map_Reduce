package Passenger2;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.TreeMap;
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



	class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private final static String emptyWords[] = { "" };
		private final static IntWritable one = new IntWritable(1);
		
		public static boolean isInteger( String str ){
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
			
			if(isInteger(oneCsvEntryTokens[3])) {
				int nbPassengers = Integer.parseInt(oneCsvEntryTokens[3]);

				context.write(new IntWritable(nbPassengers), one);

			}		
		}
	}

	class Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {

		TreeMap<Integer, Integer> passengersRates = new TreeMap<>();

		@Override
		public void setup(Context context) {}
		
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			

			
			// On copie car l'objet key reste le même entre chaque appel du reducer
			Integer keyCopy = key.get();

			
			int sum = 0;
			for (IntWritable val : values)
				sum ++;
			passengersRates.put(sum , keyCopy);


		}

		/**
		 * Méthode appelée à la fin de l'étape de reduce.
		 * 
		 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
					if(passengersRates.lastEntry().getKey() == 2) {
						context.write(new Text("2 Passenger is the most frequent") , new Text());
					}else {
						context.write(new Text("2 Passenger is not the most frequent, it is : " + passengersRates.lastEntry().getValue() + " with " + passengersRates.lastEntry().getKey() + " times" ) , new Text());
					}
		}
	}
	
	public class Passenger2 {
		private static final String INPUT_PATH = "input-Taxi/";
		private static final String OUTPUT_PATH = "output/PassengerM-";
		private static final Logger LOG = Logger.getLogger(Passenger2.class.getName());

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

		job.setOutputKeyClass(IntWritable.class);
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