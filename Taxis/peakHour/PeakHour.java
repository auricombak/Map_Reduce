package peakHour;

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



	class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static String emptyWords[] = { "" };
		private final static IntWritable one = new IntWritable(1);
		
		 public static boolean isValidDate(String date) {
		        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		        dateFormat.setLenient(false);
		        try {
		            dateFormat.parse(date.trim());
		        } catch (ParseException pe) {
		            return false;
		        }
		        return true;
		    }

			@Override
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
				String line = value.toString();
				String[] splited = line.split("\\,");
				
				if (Arrays.equals(splited, emptyWords))
					return;
				if (isValidDate(splited[1])){
					
					String [] splitDateDT = splited[1].split("\\s+");
					
					String[] timeSplited = splitDateDT[1].split(":");
					String hour = timeSplited[0];
					
					context.write(new Text(hour), one);
				}		
			}	
	}


	class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		private TreeMap<Integer , List<Text>> hourFreq = new TreeMap<>();
		private int nbsortedHours = 0;
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


				// Fréquence déjà présente
				if (hourFreq.containsKey(sum))
					hourFreq.get(sum).add(KeyCopy);
				else {
					List<Text> hours = new ArrayList<>();
					hours.add(KeyCopy);
					hourFreq.put(sum, hours);
					nbsortedHours+=hours.size();
				}

				
				// Nombre d'heures enregistrés atteintes : on supprime l'heure la moins fréquente (le premier dans hourFreq)
				while (nbsortedHours > k) {
					Integer firstKey = hourFreq.firstKey();
					List<Text> hours = hourFreq.get(firstKey);
					hours.remove(hours.size() - 1);
					nbsortedHours -- ;

					if (hours.isEmpty())
						hourFreq.remove(firstKey);
				} 			
				
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			Integer[] nbofs = hourFreq.keySet().toArray(new Integer[0]);

			// Parcours en sens inverse pour obtenir un ordre descendant
			int i = nbofs.length;

			while (i-- != 0) {
				for (Text hour : hourFreq.get(nbofs[i])) {
					context.write(new Text("From " + hour + " h during one hour " ), new Text( nbofs[i] + " Taxis in activity" ));
				}
			}
		} 

	}
	
	
	public class PeakHour {
		private static final String INPUT_PATH = "input-Taxi/";
		private static final String OUTPUT_PATH = "output/PeakHour-";
		private static final Logger LOG = Logger.getLogger(PeakHour.class.getName());

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


