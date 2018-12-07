package MostKBusTramStation;


import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



	class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

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
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] splited = line.split("\\;");
			if(isInteger(splited[4])) {
				String stopName = splited[3];
				Integer routeShortName = Integer.parseInt(splited[4]);
					context.write(new Text(stopName), new IntWritable(routeShortName));
			}
		}	
	}


	class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		private TreeMap<Integer , List<Text>> stationsFreq = new TreeMap<>();
		private int nbsortedStations = 0;
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
				if (stationsFreq.containsKey(sum)) {
					stationsFreq.get(sum).add(KeyCopy);
					nbsortedStations++;
				}
				else {
					List<Text> stops = new ArrayList<>();
					stops.add(KeyCopy);
					stationsFreq.put(sum, stops);
					nbsortedStations+=stops.size();
				}


				// Nombre de stations enregistrés atteintes : on supprime la station la moins fréquentée (le premier dans hourFreq)
				while (nbsortedStations > k) {
					
					Integer firstKey = stationsFreq.firstKey();
					List<Text> stops = stationsFreq.get(firstKey);
					
					stops.remove(stops.size() - 1);
					nbsortedStations -- ;
					
					if (stops.isEmpty()) {
						stationsFreq.remove(firstKey);
					}
				} 		
				
				
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			Integer[] nbofs = stationsFreq.keySet().toArray(new Integer[0]);
			
			// Parcours en sens inverse pour obtenir un ordre descendant
			int i = nbofs.length;

			while (i-- != 0) {
				for (Text station : stationsFreq.get(nbofs[i])) {
					context.write(new Text("The station : " + station + " is served " ), new Text( nbofs[i] + " Times" ));
				}
			}
		} 

	}
	
	
	public class MostKBusTramStop {
		private static final String INPUT_PATH = "input-TAM/";
		private static final String OUTPUT_PATH = "output/Station-";
		private static final Logger LOG = Logger.getLogger(MostKBusTramStop.class.getName());

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


