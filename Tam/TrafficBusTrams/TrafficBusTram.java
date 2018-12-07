package TrafficBusTrams;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
		
		 public static boolean isValidHour(String date) {
		        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		        dateFormat.setLenient(false);
		        try {
		            dateFormat.parse(date.trim());
		        } catch (ParseException pe) {
		            return false;
		        }
		        return true;
		    }
		 
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
			if(isValidHour(splited[7])){
				String[] siplitedDate = splited[7].split(":");
				String departureTimeHour = siplitedDate[0];
				if(isInteger(splited[4])) {
					String stopName = splited[3];
					Integer routeShortName = Integer.parseInt(splited[4]);
						context.write(new Text(stopName + ":" + departureTimeHour+"H"), new IntWritable(routeShortName));
				}
			}
		}	
	}


	class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			Text copyKey = new Text(key);
			
			int sumTram = 0;
			int sumBus = 0;
			
			for (IntWritable val : values)
				if(val.get() == 1) {
					sumTram++;
				}else {
					sumBus++;
				}		
			
			String T_Traffic = "Tram traffic : ";
			Boolean isTramT = true;
			if(sumTram==0) {
				isTramT = false;
			}else if(sumTram<5) {
				T_Traffic+="Faible";
			}else if(sumTram<10) {
				T_Traffic+="Moyen";
			}else if(sumTram>9){
				T_Traffic+="Fort";
			}
			
			String B_Traffic = "Bus traffic : ";
			Boolean isBusT = true;
			if(sumBus == 0) {
				isBusT = false;
			}else if(sumBus<5) {
				B_Traffic+="Faible";
			}else if(sumBus<10) {
				B_Traffic+="Moyen";
			}else if(sumBus>9){
				B_Traffic+="Fort";
			}
			
			String stringTraffic;
			if(isTramT && isBusT) {
				stringTraffic= T_Traffic + "||" + B_Traffic; 
			}else if(isTramT){
				stringTraffic= T_Traffic;
			}else {
				stringTraffic= B_Traffic;
			}
			
			context.write(new Text("Pour la station : " + key.toString()),new Text(stringTraffic) );
				
		}
		

	}
	
	
	public class TrafficBusTram {
		private static final String INPUT_PATH = "input-TAM/";
		private static final String OUTPUT_PATH = "output/Traffic-";
		private static final Logger LOG = Logger.getLogger(TrafficBusTram.class.getName());

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


