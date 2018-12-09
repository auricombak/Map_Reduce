package TramBusNb;

import java.io.IOException;
import java.time.Instant;
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


//Pour chaque station, donner le nombre de trams et bus par jour.

public class TramBusNb {

	private static final String INPUT_PATH = "input-TAM/";
	private static final String OUTPUT_PATH = "output/NbTramBus";
	private static final Logger LOG = Logger.getLogger(TramBusNb.class.getName());


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

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

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

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sumTram = 0;
			int sumBus = 0;
			for (IntWritable val : values)
				if(val.get() > 4)
					sumBus++;
				else
					sumTram++;

			context.write(new Text("Stop : " + key + " : " ), new Text("Nb Bus : " + sumBus + " Nb Trams : " + sumTram));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}
