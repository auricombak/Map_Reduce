package PaymentMethods;

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
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String oneCsvEntry = value.toString();
			String[] oneCsvEntryTokens = oneCsvEntry.split("\\,");
			
			if (Arrays.equals(oneCsvEntryTokens, emptyWords))
				return;
			
			String paymentType = oneCsvEntryTokens[9];
			
			if (isValidDate(oneCsvEntryTokens[1])){
				String [] splitDateDT = oneCsvEntryTokens[1].split("\\s+");		
				String[] splitTime = splitDateDT[0].split("-");				
				String day = splitTime[2];
				context.write(new Text(day), new IntWritable(Integer.parseInt(paymentType)));
				
			}		
		}	
	}


	class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			

			
			// On copie car l'objet key reste le même entre chaque appel du reducer
			String keyCopy = key.toString();

			HashMap<Integer, Integer> hm = new HashMap();
			
			for (IntWritable val : values) {
				if(hm.containsKey(val.get())) {
					hm.put(val.get(), hm.get(val.get())+1);
				}else {
					hm.put(val.get(), 1);
				}
			}
			
			int max = 0;
			int type = 0;
			for (Entry<Integer,Integer> entry : hm.entrySet()) {
				if(entry.getValue() > max) {
					type = entry.getKey();
					max = entry.getValue();
				}
			}
			String paymentMethod = "";
			switch(type) {
			case 1:
				paymentMethod = "Credit Card";
				break;
			case 2:
				paymentMethod = "Cash";
				break;
			case 3:
				paymentMethod = "No Charge";
				break;
			case 4:
				paymentMethod = "Dispute";
				break;
			case 5:
				paymentMethod = "Unknow";
				break;
			case 6:
				paymentMethod = "Voided Trip";
				break;
			}
			if( !paymentMethod.isEmpty()) {
				context.write(new Text("The " + keyCopy + " febuary / Most used payment Method is : "), new Text(paymentMethod));
			}

		}
	}
	
	public class PaymentMethodDay {
		private static final String INPUT_PATH = "input-Taxi/";
		private static final String OUTPUT_PATH = "output/PaymentMethod-";
		private static final Logger LOG = Logger.getLogger(PaymentMethodDay.class.getName());

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