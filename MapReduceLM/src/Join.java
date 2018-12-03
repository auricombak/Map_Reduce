import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//Exercice 5 - Join
//
//Maintenant c'est à vous ! Sur la base des programmes WordCount.java et GroupBy.java, 
// définir une classe Join.java permettant de joindre les lignes concernant les informations des clients et des 
// commandes contenus dans le répertoire input-join.
//
//La jointure doit être réalisée sur l'attribut custkey. Voici le schéma des relations dont les lignes sont extraites :
//
//ORDERS(orderkey,custkey,orderstatuts,totalprice,orderdate,orderpriority,clerk,ship-priority,comment)
//
//CUSTOMERS(custkey,name,address,nationkey,phone,acctbal,mktsegment,comment)
//
//Le programme doit restituer des couples (CUSTOMERS.name,ORDERS.comment)
//
//Pour réaliser la jointure il faut à l'avance recopier dans un tableau temporaire les valeurs de l'itérateur values 
// dans la méthode REDUCE, puis effectuer le parcours avec deux 'for' imbriqués sur ce tableau temporaire

public class Join {
	private static final String INPUT_PATH = "input-join/";
	private static final String OUTPUT_PATH = "output/Join-";
	private static final Logger LOG = Logger.getLogger(Join.class.getName());

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

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public boolean isInteger(String chaine) {
			try {
				Integer.parseInt(chaine);
			}catch (NumberFormatException e) {
				return false;
			}
			return true;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
//			System.out.println(line);
			String[] tokens = line.split("[|]+");

			
			if(tokens.length == 8){
				context.write(new Text(tokens[0]) , new Text(tokens[1]));
			}else{
				context.write(new Text(tokens[1]) , new Text(tokens[8]));
			}


			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//Only one element;
			ArrayList<String> list  = new ArrayList<String>();
			for(Text val : values) {
				list.add(val.toString());
			}

			context.write(key,  new Text(list.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Join");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(Text.class); 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}
