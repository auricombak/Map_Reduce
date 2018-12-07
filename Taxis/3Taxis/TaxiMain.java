package TreeBusyDays;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TaxiMain {
	private static final String INPUT_PATH = "input-Taxi/";
	private static final String OUTPUT_PATH = "output/Taxi3D-";
	private static final Logger LOG = Logger.getLogger(TaxiMain.class.getName());

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

	/**
	 * Ce programme permet le passage d'une valeur k en argument de la ligne de commande.
	 */
	public static void main(String[] args) throws Exception {
		// Borne 'k' du topk
		int k = 3;

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

		Configuration confA = new Configuration();


		Job jobA = new Job(confA, "Conf1");

		
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(IntWritable.class);
		


		jobA.setMapperClass(MapA.class);
		jobA.setReducerClass(ReduceA.class);
		
		jobA.setInputFormatClass(TextInputFormat.class);
		jobA.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(jobA, new Path(INPUT_PATH));
		String outputPath = OUTPUT_PATH + Instant.now().getEpochSecond();
		FileOutputFormat.setOutputPath(jobA, new Path(outputPath));

		jobA.waitForCompletion(true);
			
		// ====================================================
		
		Configuration confB = new Configuration();
		confB.setInt("k", k);
		
		Job jobB = new Job(confB, "Conf2");
		
		/*
		 * Affectation de la classe du comparateur au job.
		 * Celui-ci sera appelé durant la phase de shuffle.
		 */
		// jobB.setSortComparatorClass(DoubleInverseComparator.class);
		
		jobB.setOutputKeyClass(IntWritable.class);
		jobB.setOutputValueClass(Text.class);
		
		jobB.setMapperClass(MapB.class);
		jobB.setReducerClass(ReduceB.class);
		
		FileInputFormat.addInputPath(jobB, new Path(outputPath));
		FileOutputFormat.setOutputPath(jobB, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));
		
		jobB.waitForCompletion(true);
	}
}
