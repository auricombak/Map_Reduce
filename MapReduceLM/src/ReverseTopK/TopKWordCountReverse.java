package ReverseTopK;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//=========================================================================
//COMPARATEURS
//=========================================================================

/**
* Comparateur qui inverse la méthode de comparaison d'un sous-type T de WritableComparable (ie. une clé).
*/
@SuppressWarnings("rawtypes")
class InverseComparator<T extends WritableComparable> extends WritableComparator {

	public InverseComparator(Class<T> parameterClass) {
		super(parameterClass, true);
	}

	/**
	 * Cette fonction définit l'ordre de comparaison entre 2 objets de type T.
	 * Dans notre cas nous voulons simplement inverser la valeur de retour de la méthode T.compareTo.
	 * 
	 * @return 0 si a = b <br>
	 *         x > 0 si a > b <br>
	 *         x < 0 sinon
	 */
	@SuppressWarnings("unchecked")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		// On inverse le retour de la méthode de comparaison du type
		return -a.compareTo(b);
	}
}

/**
* Inverseur de la comparaison du type Text.
*/
class TextInverseComparator extends InverseComparator<Text> {

	public TextInverseComparator() {
		super(Text.class);
	}
}

class DoubleInverseComparator extends InverseComparator<DoubleWritable> {

	public DoubleInverseComparator() {
		super(DoubleWritable.class);
	}
}

/*
 * Jusqu'à présent nous avons défini nos mappers et reducers comme des classes internes à notre classe principale.
 * Dans des applications réelles de map-reduce cela ne sera généralement pas le cas, les classes seront probablement localisées dans d'autres fichiers.
 * Dans cet exemple, nous avons défini Map et Reduce en dehors de notre classe principale.
 * Il se pose alors le problème du passage du paramètre 'k' dans notre reducer, car il n'est en effet plus possible de déclarer un paramètre k dans notre classe principale qui serait partagé avec ses classes internes ; c'est la que la Configuration du Job entre en jeu.
 */
//=========================================================================
//MAPPER A
//=========================================================================

class MapA extends Mapper<LongWritable, Text, Text, Text> {

	public static boolean isDouble( String str ){
		  try{
		    Double.parseDouble( str );
		    return true;}
		  catch( Exception e ){
		    return false;
		  }
		}

	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String oneCsvEntry = value.toString();
		String[] oneCsvEntryTokens = oneCsvEntry.split("\\,");

		if (Arrays.equals(oneCsvEntryTokens, emptyWords))
			return;

		if (isDouble(oneCsvEntryTokens[20])){
			context.write(new Text(oneCsvEntryTokens[20]), new Text(oneCsvEntry));
		}		
	}
}

//=========================================================================
//REDUCER A
//=========================================================================

class ReduceA extends Reducer<Text, Text, DoubleWritable, Text> {
	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 * 
	 * Il associe une fréquence à une liste de mots.
	 */
	private TreeMap<Double, List<Text>> sortedLineProfil = new TreeMap<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		// On copie car l'objet key reste le même entre chaque appel du reducer
		Double keyCopy = Double.parseDouble(key.toString());
		
		List tmp = new ArrayList();
		for (Text val : values)
			tmp.add(new Text(val));


		// Fréquence déjà présente
		if (sortedLineProfil.containsKey(keyCopy))
			sortedLineProfil.get(keyCopy).addAll(tmp);
		else {
			List<Text> words = new ArrayList<>();

			words.addAll(tmp);
			sortedLineProfil.put(keyCopy, words);
		}
					
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 * 
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Double[] nbofs = sortedLineProfil.keySet().toArray(new Double[0]);

		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = nbofs.length;

		while (i-- != 0) {
			Double nbof = nbofs[i];

			for (Text line : sortedLineProfil.get(nbof))
				context.write(new DoubleWritable(nbof) , line);
		}
	}
}
	
// =========================================================================
// MAPPER B
// =========================================================================

class MapB extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public boolean isDouble( String str ){
		  try{
		    Double.parseDouble( str );
		    return true;}
		  catch( Exception e ){
		    return false;
		  }
		}

	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String Entry = value.toString();
		String[] Tokens = Entry.split("\\t");
//		System.out.println(Tokens[0]);
//		System.out.println(Tokens[1]);
		if (Arrays.equals(Tokens, emptyWords))
			return;

		if (isDouble(Tokens[0])){
			context.write(new DoubleWritable(Double.parseDouble(Tokens[0])), new Text (Tokens[1]));
		}		
	}
}

// =========================================================================
// REDUCER B
// =========================================================================

class ReduceB extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	

	
	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 * 
	 * Il associe une fréquence à une liste de mots.
	 */
	private LinkedHashMap<Double, List<Text>> sortedLineProfil = new LinkedHashMap<>();
	private int nbsortedWords = 0;
	private int k;

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		
		System.out.println(key.toString());
		
		// On copie car l'objet key reste le même entre chaque appel du reducer
		Double keyCopy = key.get();
		
		List tmp = new ArrayList();
		for (Text val : values)
			tmp.add(new Text(val));


		// Fréquence déjà présente
		if (sortedLineProfil.containsKey(keyCopy))
			sortedLineProfil.get(keyCopy).addAll(tmp);
		else {
			List<Text> words = new ArrayList<>();

			words.addAll(tmp);
			sortedLineProfil.put(keyCopy, words);
		}
		
		nbsortedWords+=tmp.size();
		
		// Nombre de lignes enregistrés atteint : on supprime le mot les plus grands profil (le premier dans sortedWords)
		while (nbsortedWords > k) {
			Double firstKey = sortedLineProfil.entrySet().iterator().next().getKey();
			List<Text> lines = sortedLineProfil.get(firstKey);
			lines.remove(lines.size() - 1);
			nbsortedWords--;
			if (lines.isEmpty())
				sortedLineProfil.remove(firstKey);
		}
			
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 * 
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Double[] nbofs = sortedLineProfil.keySet().toArray(new Double[0]);

		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = nbofs.length;

		while (i-- != 0) {
			Double nbof = nbofs[i];

			for (Text words : sortedLineProfil.get(nbof))
				context.write(new DoubleWritable(nbof) , words);
		}
	}
}

public class TopKWordCountReverse {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/TopkWordCount-";
	private static final Logger LOG = Logger.getLogger(TopKWordCountReverse.class.getName());

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
		int k = 30;

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


		Job jobA = new Job(confA, "Blala");

		
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(Text.class);
		


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
		
		Job jobB = new Job(confB, "Blala2");
		
		/*
		 * Affectation de la classe du comparateur au job.
		 * Celui-ci sera appelé durant la phase de shuffle.
		 */
		jobB.setSortComparatorClass(DoubleInverseComparator.class);
		
		jobB.setOutputKeyClass(DoubleWritable.class);
		jobB.setOutputValueClass(Text.class);
		
		jobB.setMapperClass(MapB.class);
		jobB.setReducerClass(ReduceB.class);
		
		FileInputFormat.addInputPath(jobB, new Path(outputPath));
		FileOutputFormat.setOutputPath(jobB, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));
		
		jobB.waitForCompletion(true);
	}
}