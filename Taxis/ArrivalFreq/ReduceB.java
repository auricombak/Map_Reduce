package ArrivalFreq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

class ReduceB extends Reducer<IntWritable, Text, IntWritable, Text> {
	

	
	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 * 
	 * Il associe une fréquence à une liste de mots.
	 */
	private TreeMap<Integer, List<Text>> datesPerFreq = new TreeMap<>();
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
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		
		// On copie car l'objet key reste le même entre chaque appel du reducer
		int keyCopy = key.get();
		
		List tmp = new ArrayList();
		for (Text val : values)
			tmp.add(new Text(val));


		// Fréquence déjà présente
		if (datesPerFreq.containsKey(keyCopy))
			datesPerFreq.get(keyCopy).addAll(tmp);
		else {
			datesPerFreq.put(keyCopy, tmp);
		}
		System.out.println(tmp.size());
		nbsortedWords+=tmp.size();
		
		// Nombre de dates enregistrés atteint : on supprime les dates ayant la plus petite frequence
		while (nbsortedWords > k) {
			Integer firstKey = datesPerFreq.firstEntry().getKey();
			List<Text> lines = datesPerFreq.get(firstKey);
			lines.remove(lines.size() - 1);
			nbsortedWords--;
			if (lines.isEmpty())
				datesPerFreq.remove(firstKey);
		}
			
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 * 
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Integer[] nbofs = datesPerFreq.keySet().toArray(new Integer[0]);

		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = nbofs.length;

		while (i-- != 0) {
			Integer nbof = nbofs[i];

			for (Text date : datesPerFreq.get(nbof))
				context.write(new IntWritable(nbof) , new Text("Demandes de taxis au départ de la zone : " + date.toString()));
		}
	}
}
