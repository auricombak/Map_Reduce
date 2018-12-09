package Amazon_Huge_Comments;

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

class ReduceB extends Reducer<DoubleWritable, Text, Text, Text> {
	

	
	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 * 
	 * Il associe une fréquence à une liste de mots.
	 */
	private TreeMap<Double, List<Text>> reviewersPerCommentSize = new TreeMap<>();
	private int nbReviewers = 0;
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
		
		
		// On copie car l'objet key reste le même entre chaque appel du reducer
		double keyCopy = key.get();
		
		List tmp = new ArrayList();
		for (Text val : values)
			tmp.add(new Text(val));


		// Fréquence déjà présente
		if (reviewersPerCommentSize.containsKey(keyCopy))
			reviewersPerCommentSize.get(keyCopy).addAll(tmp);
		else {
			reviewersPerCommentSize.put(keyCopy, tmp);
		}
		nbReviewers+=tmp.size();
		
		// Nombre de dates enregistrés atteint : on supprime les dates ayant la plus petite frequence
		while (nbReviewers > k) {
			Double firstKey = reviewersPerCommentSize.firstEntry().getKey();
			List<Text> lines = reviewersPerCommentSize.get(firstKey);
			lines.remove(lines.size() - 1);
			nbReviewers--;
			if (lines.isEmpty())
				reviewersPerCommentSize.remove(firstKey);
		}
			
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 * 
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Double[] nbofs = reviewersPerCommentSize.keySet().toArray(new Double[0]);

			
		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = nbofs.length;

		while (i-- != 0) {
			Double nbof = nbofs[i];

			for (Text name : reviewersPerCommentSize.get(nbof))
				context.write(new Text("AVG comment size : " + String.format( "%.0f",nbof) + "words per comment ") , new Text(name));
		}
	}
}
