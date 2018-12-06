package ReverseTopK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

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