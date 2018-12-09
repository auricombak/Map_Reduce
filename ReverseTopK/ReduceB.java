package ReverseTopK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

class ReduceB extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {



	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 *
	 * Il associe une fréquence à une liste de mots.
	 */
	private LinkedHashMap<Double, List<Text>> sortedLineProfil = new LinkedHashMap<>();
	private int nbClients = 0;
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
			List<Text> clients = new ArrayList<>();

			clients.addAll(tmp);
			sortedLineProfil.put(keyCopy, clients);
		}

		nbClients+=tmp.size();

		while (nbClients > k) {
			Double firstKey = sortedLineProfil.entrySet().iterator().next().getKey();
			List<Text> lines = sortedLineProfil.get(firstKey);
			lines.remove(lines.size() - 1);
			nbClients--;
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

			for (Text clients : sortedLineProfil.get(nbof))
				context.write(new DoubleWritable(nbof) , clients);
		}
	}
}
