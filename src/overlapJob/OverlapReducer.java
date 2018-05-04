package overlapJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OverlapReducer
		extends
		Reducer<IntIntWritable, DoubleDoubleWritable, IntIntWritable, DoubleWritable> {
	
	@Override
	public void reduce(IntIntWritable key,
			Iterable<DoubleDoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		/*
		 * For each value in the set of values passed to us by the mapper emit
		 * only the FIRST of its values.
		 */
		// Text myText = new Text("");
		// while( values.iterator().hasNext() ){
		// myText.set(values.iterator().next() );
		// }
		Iterator<DoubleDoubleWritable> myIter = values.iterator();
		DoubleWritable similarityWritable = new DoubleWritable();
		Double dotProduct = 0.0;
		Double cacheSquaredSum = 0.0;
		Double itemSquaredSum = 0.0;

		int count = 0;

		while (myIter.hasNext()) {

			DoubleDoubleWritable currPair = myIter.next();

			Double currCache = currPair.getLeft();
			Double currItem = currPair.getRight();

			dotProduct += (currCache * currItem);
			cacheSquaredSum += (currCache * currCache);
			itemSquaredSum += (currItem * currItem);
			count++;
		}

		if (count > 1) {
			Double similarity = (dotProduct / (Math.sqrt(cacheSquaredSum) * Math
					.sqrt(itemSquaredSum)));
			similarityWritable.set(similarity);
			context.write(key, similarityWritable);
		}
	}

}