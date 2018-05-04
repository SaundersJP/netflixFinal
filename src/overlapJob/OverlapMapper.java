package overlapJob;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class OverlapMapper extends
		Mapper<LongWritable, Text, IntIntWritable, DoubleDoubleWritable> {
	
	HashMap<Integer, List<Pair<Integer, Double>> > boughtItemMap = new HashMap<Integer, List<Pair<Integer, Double>> >();
	int count = 1;


	/*
	 * The map method runs once for each line of text in the input file. The
	 * method receives a key of type LongWritable, a value of type Text, and a
	 * Context object.
	 */
	@Override
	public void setup(Context context) {
		
		
		try {
			//Path path = new Path("cacheItems.txt");
			//FileSystem fs = FileSystem.getLocal(context.getConfiguration());

			FileReader items = new FileReader("/home/cloudera/netflixTest/cacheItems.txt");
			BufferedReader itemsBuff = new BufferedReader( items/*new InputStreamReader(fs.open(path))*/);
			String currentLine;

			// while we can get a line from positive
			while ((currentLine = itemsBuff.readLine()) != null) {
				
				String[] data = currentLine.split("\\s+");
				int movieID = Integer.parseInt(data[0]);
				int userID = Integer.parseInt(data[1]);
				Double rating = Double.parseDouble(data[2]);
				//System.out.print(Arrays.toString(data) + "\n");
				
				if(!boughtItemMap.containsKey(movieID)){
					List<Pair<Integer,Double>> userRatings = new LinkedList<Pair<Integer,Double>>();
					boughtItemMap.put(movieID, userRatings);
				}
				
				Pair<Integer, Double> myPair = new Pair<Integer, Double>(userID, rating);
				boughtItemMap.get(movieID).add(myPair);
			}
			itemsBuff.close();
			items.close();
			
		} catch (IOException e) {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		IntIntWritable itemPair = new IntIntWritable();
		DoubleDoubleWritable ratingPair = new DoubleDoubleWritable();
		
		String line = value.toString();
		String[] data = line.split("\\s+");
		int movieID = Integer.parseInt(data[0]);
		int userID = Integer.parseInt(data[1]);
		Double rating = Double.parseDouble(data[2]);
		
		for(Integer cacheMovie : boughtItemMap.keySet()){
			if(cacheMovie == movieID){
				continue;
			}
			
			for( Pair<Integer, Double> cacheUserRating : boughtItemMap.get(cacheMovie) ){
				Integer cacheUser = cacheUserRating.getLeft();
				//System.out.println("CacheMovie = " + cacheMovie + " | CacheUser = " + cacheUser);
				if(userID == cacheUser){
					Double cacheRating = cacheUserRating.getRight();
					itemPair.setLeft(cacheMovie);
					itemPair.setRight(movieID);
					ratingPair.setLeft(cacheRating);
					ratingPair.setRight(rating);
					/* System.out.println("("+ cacheMovie + ", " + movieID + ") | cacheRating = " + cacheRating + " | lineRating = " + rating); */
					context.write(itemPair, ratingPair);
				}
			}
		}
	}

}