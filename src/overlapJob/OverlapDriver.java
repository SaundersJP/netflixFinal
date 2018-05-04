package overlapJob;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 
 * 
 * @author Cloudera University
 * 
 * This class demonstrates the use of using custom comparators to do secondary sorting.
 * Job input is a text file containing lines in the following format:
 *    Lastname Firstname YYYY-Mon-DD [additional text data]
 * The job will sort the data by last name (ascending) and year (descending), grouped
 * by Lastname/Year.
 *
 */

public class OverlapDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitcode = ToolRunner.run(new Configuration(),
				new OverlapDriver(), args);
		System.exit(exitcode);
	}

	public int run(String[] args) throws Exception {
		

		if (args.length != 2) {
			System.out.printf("Usage: " + this.getClass().getName() +  "<input dir> <output dir>\n");
			System.exit(-1);
		}
		

		Job job = new Job(getConf());
		job.setJarByClass(OverlapDriver.class);

		job.setJobName("Item Overlap");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.addCacheFile(new URI("/netflix/cache/cacheItems.txt#cacheItems.txt"));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(OverlapMapper.class);
		job.setReducerClass(OverlapReducer.class);

		/*
		 * The Mapper outputs StringPair objects in which the first string
		 * is the Last Name field of the record, and the second string is
		 * the Birth Year field of the record.
		 */
		job.setMapOutputKeyClass(IntIntWritable.class);
		job.setMapOutputValueClass(DoubleDoubleWritable.class);		

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(IntIntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
