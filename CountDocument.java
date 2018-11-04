/*
 * Author: Arun Kunnumpuram Thomas
 * Student ID: 801027386
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/*
 * CountDocument will findout the number of documents in the given corpus. 
 * map will read each line in the doc output the key value as doc 1
 * reducer will add the values for doc key and find out the total number of documents.
 * Output is written to the args[1]
 * 
 */
public class CountDocument extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf(), "CountDocument");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/* Map class read each line and produce the result as doc,1 as the output*/
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
					
			String line = lineText.toString();
		
			if (line != null && line.length() > 0) {
				context.write(new Text("doc"), new IntWritable(1));
			}
		}
      }

  /* Reducer class will read the ouput from Map class and findout the no of outputs*/
	
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text word, Iterable<IntWritable> counts,
			Context context) throws IOException, InterruptedException {
		int noOfDocs = 0;
		for (IntWritable count : counts) {
			noOfDocs += count.get();
		}
		context.write(new Text("NoOfDocs"), new IntWritable(noOfDocs));
	}
}

	
	
	
	

}
