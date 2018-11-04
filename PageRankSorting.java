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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*
 * This class is used to sort the pagerank result from the PageRank Map reduce
 * 
 * Map function will produce the output value as PageRank as key and its document name as value
 * I am using a PageRankKeyGroupingComparator to sort the output from Map based on its key in the DESC order
 * Reduce function produce the result as Key and Value as PageRank and document name. There can be multiple doc with same Page Rank . That also handled in Reducer.
 */


public class PageRankSorting extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		System.out.println("Entered PR sortt");
				
		Job job = Job.getInstance(getConf(), "PageRankSorting");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setSortComparatorClass(PageRankKeyGroupingComparator.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setNumReduceTasks(1);
		int result = job.waitForCompletion(true) ? 0 : 1;
		
		//deleting the input file directory
		FileSystem fileSystem = FileSystem.get(getConf());
		Path deletePth = new Path(args[2]);
		if (fileSystem.exists(deletePth)) {
			fileSystem.delete(deletePth, true);
		}

		return result;
		
	}
	
	public static class Map extends Mapper<Text, Text, DoubleWritable, Text> {
		public void map(Text text, Text lineText, Context context) throws IOException, InterruptedException {
			
		  String TextString= text.toString();
		//  String[] docPR= TextString.split("%%%%%");
		 // String docName=docPR[0];
		  String docName=TextString;
		//  String linkAndPR=docPR[1];
		  String linkAndPR=lineText.toString();
		  String[] linkAndPRArr=linkAndPR.split("%#####%");
		  String PRString=linkAndPRArr[linkAndPRArr.length-1];
		  Double PR=Double.parseDouble(PRString);
		  context.write(new DoubleWritable(PR),new Text(docName));
			
		}
		
		
		
	}
	
	 public static class Reduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		 
		 @Override
			public void reduce(DoubleWritable pageRank, Iterable<Text> docs,
					Context context) throws IOException, InterruptedException {
		 
			 for(Text doc:docs)
			 {
				 
				 context.write(doc, pageRank);
			 }
		 
		 
	 }
	 
	
}
}

 class PageRankKeyGroupingComparator extends WritableComparator {
    protected PageRankKeyGroupingComparator() {
        super(DoubleWritable.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable k1 = (DoubleWritable)w1;
        DoubleWritable k2 = (DoubleWritable)w2;
         
        return -1*k1.compareTo(k2);
    }
}
