/*
 * Author: Arun Kunnumpuram Thomas
 * Student ID: 801027386
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
/*
 * Run method read the ourput file from CountDocument and store the no of documents in the configuration.
 * Map function will go through each line in the given corpus and extract the each document title and its output link information
 * link information is seperated by a 'link sepearator' and pass it to a reduce function.
 * Reducer function will produce the key value as documentname and its output link details with initial page rank
 * initial page rank is taken as 1/total no of documents.
 * 
 * Before returning the job status result, deleting the ouput file from CountDocument
 */

public class LinkGraphGenerator extends Configured implements Tool{
	
	static String linkSeperator="%#####%";
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//reading the total number of documents from the ouptut file of CountDocument job
		
		Long totalNoOfDocs= getTotalNumberOfDocs(args[1]+"/part-r-00000");
		Configuration conf = new Configuration();
		conf.set("numberOfDocs", totalNoOfDocs.toString());
		Job job = Job.getInstance(conf, "LinkGraphGenerator");
		job.setJarByClass(this.getClass());
		
	
		//setting it to the configuration file so that it can be used in reducer to calculate the initial page rank
				
		System.out.println("Total Number of docs"+totalNoOfDocs);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		int result= job.waitForCompletion(true) ? 0 : 1;
		FileSystem fileSystem = FileSystem.get(conf);
		Path deletePth = new Path(args[1]);
		if (fileSystem.exists(deletePth)) {
			
			fileSystem.delete(deletePth, true);
		}
		
		return result;

	}
	
	public long getTotalNumberOfDocs(String fileStringPath) throws IOException
	{
		Configuration conf = new Configuration();
		Path filePath= new Path(fileStringPath);
		FileSystem fileSystem = filePath.getFileSystem(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader( fileSystem.open(filePath)));
		String line=br.readLine();
		long count=0;
		if(line !=null)
		{
			String[] arr=line.split("\\s+");
			count=Long.parseLong(arr[1]);
		}
		//conf.set("numberOfDocs", count+"");
		return count;
		
		
	}
	
/* Map class read the input file and for each doc, it produce the result of its output link as text value with some seperator*/
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String lineStringText=lineText.toString();		
			Configuration config=context.getConfiguration();
			final Pattern pattern = Pattern.compile("<title>(.+?)</title>");
			final Matcher matcher = pattern.matcher(lineStringText);
			
			if(matcher.find())
			{
				StringBuilder result= new StringBuilder();
				String title=matcher.group(1);
				List<String> links=getTagValues(lineStringText);
				for(String link: links)
				{
					result.append(link+linkSeperator);
				}
				
				context.write(new Text(title), new Text(result.toString()));
			}
			
		}
      }
	
	private static List<String> getTagValues(final String str) {
		//first get the content inside the <text >  </text> tag because there are other tags also like comment
		final List<String> tagValues = new ArrayList<String>();
		final Pattern textPattern = Pattern.compile("<text.*?>(.+?)</text>");
		final Matcher textMatcher = textPattern.matcher(str);
		if(textMatcher.find())
		{
			String textContent=textMatcher.group(1);
			final Pattern TAG_REGEX = Pattern.compile("\\[\\[(.+?)\\]\\]");
		   
		    final Matcher matcher = TAG_REGEX.matcher(textContent);
		    while (matcher.find()) {
		        tagValues.add(matcher.group(1));
		    } 
			
		}
		 return tagValues;
		
	}


   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text word, Iterable<Text> links,
			Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
  	    String totalNumberOfDocsString = conf.get("numberOfDocs");
  	    long totalNumberOfDocs= Long.parseLong(totalNumberOfDocsString);
  	    double initialPageRank= 1/(double)(totalNumberOfDocs);
		String docName=word.toString();
		int count=0;
		StringBuilder result=new StringBuilder();
		for(Text link: links)
		{
			
			result.append(link);
		}
		result.append(initialPageRank);
		
		//appending a seperator to handle the space case in title
		//docName=docName+"%%%%%";
		context.write(new Text(docName), new Text(result.toString()));
		
		
	}

}
}
