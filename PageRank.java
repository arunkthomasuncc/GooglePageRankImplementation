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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.Text; 
/*
 * PageRank Map function read the input from LinkGraph generator. Map function calcualte the page rank for each link in the document and produce the result as
 * linkName and its pagerank also it produce the actual document name and its output link information
 * 
 * Reducer will find out the sum of page rank for a each key and produce the value as document name and its output link information along with the new page rank
 * 
 * Before returning the result, it deletes the LinkGraph Output
 * 
 */
public class PageRank extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Entered PR");
		Job job = Job.getInstance(getConf(), "PageRank");
		job.setJarByClass(this.getClass());
		
	
		//setting it to the configuration file so that it can be used in reducer to calcualte the initial page rank
				
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		
		
		int result= job.waitForCompletion(true) ? 0 : 1;
		//deleting the input file directory
		FileSystem fileSystem = FileSystem.get(getConf());
		Path deletePth = new Path(args[0]);
		if (fileSystem.exists(deletePth)) {
			fileSystem.delete(deletePth, true);
		}

		return result;
		
		

	}
	public static class Map extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text lineText, Context context) throws IOException, InterruptedException {
			
			//String[] lineTextString=lineText.toString().split("%%%%%");
		//	String docName=lineTextString[0].trim();
		//	String linksAndPR=lineTextString[1].trim();
			
			String lineTextStr=lineText.toString();
			String[] link=lineTextStr.split("%#####%");
			
			String pageRank=link[link.length-1];
			Double splittedPageRank= Double.parseDouble(pageRank)/(link.length-1);
			StringBuilder sb= new StringBuilder();
			for(int i=0;i<link.length-1;i++)
			{
				sb.append(link[i]+"%%%%%");
				context.write(new Text(link[i]),new Text(splittedPageRank.toString()));
			}
			//handling dangling node
		//	if(link.length>1)
		//	context.write(new Text(docName),new Text(sb.toString()));
				context.write(key,new Text(sb.toString()));
			
		}

}
	  public static class Reduce extends Reducer<Text, Text, Text, Text> {
			@Override
			public void reduce(Text word, Iterable<Text> links,
					Context context) throws IOException, InterruptedException {
				
				Double pr=0.0;
				String docName=word.toString();
				StringBuilder sb= new StringBuilder();
				boolean isItNewPage=true;
			//	boolean isDanglingNode=true;
				
				for(Text s:links)
				{
					String str=s.toString();
					if(str.contains("%%%%%"))
					{
						
						String[] outputLinks=str.split("%%%%%");
						for(String ele:outputLinks)
						{
							sb.append(ele+"%#####%");
						}
						isItNewPage=false;
						
					}
					else
					{
						if(str.length()>0)
						{
						pr=  pr+Double.parseDouble(str);
					//	isDanglingNode=false;
						
						}
						
						
					}
				}
				
				
				if(!isItNewPage )
				{   
					Double pageRank = (1 - 0.85) + (0.85 * pr);
					sb.append(pageRank.toString());
				//	docName=docName+"%%%%%";
				//	System.out.println("Doc Name "+docName+"PR "+pageRank.toString());
					context.write(new Text(docName),new Text(sb.toString()));
				}
			}
}
	  
}