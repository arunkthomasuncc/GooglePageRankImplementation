/*
 * Author: Arun Kunnumpuram Thomas
 * Student ID: 801027386
 */
import org.apache.hadoop.util.ToolRunner;
/*
 * Driver class run 4  following map reduce jobs sequentially
 * a) CountDocument map reducer to find out the number of docs in the given corpus
 * b) LinkGraphGenerator map reducer to generate the link graph. It will find the output links of each document and initial pagerank for the document
 * c) PageRank map reducer iteratively to find the pagerank using page rank formula
 * d) PageRankSorting to sort the page rank in DESC order. 
 */

public class Driver {

	
	 public static void main( String[] args) throws  Exception {
		
		 //calling the DocumentCount  to find the number of docs
		 int resultDocumentCount = ToolRunner.run(new CountDocument(), args);
		 if(resultDocumentCount==0)
		 {
		
			 //running the linkGraph generator
			 int LinkGraphResult= ToolRunner.run(new LinkGraphGenerator(),args);
			 int pageRankResult=0;
			 
			 if(LinkGraphResult ==0)
			 {  
				 String[] pageRanksInputs= new String[2];
				 //calling PageRank map reducer iteratively for 10 times
				 for(int i=0;i<10;i++)
				 {
					if(i==0)
					{
							 pageRanksInputs[0]=args[2];
							 pageRanksInputs[1]=args[2]+i;		 
					 }
					 else if(i==9)
					 {
						 pageRanksInputs[0]= pageRanksInputs[1];
						 pageRanksInputs[1]=args[2];
					 }
					 else
					 {
						 pageRanksInputs[0]=pageRanksInputs[1];
						 pageRanksInputs[1]= args[2]+i;
						 
					 }
					 pageRankResult = ToolRunner.run(new PageRank(),pageRanksInputs);
					 
				 }
				 if(pageRankResult ==0)
				 {
					 //calling PageRankSorting for sorting the page rank DESC
					 int Result=ToolRunner.run(new PageRankSorting(),args);
					 System.exit(Result);
				 
				 }
				 
			 }
		 }
		 
		 
		 
		 
	 } 
	 
}
