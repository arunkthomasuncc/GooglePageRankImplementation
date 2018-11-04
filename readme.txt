Creating the jar file
---------------------------------------------------------
1. Open Cloudera VM and create a java project in Eclipse
2. Add the Driver.java,CountDocument.java,LinkGraphGenerator.java,PageRank.java and PageRanknSorting.java to the src folder
3. Right click on the project and click on Build path -> Configure Build path -> Add External Jar 
   3.a go to usr/lib/hadoop and add all jars except the folders
   3.b same location add jars inside client-0.20 folder
   If you are running the project in eclipse, you may need to add jackson-core-asl.jar and jackson-mapper-asl.jar
4. After adding the jars, make sure there is no error in the files.
5. Right click on the project, click export -> java -> jar file and give name as pagerank.jar and location as
   /home/cloudera/Desktop/pagerank/pagerank.jar
   
Input files
----------------------------------------------------------
1. Download the simplewiki-20150901-pages-articles-processed.xml.gz zip from canvas
2. unzip the file and copy simplewiki-20150901-pages-articles-processed.xml
   to /home/cloudera/Desktop/pagerank/input folder
3. open the terminal and execute the following commands to copy the files to hdfs input directory
   $ sudo su hdfs
   $ hadoop fs -mkdir /user/cloudera
   $ hadoop fs -chown cloudera /user/cloudera
   $ exit
   $ sudo su cloudera
   $ hadoop fs -mkdir /user/cloudera/pagerank /user/cloudera/pagerank/input 
   $ hadoop fs -put /home/cloudera/Desktop/pagerank/input/*  /user/cloudera/pagerank/input

Running pagerank 
-----------------------------------------------------------------
1. run following command
     
    $ hadoop jar <jar location> Driver <hdfs input file location> <hdfs document count location> <hdfs linkgraph output location> <hdfs output file location>
   ex:  
	hadoop jar /home/cloudera/Desktop/pagerank/pagerank.jar Driver /user/cloudera/pagerank/input /user/cloudera/pagerank/doccount /user/cloudera/pagerank/linkgraph /user/cloudera/pagerank/output
	
	1 st argument to the Driver class is the input file location in the HDFS
	2 nd argument is the location where Document Count program writes it's output
	3 rd argument is the location where LinkGraphGenerator generates its output
	4 th argument is the location where the final output will be stored

Please note that the 2nd and 3 rd location will be removed by the argument during the program execution. Only the input and output
folders remains after the execution of the program.

2.  You can see the output 
       $ hdfs fs -cat /user/cloudera/pagerank/output/*
3. you can copy the result to local machine (cloudera vm) from hdfs location by 
	   $ hdfs fs -get /user/cloudera/pagerank/output/part-r-00000 <local location>
	   
