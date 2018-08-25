######################Assignment 3#####################

###########Task 1###########################################

Dataset is ample data of songs heared by users on an online streaming platform.The Description of data set attached in musicdata.txt is as follows:-

* 1st column -UserId
* 2nd column -TrackId
* 3rd column -Songs Share Status(1 for shared,0 for not shared)
* 4rd column -Listening Platform(Radio or web-0 for radio,1 for web)
* 5th column -Song Listening Status(0 for Skipped, 1 for Fully heared)


Write the mapreduce program for following tasks.

Task 1.1 
********
Find the number of unique listeners in the dataset.



package com;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Mapper class to filter the Company name and provide it as input for reducer

public class OnidaStateWise {
	public static class Map extends Mapper<LongWritable, Text,Text,IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fileWords = value.toString().split("\\|");
			String CompanyName = fileWords[0];
			String StateName = fileWords[3];
			
			if(CompanyName=="Onida") {
				context.write(new  Text(StateName), new IntWritable(1));
			}
		}
	}

//Reducer class to process the number of listeners in the given dataset

   public static class Reduce extends Reducer<Text,IntWritable,Text, IntWritable>{
	   @Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context con) throws IOException, InterruptedException {
		   int sum= 0;
		   
		   for(IntWritable val : value) {
			   sum=sum+val.get();
			  }
		con.write(key,new IntWritable(sum));
	}
   }

//Driver class to set all the input and output configuration.

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration con =  new Configuration();
	Job job = new Job(con, "OnidaStateWise");
	job.setJarByClass(OnidaStateWise.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)?0:1);
	
	
	
}
}

//The above class are converted into jar file as Unique.jar and musicdata.txt is taken as dataset for procesing and output stored in music_unique

[acadgild@localhost ~]$ hadoop jar Unique.jar /musicdata.txt /music_unique

[acadgild@localhost ~]$ hadoop fs -cat /music_unique/*
18/08/15 21:20:49 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
111113
111115
111117
[acadgild@localhost ~]$ 

####################################################################################

Task 1.2
********
Find the number of times a song was shared.

package com.music;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SongShared {
	public static class Map extends Mapper<IntWritable,Text,IntWritable,IntWritable>{
		@Override
		protected void map(IntWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String[] contentArray = value.toString().split("\\|");
			IntWritable TrackId = new IntWritable(Integer.parseInt(contentArray[1]));
			IntWritable SongShared = new IntWritable(Integer.parseInt(contentArray[2]));
			context.write(TrackId,SongShared);
		}
	}
    public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
    	@Override
    	protected void reduce(IntWritable key, Iterable<IntWritable> value,
    			Context context)
    			throws IOException, InterruptedException {
             int i = 0;
             for(IntWritable val : value) {
            	 i=i+val.get();
            	 context.write(key,new IntWritable(i));
             }
    	}
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"SongShared");
		job.setJarByClass(SongShared.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
	    System.exit(job.waitForCompletion(true)?0:1);
		
	}
}

[acadgild@localhost ~]$ hadoop jar songshared.jar /musicdata.txt /music_shared1

[acadgild@localhost ~]$ hadoop fs -cat /music_shared1/*
18/08/15 22:10:51 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
222	0
223	0
225	1
225	2
You have new mail in /var/spool/mail/acadgild
[acadgild@localhost ~]$

##########################################################################

Task 1.3
********
What are the number of times a song was heared.

package com.music;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SongHeard {
      public static class Map extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
    	  @Override
    	protected void map(LongWritable key, Text value,
    			Context context)
    			throws IOException, InterruptedException {
    		  String[] contentArray = value.toString().split("\\|");
    		  IntWritable TrackId = new IntWritable(Integer.parseInt(contentArray[1]));
    		  IntWritable SongShared = new IntWritable(Integer.parseInt(contentArray[4]));
    		  context.write(TrackId, SongShared);
    	
    	}
      }
      
      public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
    	  @Override
    	protected void reduce(IntWritable key, Iterable<IntWritable> value,
    			
    			Context context)
    			throws IOException, InterruptedException {
    		  int i = 0;
    		  for(IntWritable val : value) {
    			  i=i+val.get();
    			  context.write(key, new IntWritable(i));
    			  
    		  }
    	
    	}
      }
      public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"SongHeard");
		job.setJarByClass(SongHeard.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}


[acadgild@localhost ~]$ hadoop jar heared.jar /musicdata.txt /music_heard

[acadgild@localhost ~]$ hadoop fs -cat /music_heard/*
18/08/15 22:17:10 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
222	0
223	1
225	0
225	0
[acadgild@localhost ~]$









Sqoop
*****
Task 2.2
********
Use Sqoop tool to import data present in SQOOPOUT folder made while demo of import table
****************************************************************************************

//IMPORTING  THE TABLE FROM MYSQL BY USING THE FOLLOWING COMMANDS//

[acadgild@localhost ~]$ sqoop importt --connect jdbc:mysql://localhost/simplidb --username root -P  --query "select * from SQOOPOUT where PersonId=101"
Warning: /home/acadgild/install/sqoop/sqoop-1.4.6.bin__hadoop-2.0.4-alpha/../hcatalog does not exist! HCatalog jobs will fail.
Please set $HCAT_HOME to the root of your HCatalog installation.
Warning: /home/acadgild/install/sqoop/sqoop-1.4.6.bin__hadoop-2.0.4-alpha/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
18/07/27 16:05:25 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6
Enter password: 
18/07/27 16:05:31 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
Fri Jul 27 16:05:32 IST 2018 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
-----------------------------------------------------------------------------------------------------------
| PersonId    | LastName             | FirstName            | Address              | city                 | 
-----------------------------------------------------------------------------------------------------------
| 101         | kumar                | krishna              | kovilpatti           | Tuticorin            | 
| 101         | kandan               | Mani                 | kovilpatti           | Tuticorin            | 
| 101         | kandan               | Mani                 | hasthampattty        | Tuticorin            | 
-----------------------------------------------------------------------------------------------------------
[acadgild@localhost ~]$ 

Task 2.1
********

//*Use Sqoop tool to export data present in SQOOPOUT folder made while demo of import table

//Using simplidb datadase for table creation

mysql> use simplidb;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

//Creatig table Sqoopout with id, name , and salary as parameters

Database changed
mysql> create table Sqoopout(id int,name varchar(30),salary float);
Query OK, 0 rows affected (0.20 sec)

//Manually created the file Sqoopout and pasted in hadoop fs

[acadgild@localhost ~]$ hadoop fs -put /home/acadgild/Sqoopout /


//Using the below Sqoop export command to export the data from HDFS to mysql as data injection.


[acadgild@localhost ~]$ sqoop export --connect jdbc:mysql://localhost/simplidb --table Sqoopout --export-dir /Sqoopout --username root --password Root@123 --input-fields-terminated-by ','

//File Successfully Exported to Mysql database

18/08/22 23:32:52 INFO mapreduce.ExportJobBase: Exported 5 records.
You have new mail in /var/spool/mail/acadgild
[acadgild@localhost ~]$


//* Output Snippet from Mysql as Sqoop Export from HDFS

mysql> select * from Sqoopout;
+------+------------+--------+
| id   | name       | salary |
+------+------------+--------+
|  503 | Krishna    |  25000 |
|  502 | ManiKandan |   2000 |
|  500 | Pandya     |   1000 |
|  504 | kumar      |   3000 |
|  501 | Raj        |   1500 |
+------+------------+--------+
5 rows in set (0.02 sec)

mysql> 


####################################END#######################################



*****************************************************************************************





