package lab.CDS_CombineTwoFiles;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Combine extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Combine(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	  
//check if the required number of arguments are provided.	  
/*	  if (args.length < 3) {
			System.err.printf("Usage: %s needs minimum three arguments, input and output directories\n", getClass().getSimpleName());
			return -1;
		}
	    */
	//create a new Job, set the name of the job and the main class.    
	    Configuration conf = getConf();
	    Job job = new Job(conf, this.getClass().toString());
	    job.setJobName("WordCount");
	    job.setJarByClass(Combine.class);

	    
	    Path AinputPath = new Path("/home/cloudera/wc_in_out/shiva_input/movies.dat");
	    Path BinputPath = new Path("/home/cloudera/wc_in_out/shiva_input/ratings.dat");
	    Path outputPath = new Path("/home/cloudera/wc_in_out/shiva_output1");
	    
	    
	    
/*    Path AinputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/input/movies.dat");
    Path BinputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/input/ratings.dat");
    Path outputPath = new Path("/home/cloudera/wc_in_out/shiva_output1");
*/
//If output directory already exists in the file system, then delete that directory.
    // configuration should contain reference to your namenode
    FileSystem fs = FileSystem.get(new Configuration());
    // true stands for recursively deleting the folder you gave
   // fs.delete(new Path("/home/cloudera/wc_in_out/CDS_Project/output"), true);
    
    fs.delete(new Path("/home/cloudera/wc_in_out/shiva_output1"), true);
    
//set the input and the output paths from the arguments.   
    MultipleInputs.addInputPath(job, AinputPath, TextInputFormat.class, JoinAMapper.class);
    MultipleInputs.addInputPath(job, BinputPath, TextInputFormat.class, JoinBMapper.class);
    
    FileOutputFormat.setOutputPath(job, outputPath);

 
//    set the key value type classes and the output format class. These classes need to be the same type whichw e use in the map and reduce for the output.
  
    //job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    //job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

//execute the job and wait for its completion
    int returnValue = job.waitForCompletion(true) ? 0 : 1;
    
    if(job.isSuccessful()) {
    	
    		            System.out.println("Job was successful");
    	
    		        } else if(!job.isSuccessful()) {
    	
    		            System.out.println("Job was not successful");          
    	
    		        }
    return returnValue; 
  }


  public static class JoinAMapper extends
  Mapper<LongWritable, Text, LongWritable, Text> {
	  
	  
    @Override
    public void map(LongWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {

    	
        String valueString = value.toString();
        String[] SingleNodeData = valueString.split("::");
       // System.out.println("after first mapper "+Long.parseLong(SingleNodeData[0])+" "+SingleNodeData[1] );

        context.write(new LongWritable(Long.parseLong(SingleNodeData[0])), new Text("fromOne,"+SingleNodeData[1]));
       
    }
  }

  
  public static class JoinBMapper extends
  Mapper<LongWritable, Text, LongWritable, Text> {
	  
    @Override
    public void map(LongWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    	//System.out.println(value);

        String valueString = value.toString();
        String[] SingleNodeData = valueString.split("::");
        
        System.out.println("after second Mapper: Key is:"+ Long.parseLong(SingleNodeData[1])+" Value is:  "+SingleNodeData[0]+"   "+SingleNodeData[2]);


        context.write(new LongWritable(Long.parseLong(SingleNodeData[1])),new Text("fromTwo,"+SingleNodeData[0]+"::"+SingleNodeData[1]+"::"+SingleNodeData[2]));
       
    }
  }

  public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
    	  String Mname = "NotAvailable";
    	  String merge = "";
    	  String UserID = "";
    	  boolean ratingStatus = false;
    	 
ArrayList<Text> cache = new ArrayList<Text>();


/*for(Text val:values)
	System.out.println("In reducer: "+ val.toString());
*/


    	for(Text value: values) {
//System.out.println(key+" "+value);

    	//	System.out.println("In reducer: "+ value.toString());

String[] a = value.toString().split(",");

    		if(a[0].equals("fromOne")) {
    			Mname = a[1];
    			System.out.println(Mname);
    		}
    		
    		Text writable = new Text();
    		writable.set(value);
    		cache.add(writable);
    		}
    	
    	int length = cache.size();
    	
    	for(int i=0;i<length;i++) {
    		
    		String[] a = cache.get(i).toString().split(",");


    		

    		if(a[0].equals("fromTwo")) {

        		System.out.println("before second is:   "+a[1]);

    			ratingStatus = true;
    			String[] details = a[1].split("::");

    			UserID = details[0]; 
    			merge = Mname+"::"+details[2];
    		//	System.out.println("second "+merge);

    			System.out.println("second is:   "+merge);

        		
        		if(ratingStatus == true && Mname != "NotAvailable"){
        			   // 		System.out.println("after reducer: "+merge);
        			      context.write(new LongWritable(Long.parseLong(UserID)), new Text(","+UserID+","+merge));
        			      }
        		
    		}
    		
    		
    		
    		
    	}
    	


    	System.out.println(" ");

    	
    }
  }
}

