package lab.CDS_AllPairs;




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

public class AllPairs extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new AllPairs(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	  
//check if the required number of arguments are provided.	  
/*	  if (args.length < 2) {
			System.err.printf("Usage: %s needs minimum three arguments, input and output directories\n", getClass().getSimpleName());
			return -1;
		}
	
	    */
	//create a new Job, set the name of the job and the main class.    
	    Configuration conf = getConf();
	    Job job = new Job(conf, this.getClass().toString());
	    job.setJobName("WordCount");
	    job.setJarByClass(AllPairs.class);

	    
	  
/*    Path inputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/testing/sample.dat");
    Path outputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output");*/
	    
	    
	    Path inputPath = new Path("/home/cloudera/wc_in_out/shiva_output2");
	    Path outputPath = new Path("/home/cloudera/wc_in_out/shiva_output3");
	    
	    

//If output directory already exists in the file system, then delete that directory.
    // configuration should contain reference to your namenode
    FileSystem fs = FileSystem.get(new Configuration());
    // true stands for recursively deleting the folder you gave
/*    fs.delete(new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output"), true);
*/
    
    fs.delete(new Path("/home/cloudera/wc_in_out/shiva_output3"), true);

    
//set the input and the output paths from the arguments.   
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

 
//    set the key value type classes and the output format class. These classes need to be the same type whichw e use in the map and reduce for the output.
  
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setMapperClass(JoinBMapper.class);
  //  job.setCombinerClass(Reduce.class);
    
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

  
  public static class JoinBMapper extends
  Mapper<LongWritable, Text, Text, Text> {
	  
    @Override
    public void map(LongWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    		
    	
    	String val = value.toString();
    	String[] splitNameRating = val.split(" && ");
    	String name = splitNameRating[0];
    	String rating = splitNameRating[1];
    	
    	System.out.println(name+"   "+rating);
    	
    	name = name.substring(1, name.length()-1);
    	
    	context.write(new Text(name), new Text(rating));
       
    }
  }


  public static class Reduce extends Reducer<Text, Text, Text, CompletePairs> {

	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
String allRatings = "";
int count=0;

    	for(Text value: values) {
    		
    	//	System.out.println(key+"    "+value);
    		if(count ==0)
    			allRatings = value.toString();
    		else
    			allRatings = allRatings+" , "+value.toString();
    		
    		
    	//	System.out.println(value.toString());


    	count++;
    		
    	}
    	
		context.write(null, new CompletePairs(key,allRatings));
    	/*System.out.println(key+ "   "+allRatings);
    	System.out.println(" ");*/

	//	context.write(key, new Text(allRatings));

    }
  }
  

  
  
}


