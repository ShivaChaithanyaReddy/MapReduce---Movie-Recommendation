package lab.CDS_PairingMovies;



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

public class Pairing extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Pairing(), args);
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
	    job.setJarByClass(Pairing.class);

	    
	  
/*    Path inputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/output");
    Path outputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output1");
*/
	    
	    Path inputPath = new Path("/home/cloudera/wc_in_out/shiva_output1");
	    Path outputPath = new Path("/home/cloudera/wc_in_out/shiva_output2");

	    
    FileSystem fs = FileSystem.get(new Configuration());
    // true stands for recursively deleting the folder you gave
/*    fs.delete(new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output1"), true);
*/
    
    fs.delete(new Path("/home/cloudera/wc_in_out/shiva_output2"), true);

    
//set the input and the output paths from the arguments.   
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

 
//    set the key value type classes and the output format class. These classes need to be the same type whichw e use in the map and reduce for the output.
  
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
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
  Mapper<LongWritable, Text, LongWritable, Text> {
	  
    @Override
    public void map(LongWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    	
    	String[] as = value.toString().split(",");
    	
    	//System.out.println(as[0]+"  second one is  "+as[1]+"  Third one is  "+as[2]);
    	
    	context.write(new LongWritable(Long.parseLong(as[1])),new Text(as[2]));
       
    }
  }

  public static class Reduce extends Reducer<LongWritable, Text, MovieTextPairs, BothPairs> {

	//  ArrayList<Text> alist1;
	
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
    	ArrayList<Text> alist = new ArrayList<Text>();
    	
    	for(Text V:values) 
    	{
    		Text wr = new Text();
    		wr.set(V);
    		alist.add(wr);
    		System.out.println("In First itreation:"+V.toString());
    	}
    	
    	int count = alist.size();
    	
    	for(int i=0;i<count-1;i++) {
    		for(int j=i+1;j<count;j++){
    			
    			String[] movie1 = alist.get(i).toString().split("::");		/// contains name of the movie and rating 
			String[] movie2 = alist.get(j).toString().split("::");
			
			MovieTextPairs m = new MovieTextPairs();
			m.setMovie1(movie1[0]);
			m.setMovie2(movie2[0]);
			//MovieTextPairs ascMovies = m.ascendingOder(m);
			
			RatingIntPairs r = new RatingIntPairs();
			r.setRating1(Integer.parseInt(movie1[1]));
			r.setReting2(Integer.parseInt(movie2[1]));
			
			BothPairs b = new BothPairs(m,r);
			BothPairs bFinal = b.ascendingOder(b);
			
			
    		System.out.println("In second Iterarion: "+ bFinal);
    		context.write(null, bFinal);
    		}
    	}
    	
    	
    	
    		
    	System.out.println(" ");
    }
  }
}

