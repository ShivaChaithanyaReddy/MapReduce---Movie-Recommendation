package lab.CDS_MultipleMovie;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.util.*;

public class Multi extends Configured implements Tool {

	public static ArrayList<String> arrayL = new ArrayList<String>();
	 static StringBuffer temp;
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Multi(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	  
//check if the required number of arguments are provided.	  
/*	  if (args.length < 2) {
			System.err.printf("Usage: %s needs minimum three arguments, input and output directories\n", getClass().getSimpleName());
			return -1;
		}
	
	    */
	  int length = args.length;

	//create a new Job, set the name of the job and the main class.    
	    Configuration conf = getConf();
	    Job job = new Job(conf, this.getClass().toString());
	    job.setJobName("WordCount");
	    job.setJarByClass(Multi.class);

	    
	  
/*    Path inputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/testing/sample.dat");
    Path outputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output");*/
	    
	    
	    Path inputPath = new Path("/home/cloudera/wc_in_out/shiva_output4");
	    Path outputPath = new Path("/home/cloudera/wc_in_out/shiva_output5");

	    
    FileSystem fs = FileSystem.get(new Configuration());
    // true stands for recursively deleting the folder you gave
/*    fs.delete(new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output1"), true);
*/
    
    fs.delete(new Path("/home/cloudera/wc_in_out/shiva_output5"), true);
    
  temp = new StringBuffer();
    
    for(int i=0;i<length;i++) {
    	if(args[i].equals("-m")) {
    		arrayL.add(args[i+1]);
    		temp.append("##"+args[i+1]);
    		i++;
    	}
    }
    
    
//set the input and the output paths from the arguments.   
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

 
//    set the key value type classes and the output format class. These classes need to be the same type whichw e use in the map and reduce for the output.
  
    job.setOutputFormatClass(TextOutputFormat.class);
 //   job.setOutputKeyClass(Text.class);
    
    
    
    
  //  job.setSortComparatorClass(SortDoubleComparator.class);

    
    
    
    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);
    
    
    
      Configuration def = new Configuration();
	  ChainMapper.addMapper(job, JoinBMapper.class, LongWritable.class, Text.class, Text.class, Text.class, def);



	  
  //  job.setMapperClass(JoinBMapper.class);
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

	    	String[] line = value.toString().split(" && ");
	    	String MoviePair = line[0].substring(1, line[0].length()-1);
	    	String StatCor = line[2];
	    	String Cor = line[1];
	    	String CosCor = line[3];
	    	
	    	int number = Integer.parseInt(line[4]);
	    	
	    	String[] movies = MoviePair.split(",");
	    	
	    	String m1 = movies[0];
	    	String m2 = movies[1];
	    	
		//	System.out.println(MovieName+" "+m1);

			
	    	String[] AllMovies = temp.toString().split("##");
	    	
	    	for(String ss: AllMovies) {
	    		if(ss.equals(m1))
	    			context.write(new Text(ss)/*new DoubleWritable(Double.parseDouble(Cor))*/, new Text(ss+" ["+m2+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"));
	    		else if(ss.equals(m2))
	    			context.write(new Text(ss)/*new DoubleWritable(Double.parseDouble(Cor))*/, new Text(ss+" ["+m1+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"));
	    	}
	    	
	    }
	  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
    	for(Text value: values)
    	{
        	context.write(null, value);

    		
    	}
    }
  }

  

  public static class SortDoubleComparator extends WritableComparator {

		//Constructor.
		 
		protected SortDoubleComparator() {
			super(DoubleWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable k1 = (DoubleWritable)w1;
			DoubleWritable k2 = (DoubleWritable)w2;
			
			return -1 * k1.compareTo(k2);
		}
	}
  
}


