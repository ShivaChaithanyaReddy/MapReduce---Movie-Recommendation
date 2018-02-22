package lab.CDS_Similarity;




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

import Jama.Matrix;

public class Similarity extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Similarity(), args);
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
	    job.setJarByClass(Similarity.class);

	    
	  
/*    Path inputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/output");
    Path outputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output1");
*/
	    
	    Path inputPath = new Path("/home/cloudera/wc_in_out/shiva_output3");
	    Path outputPath = new Path("/home/cloudera/wc_in_out/shiva_output4");

	    
    FileSystem fs = FileSystem.get(new Configuration());
    // true stands for recursively deleting the folder you gave
/*    fs.delete(new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output1"), true);
*/
    
    fs.delete(new Path("/home/cloudera/wc_in_out/shiva_output4"), true);

    
//set the input and the output paths from the arguments.   
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

 
//    set the key value type classes and the output format class. These classes need to be the same type whichw e use in the map and reduce for the output.
  
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
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
    	
    	String[] as = value.toString().split(" &&& ");
    	
    	
    	as[0] = as[0].substring(1, as[0].length()-1);
    	//System.out.println(as[0]+"  Value is  "+as[1]);
    	
    	context.write(new Text(as[0]), new Text(as[1]));
       
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
    	
    	ArrayList<Text> alist = new ArrayList<Text>();
    	int cc=0;
    	
    	for(Text v: values) {
    		Text t = new Text();
    		t.set(v);
    		alist.add(t);
    		
    		String[] ratingPair = v.toString().split(" , ");
    		cc = ratingPair.length;
        	}
    	
    	
    	
    	int[] rating1 = new int[cc];
    	int[] rating2 = new int[cc];
    	int count = 0;
    	
    	for(Text value: alist) {
    		String[] ratingPairs = value.toString().split(" , ");
    		
/*    		for(String rat : ratingPairs) {
    			
    			System.out.println(rat);
    	    			
    		}*/
    		
    		for(String pair: ratingPairs) {
    	        System.out.println("pair is: "+pair);

    			
    			int r1 = Integer.parseInt((String.valueOf(pair.split(",")[0].substring(1) )));
    			int r2 =  Integer.parseInt(String.valueOf(pair.split(",")[1].substring(0, pair.split(",")[1].length()-1)));
    			
    			rating1[count] = r1;
    			rating2[count] = r2;
    			
    			count++;
	
    			/*    			int r1 = Integer.parseInt(pair.substring(1, pair.length()-3));
    			int r2 = Integer.parseInt(pair.substring(3, pair.length()-1));
    			
    			System.out.println(r1+""+r2);

    			rating1[count] = r1;
    			rating2[count] = r2;
    			*/
    		}
    	}
/*    	for(int a: rating1){
        System.out.println("rat1 "+a);
    	}
    	
    	for(int a: rating2){
            System.out.println(a);
        	}
    	*/
    	
    	double stat = StatCorrelation.Correlation(rating1, rating2);
   
    /////cosine correlation:
    double cos = CosineCorrelation.computeSimilarity(rating1, rating2);
    
    if(!String.valueOf(stat).equals("NaN")) {

        double cor = 0.5 * (stat + cos);
        
        context.write(null,	new Text("("+key+")"+" && "+ String.valueOf(cor)+" && "+String.valueOf(stat)+" && "+ String.valueOf(cos)+" && "+count ));
      }
    }
  }
}

