package lab.CDS_Final;



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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;

import Jama.Matrix;

public class Final extends Configured implements Tool {
	
	public static ArrayList<String> arrayL = new ArrayList<>();
	public static int cc = 0;
	
	
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Final(), args);
    System.exit(res);
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
  
  

  public int run(String[] args) throws Exception {
	  
	  int length = args.length;
	  

	  
//check if the required number of arguments are provided.	  
	  if (args.length < 2) {
			System.err.printf("Usage: %s needs minimum two arguments, input and output directories\n", getClass().getSimpleName());
			return -1;
		}
	
	    
	//create a new Job, set the name of the job and the main class.    
	    Configuration conf = getConf();
	    Job job = new Job(conf, this.getClass().toString());
	    job.setJobName("WordCount");
	    job.setJarByClass(Final.class);

	    
	  
/*    Path inputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/output");
    Path outputPath = new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output1");
*/
	    
  
	    Path inputPath = new Path("/home/cloudera/wc_in_out/shiva_output4");
	    Path outputPath = new Path("/home/cloudera/wc_in_out/shiva_output5");

	    
    FileSystem fs = FileSystem.get(new Configuration());
    // true stands for recursively deleting the folder you gave
/*    fs.delete(new Path("/home/cloudera/wc_in_out/CDS_Project/sorting/output1"), true);
*/
    
    fs.delete(new Path("/home/cloudera/wc_in_out/shiva_output5"), true);
    
    
//set the input and the output paths from the arguments.   
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

 
//    set the key value type classes and the output format class. These classes need to be the same type whichw e use in the map and reduce for the output.
  
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    
    job.setSortComparatorClass(SortDoubleComparator.class);
    
    StringBuffer temp = new StringBuffer();
    
    for(int i=0;i<length;i++) {
    	if(args[i].equals("-m")) {
    		arrayL.add(args[i+1]);
    		temp.append("##"+args[i+1]);
    	}
    }
    
    boolean tt = false;
    
   // job.setMapperClass(JoinBMapper.class);
  //  job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

    
    for(int i=1;i<length;i++) {
		  switch(args[i]) {
		  case "-m":
//			  if(tt == true)
//				  break;
//			  else if(arrayL.size() ==1) {
				  Configuration splitMapConfig = new Configuration();
				  i++;
				  splitMapConfig.set("Movie", args[i]);
				  ChainMapper.addMapper(job, MovieMapper.class, DoubleWritable.class, Text.class, DoubleWritable.class, Text.class, splitMapConfig);
//			  }
/*			  else {
				 // job = new Job(conf, this.getClass().toString());
				  tt = true;
				  Configuration splitMapConfig = new Configuration();
				  i = length-1;
				  splitMapConfig.set("NextMovie", temp.toString());
				  ChainMapper.addMapper(job, MultipleMovieMapper.class, DoubleWritable.class, Text.class, DoubleWritable.class, Text.class, splitMapConfig);
				  
				  Configuration splitMapConfig1 = new Configuration();
				  splitMapConfig1.set("NextMovie", temp.toString());
				  ChainMapper.addMapper(job, makeOrder.class, DoubleWritable.class, Text.class, DoubleWritable.class, Text.class, splitMapConfig1);
				  
			  }*/
			  break;
		  case "-k":
			  cc =0;
			  Configuration numberOfItems = new Configuration();
			  i++;
			  numberOfItems.set("number", args[i]);
			  ChainMapper.addMapper(job, numberMapper.class, DoubleWritable.class, Text.class, DoubleWritable.class, Text.class, numberOfItems);
			  break;
		  case "-l":
			  Configuration lowerBound = new Configuration();
			  i++;
			  lowerBound.set("lower", args[i]);
			  ChainMapper.addMapper(job, BoundMapper.class, DoubleWritable.class, Text.class, DoubleWritable.class, Text.class, lowerBound);
			  break;
		  case "-p":
			  Configuration minRating = new Configuration();
			  i++;
			  minRating.set("min", args[i]);
			  ChainMapper.addMapper(job, MinRatingMapper.class, DoubleWritable.class, Text.class, DoubleWritable.class, Text.class, minRating);
			  break;
		  default:
			  Configuration def = new Configuration();
			  ChainMapper.addMapper(job, defaultMapper.class, LongWritable.class, Text.class, DoubleWritable.class, Text.class, def);


			  /*			  throw new IllegalArgumentException("Invalid argument specified " + args[i]);
*/		  }
		  
	  }
    
    

//execute the job and wait for its completion
    int returnValue = job.waitForCompletion(true) ? 0 : 1;
    
    if(job.isSuccessful()) {
    	
    		            System.out.println("Job was successful");
    	
    		        } else if(!job.isSuccessful()) {
    	
    		            System.out.println("Job was not successful");          
    	
    		        }
    return returnValue; 
  }

  
  
  
  
  public static class defaultMapper extends
  Mapper<LongWritable, Text, DoubleWritable, Text> {
	  
    @Override
    public void map(LongWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {

    	String[] line = value.toString().split(" && ");
    	String Cor = line[1];
    	
		  context.write(new DoubleWritable(Double.parseDouble(Cor)), value);
    	
    }
  }
  
  
  public static class MovieMapper extends
  Mapper<DoubleWritable, Text, DoubleWritable, Text> {
	  
    @Override
    public void map(DoubleWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    	
    	Configuration co = context.getConfiguration();
    	String MovieName = co.get("Movie");
    	
    	
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

		
    	if(MovieName.equals(m1)) {
    		context.write(new DoubleWritable(Double.parseDouble(Cor)), new Text(MovieName+" ["+m2+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"));
    	System.out.println(MovieName+" ["+m2+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"+"     "+new DoubleWritable(Double.parseDouble(Cor)));
    	}
    	else if(MovieName.equals(m2)) {
    		context.write(new DoubleWritable(Double.parseDouble(Cor)), new Text(MovieName+" ["+m1+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"));
        	System.out.println(MovieName+" ["+m1+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"+"    "+new DoubleWritable(Double.parseDouble(Cor)));

    	}
    	System.out.println(" ");
    	
    }
  }
  
  
  public static class numberMapper extends
  Mapper<DoubleWritable, Text, DoubleWritable, Text> {
	  
    @Override
    public void map(DoubleWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    	cc++;
    	Configuration co = context.getConfiguration();
    	int numberItems = Integer.parseInt(co.get("number"));
    	
    	//System.out.println("asdfcgvbfdsfghdsdfghgfdsasdfghj"+value);
    	if(cc <= numberItems)
		  context.write(key, value);
    		//System.out.println("value in number mapper is: "+value);
    	
    }
  }
  
  public static class BoundMapper extends
  Mapper<DoubleWritable, Text, DoubleWritable, Text> {
	  
    @Override
    public void map(DoubleWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    	
    	Configuration co = context.getConfiguration();
    	double lowerBound = Double.parseDouble(co.get("lower"));
    	
    	if(Double.parseDouble(value.toString().split(",")[1]) > lowerBound)
		  context.write(key, value);
    	
    }
  }
  
  public static class MinRatingMapper extends
  Mapper<DoubleWritable, Text, DoubleWritable, Text> {
	  
    @Override
    public void map(DoubleWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    	
    	Configuration co = context.getConfiguration();
    	int numberItems = Integer.parseInt(co.get("min"));
		 
    	
    	if(Integer.parseInt(value.toString().split(", ")[4].substring(0, 1)) > numberItems )
    			context.write(key, value);
    		//System.out.println("number is: "+numberItems);
    	
    }
  }
  
  

  public static class MultipleMovieMapper extends
  Mapper<DoubleWritable, Text, DoubleWritable, Text> {
	  
    @Override
    public void map(DoubleWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {
    	
    	Configuration co = context.getConfiguration();
    	String MovieName1 = co.get("NextMovie");
    	
    	System.out.println("Complete String is: "+value.toString());
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

		
    	String[] AllMovies = MovieName1.split("##");
    	
    	for(String ss: AllMovies) {
    		if(ss.equals(m1))
    			context.write(new DoubleWritable(Double.parseDouble(Cor)), new Text(ss+" ["+m2+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"));
    		else if(ss.equals(m2))
    			context.write(new DoubleWritable(Double.parseDouble(Cor)), new Text(ss+" ["+m1+", "+Cor+", "+StatCor+", "+CosCor+", "+number+"]"));
    	}
    	
    }
  }
  
  

  public static class makeOrder extends
  Mapper<LongWritable, Text, DoubleWritable, Text> {
	  
    @Override
    public void map(LongWritable key, Text value,
    		Mapper.Context context) throws IOException, InterruptedException {

    	String[] line = value.toString().split(" && ");
    	String Cor = line[1];
    	
		  context.write(new DoubleWritable(Double.parseDouble(Cor)), value);
    	
    }
  }
  
  
  

  public static class Reduce extends Reducer<DoubleWritable, Text, Text, Text> {
	
    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
    	for(Text value: values)
    	context.write(null, value);
    }
  }
}

