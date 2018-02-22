package lab.CDS_PairingMovies;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MovieTextPairs implements Writable {
	
	private String Movie1;
	private String Movie2;
	
	
	
	@Override
	public String toString() {
		return "("+Movie1+","+Movie2+")";
	}
	public String getMovie1() {
		return Movie1;
	}
	public void setMovie1(String movie1) {
		Movie1 = movie1;
	}
	public String getMovie2() {
		return Movie2;
	}
	public void setMovie2(String movie2) {
		Movie2 = movie2;
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Movie1 = in.readLine();
		Movie2 = in.readLine();
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeBytes(Movie1);
		out.writeBytes(Movie2);
	}
	public int compareTo(MovieTextPairs o) {
		// TODO Auto-generated method stub
		int compare = o.getMovie1().compareTo(o.getMovie2());
		return compare;
	}
	
	public MovieTextPairs ascendingOder(MovieTextPairs o) {
		
		int value = compareTo(o);
		if(value <0) {
			return o;
		}else if(value >0) {
			String m1 = o.getMovie1();
			o.setMovie1(o.getMovie2());
			o.setMovie2(m1);
		}
		return o;
	}


}
