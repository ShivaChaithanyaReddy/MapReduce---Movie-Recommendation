package lab.CDS_PairingMovies;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RatingIntPairs  implements Writable{
	
	private int rating1;
	private int rating2;
	
	
	@Override
	public String toString() {
		return "("+rating1+","+rating2+")";

	}

	public int getRating1() {
		return rating1;
	}

	public void setRating1(int rating1) {
		this.rating1 = rating1;
	}

	public int getReting2() {
		return rating2;
	}

	public void setReting2(int reting2) {
		this.rating2 = reting2;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		rating1 = in.readInt();
		rating2 = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(rating1);
		out.writeInt(rating2);
	}

}
