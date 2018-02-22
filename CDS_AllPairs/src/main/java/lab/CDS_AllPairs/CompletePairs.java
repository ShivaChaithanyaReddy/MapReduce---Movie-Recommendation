package lab.CDS_AllPairs;

import org.apache.hadoop.io.Text;

public class CompletePairs {

	

	Text moviePair;
	String AllRatingPairs;
	
	
	
	
	public CompletePairs(Text moviePair, String allRatingPairs) {
		super();
		this.moviePair = moviePair;
		AllRatingPairs = allRatingPairs;
	}
	@Override
	public String toString() {
		return "("+moviePair+") &&& "+AllRatingPairs;

	}
	public Text getMoviePair() {
		return moviePair;
	}
	public void setMoviePair(Text moviePair) {
		this.moviePair = moviePair;
	}
	public String getAllRatingPairs() {
		return AllRatingPairs;
	}
	public void setAllRatingPairs(String allRatingPairs) {
		AllRatingPairs = allRatingPairs;
	}
	
	

	
}
