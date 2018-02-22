package lab.CDS_PairingMovies;

public class BothPairs {

	MovieTextPairs m;
	RatingIntPairs r;
	
	
	public BothPairs(MovieTextPairs m, RatingIntPairs r) {
		super();
		this.m = m;
		this.r = r;
	}
	public MovieTextPairs getM() {
		return m;
	}
	public void setM(MovieTextPairs m) {
		this.m = m;
	}
	public RatingIntPairs getR() {
		return r;
	}
	public void setR(RatingIntPairs r) {
		this.r = r;
	}
	@Override
	public String toString() {
		return m+" && "+r;
	}
	
	public int compareTo(BothPairs o) {
		// TODO Auto-generated method stub
		 MovieTextPairs mp = o.getM();
		 String m1 = mp.getMovie1();
		 String m2 = mp.getMovie2();
		 
		 int compare =		m1.compareTo(m2);
		return compare;
	}
	
	public BothPairs ascendingOder(BothPairs o) {
		
		int value = compareTo(o);
		if(value <0) {
			return o;
		}else if(value >0) {
			String m1 = o.getM().getMovie1();
			o.getM().setMovie1(o.getM().getMovie2());
			o.getM().setMovie2(m1);
			
			int r1 = o.getR().getRating1();
			o.getR().setRating1(o.getR().getReting2());
			o.getR().setReting2(r1);
		}
		return o;
	}
	
	
	
}
