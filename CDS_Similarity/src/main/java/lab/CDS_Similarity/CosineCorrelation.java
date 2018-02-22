package lab.CDS_Similarity;

import Jama.Matrix;

public class CosineCorrelation {

	  protected static double computeSimilarity(int[] r1, int[] r2) {
	    	  
		  double dotProduct = 0, normal1 = 0, normal2 = 0, eucledianDist = 0;
          for (int i=0;i<r1.length;i++) dotProduct += r1[i] * r2[i];
          for (int k : r1) normal1 += k * k;
          for (int k : r2) normal2 += k * k;
          eucledianDist = Math.sqrt(normal1) * Math.sqrt(normal2);
          return dotProduct / eucledianDist;
		
	  }
	}