package ecp.bigdata.Tutorial1;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.util.Arrays;


public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("arbres.csv");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		int nb_lignes = 0;
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);		
			// read line by line
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				String[] words = line.split(";");
				System.out.println(words[5] + " " + words[6]);
				// go to the next line
				line = br.readLine();
				nb_lignes++;
			}
		}
		finally{
			//close the file
			System.out.println("The number of lines is " + nb_lignes);
			inStream.close();
			fs.close();
		}
	}
}
