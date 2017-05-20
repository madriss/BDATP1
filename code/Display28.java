package ecp.bigdata.Tutorial1;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Display28 {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("isd-history.txt");
		
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
				nb_lignes++;
				if (nb_lignes >= 23){
					// Process of the current line
					if (line.length() > 0){
						String station = line.substring(13, 42);
						String fips = line.substring(43,45);
						String altitude = line.substring(74,81);
						System.out.println("station : " + station + " FIPS : " + fips + " Altitude : " + altitude);
						// go to the next line
						line = br.readLine();
						continue;
					}
					line = br.readLine();
					continue;
				}
				line = br.readLine();
				continue;
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
