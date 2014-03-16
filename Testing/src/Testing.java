import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Testing {
	
	
	public static void readFile(String file) throws IOException {
	        Configuration conf = new Configuration();
	        conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	        //conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

	        FileSystem fileSystem = FileSystem.get(conf);
	        Enumeration names;
	        String str;
	        Path path = new Path(file);
	        Path path1 = new Path("/user/hduser/testing/finaltestdata_wo_c.data");
	        Path path3 = new Path("/user/hduser/testing/finaltestdata_w_c.data");
	        
	        
	        if (!fileSystem.exists(path)) {
	            System.out.println("File input file part-00000 " + file + " does not exists");
	            return;
	        }
	        
	        if (!fileSystem.exists(path1)) {
	            System.out.println("File finaltestdata_wo_c.data " + file + " does not exists");
	            return;
	        }
	        
	        if (!fileSystem.exists(path3)) {
	            System.out.println("File finaltestdata_w_c.data " + file + " does not exists");
	            return;
	        }
	        
	        
	        
	        Path path2 = new Path("/user/hduser/result/result.data");
	        if (fileSystem.exists(path2)) {
	            System.out.println("File /user/hduser/result/result.data already exists");
	            return;
	        }
	     // Create a new file and write data to it.
	        FSDataOutputStream out = fileSystem.create(path2);
	        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(out));
	        

	        FSDataInputStream in = fileSystem.open(path);
	        BufferedReader d = new BufferedReader(new InputStreamReader(in));

	        FSDataInputStream in1 = fileSystem.open(path1);
	        BufferedReader d1 = new BufferedReader(new InputStreamReader(in1));
	        
	        FSDataInputStream in3 = fileSystem.open(path3);
	        BufferedReader d3 = new BufferedReader(new InputStreamReader(in3));
	        
	        
	        //System.out.println("************************ buffered readers created *************************");
	        
	             
	        System.out.println();
	        //System.out.println("************* populating and creating hash table **********************");
	        Hashtable data = new Hashtable();
	        float fact11 = 23068;
	        float fact22 = 7650;
	        while(true)
	        {
	        	String line = null;
				
				line = d.readLine();
			
				if (line == null) 
				{
					break;
				}
				 
				//System.out.println(line);
				float conv = 0;
				float attr1 = 0;
				String[] fields = line.split("\\t");
				//System.out.println("************************ read value 1  "+fields[0]);
				//System.out.println("************************ read value 1  "+fields[1]);
				if(fields[0].startsWith("<=50K_"))
				{	//System.out.println("**************** <=50K  "+fields[0] +" *********************");
					attr1 = Float.valueOf(fields[1]);
					//System.out.println("**************** "+ attr1 +" *********************");
					conv = (float)(attr1/fact11);
					//System.out.println("**************** "+ conv +" *********************");
				}
				else
				{
					//System.out.println("**************** >50K  "+fields[0] +" *********************");
					attr1 = Float.valueOf(fields[1]);
					//System.out.println("**************** "+ attr1 +" *********************");
					conv = (float)(attr1/fact22);
					//System.out.println("**************** "+ conv +" *********************");
				}
				
				data.put(fields[0],(float)conv);
				
				
	        }
	        
	        //System.out.println("********************** Displaying content of hash table ***********************");
	        
	      /*  names = data.keys();
	        while(names.hasMoreElements()) {
	           str = (String) names.nextElement();
	           System.out.println(str + ": " +
	           data.get(str));
	        }*/
	        
	        System.out.println();
	        System.out.println("*******************************************************");
	        System.out.println("Entering to read test data");
	        System.out.println("*******************************************************");
	        int poscount=0;
	        int negcount=0;
	        int total = 0;
	        
	        while(true)
	        {
	        	String line1 = null;
	        	String line3 = null;
	        	String compare = null;
				
				line1 = d1.readLine();
				line3 = d3.readLine();
				if (line1 == null || line1 == null) 
				{
					break;
				}
				System.out.println("*Record to classify : "+ line1+" *");
				//float attr11=0,attr12=0,attr21=0,attr22=0,attr31=0,attr32=0,attr41=0,attr42=0,attr51=0,attr52=0;
				float ans1=0,ans2=0;
				float fans1, fans2;
				float fact1 = ((float)7650/(float)30718);
				float fact2 = ((float)23068/(float)30718);
				String cl = "";
				//float asd[][];
				float[][] asd = new float[3][7];
				//System.out.println(line);
				String[] fields1 = line1.split(" ");
				String[] fields3 = line3.split(" ");
				compare = fields3[6].trim();
				//System.out.println(fields1[5]+"********************");
				//System.out.println("************** Entering for for calculating probability ****************");
				for(int i=0;i<fields1.length;i++)
				{
					//System.out.println("************** in loop for getting data from hash table ****************");
					//for >50K
					try{
					
					if(i==5)
					{
						asd[0][i] = (Float) data.get(">50K_"+fields1[i].trim());
					}
					else
					{asd[0][i] = (Float) data.get(">50K_"+fields1[i].trim());}
					}
					catch(Exception ex)
					{System.out.println(ex.toString());}
					// for <=50K
					//System.out.println(asd[0][i]);
					asd[1][i] = (Float) data.get("<=50K_"+fields1[i].trim());
					//System.out.println(asd[1][i]);				
				}
				//System.out.println("************** loop exit ****************");
				ans1 = asd[0][0]*asd[0][1]*asd[0][2]*asd[0][3]*asd[0][4]*asd[0][5]; 
				ans2 = asd[1][0]*asd[1][1]*asd[1][2]*asd[1][3]*asd[1][4]*asd[1][5];
				fans1 = fact1*ans1;
				fans2 = fact2*ans2;
				System.out.println("************** calculating final class label >50K --> "+fans1 +"and <=50K --> "+fans2 +" *************");
				if(fans1 >= fans2 )
				{cl=">50K";}
				else {cl="<=50K";}
				if(compare.compareTo(cl) == 0)
				{
					poscount++;
					total++;
				}
				else
				{
					negcount++;
					total++;
				}
				
				
				System.out.println("******************* class label decided as "+cl+"*********************");
				//System.out.println("********************* writing data in a file *******************");
				try{
				for(int i=0;i<fields1.length;i++)
				{
					bw.write(fields1[i]);
					bw.write(" ");
				
				}
				bw.write(cl);
				bw.write("\n");
				}
				catch(Exception ece)
				{System.out.println(ece.toString());}
				//System.out.println("************************ Writing done ****************************");
				//System.out.println(cl);
				System.out.println("----------------------------------------------------------------");
	        }
	        System.out.println("************************ Writing Data to file Done ****************************");
	        System.out.println("**********DONE**************DONE****************DONE****************DONE");
	        
	        System.out.println("************************ Accuracy ****************************");
	        System.out.println("Positive Classification :"+poscount );
	        System.out.println("Negative Classification :"+negcount );
	        System.out.println("Total Classification    :"+total );
	        float accu = ((float)((float)poscount/(float)total))*100;
	        System.out.println("Accuracy                :"+accu+" %" );
	        
	        System.out.println("******************* Classification Successful *************************");
	       /* byte[] b = new byte[1024];
	        int numBytes = 0;
	        while ((numBytes = in.read(b)) > 0) {
	            out.write(b, 0, numBytes);
	        }*/

	        in.close();
	        d.close();
	        out.close();
	        bw.close();
	        fileSystem.close();
	    }
	
	
	
	
	public static void main (String args[])
	{
		try
		{
			
			readFile("/user/hduser/dipam-output1/part-00000");
			
		}
		catch(Exception ex)
		{
			
		}
		
	}

}
