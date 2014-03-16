import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CensusIncomeReducer extends MapReduceBase
    implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	public void reduce(Text arg0, Iterator<FloatWritable> arg1,
			OutputCollector<Text, FloatWritable> arg2, Reporter arg3)
			throws IOException {

		float sum = 0;
		float prob = 0;
	    while (arg1.hasNext()) {
	      FloatWritable value = arg1.next();
	      sum += value.get();
	    }
	    if(arg0.toString().startsWith("<=50K"))
	    { prob = (float)((float)sum/(float)23068) ;}
	    else
	    {prob =  (float)((float)sum/(float)7650); }
	    arg2.collect(arg0, new FloatWritable((float)(sum)));
	}



public static void writeToFile(String text) {
	/*FileSystem fs= FileSystem.get(configuration);
	FSDataOutputStream out=null;
	try{
	fs = FileSystem.get(URI.create("/user/hduser/intermediate/intermediate"+passedindex+".txt"), configuration);
	out = fs.create(new Path("/user/hduser/intermediate/intermediate"+passedindex+".txt"),true);
//	FSDataOutputStream out = fs.create(new Path("/user/hduser/intermediate/intermediate"+passedindex+".txt"),true);
	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
	bw.write(text+"\n");

	bw.close();
	out.close();
	fs.close();
	}
	catch(Exception e){
		System.out.println(e.getMessage());
	}*/
	Configuration conf = new Configuration();
	conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	
	try{
		Path path = new Path("user/hduser/intermediate/intermediate"+passedindex+".txt");
		FileSystem fileSystem = FileSystem.get(conf);
		if(!fileSystem.exists(path)){
			System.out.println("File doesnt exist");
		}
		FSDataOutputStream out = fileSystem.create(path);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		bw.write(text);
		
		out.close();
		bw.close();
		fileSystem.close();
		
	}catch(Exception e){
		System.out.println(e.getMessage());
	}
	
	
}

}



}