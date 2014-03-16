import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CensusIncomeMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, FloatWritable> {
	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, FloatWritable> arg2, Reporter arg3)
			throws IOException {
		String attr[]=(arg1.toString()).split(" ");
		
		String classifier=attr[6];
		for( int i=0;i<6;i++)
		{
			arg2.collect(new Text(classifier+"_"+attr[i]), new FloatWritable(Float.valueOf(1)));
		}
	}
}