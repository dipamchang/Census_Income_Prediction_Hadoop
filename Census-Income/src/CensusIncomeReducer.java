import java.io.IOException;
import java.util.Iterator;
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
	    arg2.collect(arg0, new FloatWritable((float)(prob)));
	}
}