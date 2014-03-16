import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class CensusIncome {

  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(CensusIncome.class);

    // specify output types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(FloatWritable.class);

    // specify input and output dirs
    //FileInputFormat.setInputPaths(conf, new Path(args[1]));
    //FileOutputFormat.setOutputPath(conf, new Path(args[2]));
    
    FileInputFormat.setInputPaths(conf, new Path("/user/hduser/dipam"));
    FileOutputFormat.setOutputPath(conf, new Path("/user/hduser/dipam-output1"));

    // specify a mapper
    conf.setMapperClass(CensusIncomeMapper.class);

    // specify a reducer
    conf.setReducerClass(CensusIncomeReducer.class);
    conf.setCombinerClass(CensusIncomeReducer.class);

    client.setConf(conf);
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}