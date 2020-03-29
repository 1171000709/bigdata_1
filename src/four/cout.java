package four;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Random;

public class cout {
  private static int sum=0;
  private static int nows=0;
    
  public static class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    /**
    
    */  
    

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
          String[] tem=value.toString().split("\\|");
          if (tem[6].contains("?")||tem[11].contains("?")) {
            ++sum;
          }
          context.write(new IntWritable(sum), new Text("a"));
    }
  }

public static class IntSumReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

  
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
      context.write(NullWritable.get(), new Text(""+sum));
    }


    private double get_minkowski(String string, String string2, String string3, String string4,
        String string5, String string6, String string7, String string8) {
      double x1,x2,x3,x4,y1,y2,y3,y4,ss=0;
      x1=Double.valueOf(string);x2=Double.valueOf(string2); x3=Double.valueOf(string3);x4=Double.valueOf(string4);
      y1=Double.valueOf(string5);y2=Double.valueOf(string6); y3=Double.valueOf(string7);y4=Double.valueOf(string8);
      
      ss+=Math.abs(x1-y1)*1.0/9+Math.abs(x1-y1)*3.0/9+Math.abs(x1-y1)*3.0/9+Math.abs(x1-y1)*2.0/9;
      return ss;
    }

    private int get_hanmin(String string, String string2, String string3, String string4) {
      int cnt=0;
      if (string.equals(string3)==false)cnt++;
      if (string2.equals(string4)==false)cnt++;
      return cnt;
    }
  }

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
     /**
     * 
     */
   
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "cout"); //
    job.setJarByClass(cout.class);
    job.setMapperClass(TokenizerMapper.class); //
   // job.setCombinerClass(IntSumReducer.class);    //
    job.setReducerClass(IntSumReducer.class); //
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);        //
    job.setOutputValueClass(Text.class);    //

    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/D_Filter2"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/cout"));

    System.exit(job.waitForCompletion(true) ?0 : 1);     
}

}
