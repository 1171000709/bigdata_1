package two;

import java.io.IOException;
import java.util.ArrayList;
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

public class two {
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**

    */  
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
            String[] tem=value.toString().split("\\|");
            context.write(new Text(tem[10]), value);
    }
}

public static class IntSumReducer extends Reducer<Text, Text, NullWritable, Text> {
    /**

     */
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String temp= val.toString();
            int ci=0;boolean is=true;
            for (String t:temp.split("\\|")) {
              ci++;
              if (ci==2) {
                double longitude=Double.valueOf(t);
                if (longitude<8.1461259|| longitude> 11.1993265) {
                      is=false;break;
                }
              }else if (ci==3) {
                 double  latitude=Double.valueOf(t);
                 if ( latitude<56.5824856||  latitude> 57.750511) {
                        is=false;break;
                }
              }else if (ci>3)break;
            }
            if (is==true)context.write(NullWritable.get(), new Text(temp));
        }
       }
}


public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
     /**
     * 
     */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "two"); //
    job.setJarByClass(two.class);
    job.setMapperClass(TokenizerMapper.class); //
   // job.setCombinerClass(IntSumReducer.class);    //
    job.setReducerClass(IntSumReducer.class); //
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);        //
    job.setOutputValueClass(Text.class);    //
    
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/D_Sample"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/D_Filter"));

    System.exit(job.waitForCompletion(true) ?0 : 1);       
}

}
