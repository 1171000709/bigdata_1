package one;

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

public class one {
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**

    */  
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
            String tep=value.toString();
            String[] k=tep.split("\\|");
            context.write(new Text(k[10]), value);
    }
}

public static class IntSumReducer extends Reducer<Text, Text, NullWritable, Text> {
    /**

     */
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        ArrayList<String> ids = new ArrayList<String>();  
        ArrayList<String> sample = new ArrayList<String>();
        for (Text val : values) {
            String temp= val.toString();
            sum++;
            ids.add(temp); 
        }
        
        int k=Math.max(1,(int) (sum*0.2));
        //int k=2;
        for (int i=1;i<=sum;i++) {
          if (sample.size()<k)
            sample.add(ids.get(i-1));
          else {
            Random random = new Random();
            int j = random.nextInt(k);
 //           if (j < k) {
            sample.set(j,ids.get(i-1));
   //         }
          }
        }
        String str= String.join("\n",sample);
        context.write(NullWritable.get(), new Text(str));
    }
}

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
     /**
     * 
     */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "one"); //
    job.setJarByClass(one.class);
    job.setMapperClass(TokenizerMapper.class); //
   // job.setCombinerClass(IntSumReducer.class);    //
    job.setReducerClass(IntSumReducer.class); //
    job.setOutputKeyClass(Text.class);        //
    job.setOutputValueClass(Text.class);    //
    
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/in/data.txt"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/D_Sample"));

    System.exit(job.waitForCompletion(true) ?0 : 1);       
}

}
