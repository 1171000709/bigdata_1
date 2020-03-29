package lgs;

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wc {
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**

    */  
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            context.write(new Text(itr.nextToken()), new IntWritable(1));
        }
    }
}

public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
  
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
     /**
     * Configuration��map/reduce��j�����࣬��hadoop�������map-reduceִ�еĹ���
     */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "wc"); //����һ���û������job����
    job.setJarByClass(wc.class);
    job.setMapperClass(TokenizerMapper.class); //Ϊjob����Mapper��
    job.setCombinerClass(IntSumReducer.class);    //Ϊjob����Combiner��
    job.setReducerClass(IntSumReducer.class); //Ϊjob����Reducer��
    job.setOutputKeyClass(Text.class);        //Ϊjob�������������Key��
    job.setOutputValueClass(IntWritable.class);    //Ϊjob�������value��
    
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/in/a.txt"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/in/out"));

    System.exit(job.waitForCompletion(true) ?0 : 1);        //����job
}

}
