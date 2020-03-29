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

public class four {
  private static int sum=1966;
  private static int nows=0;
  private static ArrayList<String> lis=new ArrayList<String>();
    
  public static class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    /**
    
    */  
    

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
          String[] tem=value.toString().split("\\|");
          if (tem[6].contains("?")||tem[11].contains("?")) {
            ++nows;
            if (nows<=sum)
            context.write(new IntWritable(nows), value);
          }else {
              lis.add(value.toString());
              context.write(new IntWritable(sum+1), value);
          }
    }
  }

public static class IntSumReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

  
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
      if (key.get()<sum+1) {
            //ArrayList<String> lis=new ArrayList<String>();
            String now="";
            for (Text val : values) {
              String o=val.toString();
              now=o;
            }
            

            String[] temp=now.split("\\|");
            double sva=0;int sci=0;
            if (temp[11].contains("?")) {
              int ma=3;int dis=0;
              
              for (String ok1:lis) {
                String[] z=ok1.split("\\|");
                dis=get_hanmin(temp[9],temp[10],z[9],z[10]);
                if (dis<ma)ma=dis;
                }
              for (String ok1:lis) {
                String[] z = ok1.split("\\|");
                dis=get_hanmin(temp[9],temp[10],z[9],z[10]);
                if (dis==ma) {sva+=Double.valueOf(z[11]);sci++;}
              }
              //填充income
              temp[11]=""+String.format("%.0f",sva/sci);
            }
           
            sva=0;sci=0;
            if (temp[6].contains("?")) {
              double ma=Double.MAX_VALUE;double dis=0;
              
              for (String ok1:lis) {
                String[] z=ok1.split("\\|");
                dis=get_minkowski(temp[11],temp[1],temp[2],temp[3],z[11],z[1],z[2],z[3]);
                if (dis<ma)ma=dis;
                }
              for (String ok1:lis) {
                String[] z = ok1.split("\\|");
                dis=get_minkowski(temp[11],temp[1],temp[2],temp[3],z[11],z[1],z[2],z[3]);
                if (dis==ma) {sva+=Double.valueOf(z[6]);sci++;}
              }
              //填充rating
              temp[6]=""+String.format("%.2f",sva/sci);;
            }
            context.write(NullWritable.get(),new Text(String.join("|", temp)));
            
      }
      else {
        for (Text val : values) 
          context.write(NullWritable.get(),val);
           
        }
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
    Job job = Job.getInstance(conf, "four"); //
    job.setJarByClass(four.class);
    job.setMapperClass(TokenizerMapper.class); //
   // job.setCombinerClass(IntSumReducer.class);    //
    job.setReducerClass(IntSumReducer.class); //
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);        //
    job.setOutputValueClass(Text.class);    //

    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/D_Filter2"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/D_Done"));

    System.exit(job.waitForCompletion(true) ?0 : 1);     
}

}
