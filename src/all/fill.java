package all;

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

public class fill {
  //private static int sum=1966;
  private static int nows=0;
  private static double minLongitude=Double.MAX_VALUE;
  private static double maxLongitude=Double.MIN_NORMAL;
  private static double minLatitude=Double.MAX_VALUE;
  private static double maxLatitude=Double.MIN_NORMAL;
  private static double minAltitude=Double.MAX_VALUE;
  private static double maxAltitude=Double.MIN_NORMAL;
  private static double minSalary=Double.MAX_VALUE;
  private static double maxSalary=Double.MIN_NORMAL;
  
  private static ArrayList<Double> lis1=new ArrayList<Double>();
  private static ArrayList<Double> lis2=new ArrayList<Double>();
  private static ArrayList<Double> lis3=new ArrayList<Double>();
  private static ArrayList<Double> lis6=new ArrayList<Double>();
  private static ArrayList<String> lis9=new ArrayList<String>();
  private static ArrayList<String> lis10=new ArrayList<String>();
  private static ArrayList<Double> lis11=new ArrayList<Double>();
    
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
    
    */  
    

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
          String[] tem=value.toString().split("\\|");
          if (tem[6].contains("?")||tem[11].contains("?")) {
            context.write(new Text(tem[10]), value);
          }else {
              ++nows;
              double a1=Double.valueOf(tem[1]);
              minLongitude=Math.min(minLongitude,a1);
              maxLongitude=Math.max(maxLongitude,a1);
              
              double a2=Double.valueOf(tem[2]);
              minLatitude=Math.min(minLatitude,a2);
              maxLatitude=Math.max(maxLatitude,a2);
              
              double a3=Double.valueOf(tem[3]);
              minAltitude=Math.min(minAltitude, a3);
              maxAltitude=Math.max(maxAltitude, a3);
              
              double a11=Double.valueOf(tem[11]);
              minSalary=Math.min(minSalary,a11);
              maxSalary=Math.max(maxSalary,a11);
              
              lis1.add(a1);
              lis2.add(a2);
              lis3.add(a3);
              lis6.add(Double.valueOf(tem[6]));
              lis9.add(tem[9]);
              lis10.add(tem[10]);
              lis11.add(a11);
              context.write(new Text("1"), value);
          }
    }
  }

public static class IntSumReducer extends Reducer<Text, Text, NullWritable, Text> {

  
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
      if (key.toString().equals("1")) {
          for (Text val : values) 
          context.write(NullWritable.get(),val);
      }
      else {
        for (Text val : values) {
          String o=val.toString();
          String[] temp=o.split("\\|");
         
          
          if (temp[11].contains("?")) {
          int mi=3,dis=0,cnt=0;
          double ans=0,sum=0;
          
          for (int i=0;i<nows;i++) {
            dis=get_hanmin(temp[9],temp[10],lis9.get(i),lis10.get(i));
            if (dis<mi){mi=dis;cnt=1;ans=lis11.get(i);sum=ans;}
            else if (dis==mi) {cnt++;ans=lis11.get(i);sum+=ans;}
          }
          //填充income 
          temp[11]=""+String.format("%.0f",sum/cnt);
         }
       
          
          if (temp[6].contains("?")) {
          double ma=Double.MAX_VALUE;
          double dis=0,ans=0,sum=0;
          int cnt=0;
          
          for (int i=0;i<nows;i++) {
            double a1=(lis1.get(i)-minLongitude)/(maxLongitude-minLongitude);
            double a2=(lis2.get(i)-minLatitude)/(maxLatitude-minLatitude);
            double a3=(lis3.get(i)-minAltitude)/(maxAltitude-minAltitude);
            double a11=(lis11.get(i)-minSalary)/(maxSalary-minSalary);
            
            dis=get_minkowski(temp[11],temp[1],temp[2],temp[3],a11,a1,a2,a3);
            if (dis<ma){ma=dis;ans=lis6.get(i);sum=ans;cnt=1;}
            else if (dis==ma) {cnt++;ans=lis6.get(i);sum+=ans;}
            }
          //填充rating
          temp[6]=""+String.format("%.2f",sum/cnt);
          }
          
          
          context.write(NullWritable.get(),new Text(String.join("|", temp)));
        }
       }
    }


    private double get_minkowski(String string, String string2, String string3, String string4,
        double y1, double y2, double y3, double y4) {
      double x1,x2,x3,x4,ss=0;
      x1=Double.valueOf(string);x2=Double.valueOf(string2); x3=Double.valueOf(string3);x4=Double.valueOf(string4);
      
      ss+=Math.abs(x1-y1)*1.0/9+Math.abs(x2-y2)*3.0/9+Math.abs(x3-y3)*3.0/9+Math.abs(x4-y4)*2.0/9;
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
    Job job = Job.getInstance(conf, "fill"); //
    job.setJarByClass(fill.class);
    job.setMapperClass(TokenizerMapper.class); //
   // job.setCombinerClass(IntSumReducer.class);    //
    job.setReducerClass(IntSumReducer.class); //
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);        //
    job.setOutputValueClass(Text.class);    //

    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/allD_Filter"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/allD_Done"));

    System.exit(job.waitForCompletion(true) ?0 : 1);     
}

}
