package three;

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

public class three {
  private static double max = 0;
  private static double min = 105;
  private static int num = 0;
    
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
    
    */  


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
          String tep=value.toString();
          String temp[]=tep.split("\\|");
  //        System.out.println(temp[10]);
          
          if (temp[6].contains("?")) {
             //System.out.println(tep);
           }else {
              double cs=Double.valueOf(temp[6]);
              max=Math.max(max, cs);
              min=Math.min(min, cs);
            }
          context.write(new Text(temp[10]), value);
     // context.write(new Text("a"),new Text("a"));
    }
    
}

public static class IntSumReducer extends Reducer<Text, Text, NullWritable, Text> {

  
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        
      //归一化  
        for (Text val : values) {
            String tep= val.toString();
            String temp[]=tep.split("\\|");
            try {
              temp[4]=normal(temp[4]);
            } catch (ParseException e) {
              e.printStackTrace();
            }
            try {
              temp[8]=normal(temp[8]);
            } catch (ParseException e) {
              e.printStackTrace();
            }
            
            
            temp[5]=normal_temp(temp[5]);
            if ((temp[6].contains("?")==false))temp[6]=normal2(temp[6]);
            String ok=String.join("|",temp);
            context.write(NullWritable.get(), new Text(ok));
        }
    //  context.write(NullWritable.get(), new Text("a"));
    }

    private String normal_temp(String temp) {
      if (temp.contains("℃")){
       // System.out.print("###");
        int z=temp.indexOf("℃");
        String now=temp.substring(0,z);
        double o=Double.valueOf(now);
        double o2=(o*9/5)+32;
        String ok=""+String.format("%.2f",o2)+"℉";
        return ok;
      }else return temp;
    }

    private String normal2(String w) {
      double ans=(Double.valueOf(w)-min)/(max-min);
      String s=String.format("%.2f",ans);
      return s;
    }

    private String normal(String w) throws ParseException {
      String now="";
      if (w.charAt(0)>='A'&&w.charAt(0)<='Z') {
        SimpleDateFormat sdf = new SimpleDateFormat("MMMM dd,yyyy", Locale.ENGLISH);
        Date date2 = sdf.parse(w);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        now=simpleDateFormat.format(date2);
      }else
      if (w.charAt(4)=='/') {
        now=w.replaceAll("/", "-");
      }else now=w;
      return now;
    }
}

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
     /**
     * 
     */
   
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "three"); //
    job.setJarByClass(three.class);
    job.setMapperClass(TokenizerMapper.class); //
   // job.setCombinerClass(IntSumReducer.class);    //
    
    job.setReducerClass(IntSumReducer.class); //
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);        //
    job.setOutputValueClass(Text.class);    //

    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/D_Filter"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/D_Filter2"));

    System.exit(job.waitForCompletion(true) ?0 : 1);     
   /* System.out.println(num);
    System.out.println(max);
    System.out.println(min);*/
}

}
