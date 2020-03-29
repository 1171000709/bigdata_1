package all;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
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

public class all {
  private static  double max = Double.MIN_NORMAL;
  private static  double min = Double.MAX_VALUE;
  

    
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
    
    */  
    

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
          String tep=value.toString();
          String temp[]=tep.split("\\|");
          if (temp[6].contains("?")) {
            
          }else {
              double cs=Double.valueOf(temp[6]);
              max=Math.max(cs, max);
              min=Math.min(cs, min);
           //   System.out.println(max+" "+min);
          }
          context.write(new Text(temp[10]), value);
    }
  }

public static class IntSumReducer extends Reducer<Text, Text, NullWritable, Text> {

  
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
      //抽样
      int sum = 0;
      ArrayList<String> ids = new ArrayList<String>();
      ArrayList<String> sample = new ArrayList<String>();
      for (Text val : values) {
          String temp= val.toString();
          sum++;
          ids.add(temp);
      }
      
      int k=Math.max(1,(int) (sum*0.2));
      for (int i=1;i<=sum;i++) {
        if (sample.size()<k)
          sample.add(ids.get(i-1));
        else {
          Random random = new Random();
          int j = random.nextInt(k);
          sample.set(j,ids.get(i-1));
        }
      }
      
      //过滤
      Iterator<String> iter = sample.iterator();  
      while(iter.hasNext()){  
          String now = iter.next();  
          String[] temp=now.split("\\|");
          double longitude=Double.valueOf(temp[1]);
          if (longitude<8.1461259|| longitude> 11.1993265) {
                  iter.remove();
                  }
          else      {
                    double  latitude=Double.valueOf(temp[2]);
                    if ( latitude<56.5824856||  latitude> 57.750511) {
                           iter.remove();
                            }
                    }
      }
      
      
      //归一化
      for (String tep:sample) {
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
      String s=String.format("%.4f",ans);
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
    Job job = Job.getInstance(conf, "all"); //
    job.setJarByClass(all.class);
    job.setMapperClass(TokenizerMapper.class); //
   // job.setCombinerClass(IntSumReducer.class);    //
    job.setReducerClass(IntSumReducer.class); //
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);        //
    job.setOutputValueClass(Text.class);    //

    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/in/data.txt"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/allD_Filter"));

    System.exit(job.waitForCompletion(true) ?0 : 1);     
}

}
