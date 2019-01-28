import java.lang.Object;
import java.util.Arrays;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class q1q2 {

  public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] token = value.toString().split(",");
      if(!token[0].equals("Year")){
        String month = token[1];
        String day = token[3];
        String hour = "";
        
        if(token[6].length()<=2){
            hour = token[6];
        }
        else if(token[6].length()==3){
            hour = token[6].substring(0,1);
        }
        
        else{
          hour = token[6].substring(0,2);
        }

        if(!token[14].equals("NA")){
          int arrdelay = Integer.parseInt(token[14]);
          if(arrdelay < 0){
            arrdelay = 0;
          }
          String out = month + "--" + day + "--" + (Integer.parseInt(hour) % 24);
          context.write(new Text(out), new IntWritable(arrdelay));
        }
      }
    }
  }
  public static class MyCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val : values){
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
  }
  
  public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {
    private Text outvalue = new Text();
    private IntWritable outkey = new IntWritable();
    
    HashMap<String, Integer> monthMap = new HashMap<String, Integer>();
    HashMap<String, Integer> dayMap = new HashMap<String, Integer>();
    HashMap<String, Integer> hourMap = new HashMap<String, Integer>();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      for(IntWritable val : values){
        String[] token = key.toString().split("--");
        int delay = val.get();
        String month = token[0];
        String day = token[1];
        String hour = token[2];
        setMap(monthMap, month, delay);
        setMap(dayMap, day, delay);
        setMap(hourMap, hour, delay);


      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
    
        int monthMin = getmin(monthMap);
        int dayMin = getmin(dayMap);
        int hourMin = getmin(hourMap);
        
        for(String key : monthMap.keySet()){
            if(monthMap.get(key) == monthMin){
                context.write(new Text("Best Month: " + key), new IntWritable(monthMin));
            }
        }

        for(String key : dayMap.keySet()){
            if(dayMap.get(key) == dayMin){
                context.write(new Text("Best Day: " + key), new IntWritable(dayMin));
            }
        }
        
        for(String key : hourMap.keySet()){
            if(hourMap.get(key) == hourMin){
                context.write(new Text("Best Hour: " + key), new IntWritable(hourMin));
            }
        }
        
       
        int monthMax = getmax(monthMap);
        int dayMax = getmax(dayMap);
        int hourMax = getmax(hourMap);
    
    
        for(String key : monthMap.keySet()){
            if(monthMap.get(key) == monthMax){
                context.write(new Text("Worst Month: " + key), new IntWritable(monthMax));
            }
        }

        for(String key : dayMap.keySet()){
            if(dayMap.get(key) == dayMax){
                context.write(new Text("Worst Day: " + key), new IntWritable(dayMax));
            }
        }
        
        for(String key : hourMap.keySet()){
            if(hourMap.get(key) == hourMax){
                context.write(new Text("Worst Hour: " + key), new IntWritable(hourMax));
            }
        }   
    
    
    }
  }
  public static int getmax(HashMap<String, Integer> map){
    int max = (int)Collections.max(map.values());
    return max;
  }
  public static int getmin(HashMap<String, Integer> map){
    int min = (int)Collections.min(map.values());
    return min;
  }

  public static void setMap(HashMap<String, Integer> map, String key, int delay){
    if(!map.containsKey(key)){
        map.put(key, delay);
    }
    else{
        map.put(key, map.get(key) + delay);
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "job1");
    job1.setJarByClass(q1q2.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setCombinerClass(MyCombiner.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    System.exit(job1.waitForCompletion(true)?0:1);
  }
}
