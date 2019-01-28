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
import java.util.TreeMap;
import java.util.Comparator;
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

public class q3 {

  public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] token = value.toString().split(",");
      if(!token[0].equals("Year")){
        String year = token[0];
        String origin = token[16];
        String dest = token[17];
        String yearOrigin = year + "--" + origin;
        String yearDest = year + "--" + dest;
        context.write(new Text(yearOrigin), new IntWritable(1));
        context.write(new Text(yearDest), new IntWritable(1));
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
  
    HashMap<String, HashMap<String, Integer>> flightNumberMap = new HashMap<String, HashMap<String, Integer>>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val : values){
            sum += val.get();
        }
        String[] token = key.toString().split("--");
        String year = token[0];
        String iaac = token[1];
                
        if(!flightNumberMap.containsKey(year)){
            HashMap<String, Integer> m = new HashMap<String, Integer>();
            m.put(iaac, sum);
            flightNumberMap.put(year, m);
        }
        else{
            flightNumberMap.get(year).put(iaac, sum);
        }
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(String key : flightNumberMap.keySet()){
            LinkedList list = sortValues(flightNumberMap.get(key));
            for(int i = 0; i < 10; i++){
                for(String key2 : flightNumberMap.get(key).keySet()){
                    if(flightNumberMap.get(key).get(key2) == list.get(i)){
                        context.write(new Text(key + " " + key2), new IntWritable(flightNumberMap.get(key).get(key2)));
                    }
                }
            }
        }
    }
  }
    public static LinkedList<Integer> sortValues(HashMap<String, Integer> map){
        LinkedList list = new LinkedList(map.values());
        Collections.sort(list);
        Collections.reverse(list);
        return list;
    }

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(q3.class);
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
