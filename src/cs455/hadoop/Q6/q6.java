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

public class q6 {

  public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] token = value.toString().split(",");
      if (!token[0].equals("Year")) {
        String origin = token[16];
        String dest = token[17];
        if (!token[25].equals("NA")) {
          int weatherDelay = Integer.parseInt(token[25]);
          if (weatherDelay > 0) {
            context.write(new Text(origin), new Text("1"));
            context.write(new Text(dest), new Text("1"));
          }
        }
      }
    }
  }

  public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] token = value.toString().split(",");
      if (token.length != 7)
        return;
      if (!token[0].equals("itat")) {
        String itat = token[0].replaceAll("[^A-Za-z0-9]","");
        String city = token[2].replaceAll("[^A-Za-z0-9]","");
        if(!itat.equals("NA") && !city.equals("NA")){
          context.write(new Text(itat), new Text(city));
        }
      }
    }
  }

  public static class Reducer1 extends Reducer<Text, Text, Text, IntWritable> {

    HashMap<String, String> airportsMap = new HashMap<String, String>(); //<itat, city>
    HashMap<String, Integer> weatherDelayMap = new HashMap<String, Integer>(); // <itat, count>

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      Iterator<Text> valuesIterator = values.iterator();
      while (valuesIterator.hasNext()) {
        String value = valuesIterator.next().toString();
        //context.write( key, new Text(value));
        if (!value.equals("1")) {
          airportsMap.put(key.toString(), value);
        }        
        else {
          if (!weatherDelayMap.containsKey(key.toString())) {
            weatherDelayMap.put(key.toString(), 1);
          } else {
            weatherDelayMap.put(key.toString(), weatherDelayMap.get(key.toString()) + 1);
          }
        }
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        LinkedList list = sortValues(weatherDelayMap);
        for (String itat : weatherDelayMap.keySet()) {
            for (int i = 0; i < 10; i++) {
              if (weatherDelayMap.get(itat) == list.get(i)) {
                context.write(new Text(airportsMap.get(itat)), new IntWritable(weatherDelayMap.get(itat)));
              }
            }
          }
        }
  }

  public static LinkedList<Integer> sortValues(HashMap<String, Integer> map) {
    LinkedList list = new LinkedList(map.values());
    Collections.sort(list);
    Collections.reverse(list);
    return list;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "job1");
    job1.setJarByClass(q6.class);
    //job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    //job1.setCombinerClass(MyCombiner.class);

    MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Mapper1.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Mapper2.class);

    FileOutputFormat.setOutputPath(job1, new Path(args[2]));

    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}
