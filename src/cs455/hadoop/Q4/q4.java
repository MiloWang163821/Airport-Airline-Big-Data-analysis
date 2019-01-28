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

public class q4 {

    public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split(",");
            if (!token[0].equals("Year")) {
                String carrier = token[8];
                if (!token[14].equals("NA")) {
                    int arrdelay = Integer.parseInt(token[14]);
                    if (arrdelay < 0) {
                        arrdelay = 0;
                    }
                    context.write(new Text(carrier), new IntWritable(arrdelay));
                }
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        HashMap<String, Integer> mostDelayMap = new HashMap<String, Integer>();
        HashMap<String, Integer> averageDelayMap = new HashMap<String, Integer>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalDelay = 0;
            int delayFlights = 0;
            for (IntWritable val : values) {
                totalDelay += val.get();
                delayFlights++;
            }
            mostDelayMap.put(key.toString(), delayFlights);
            averageDelayMap.put(key.toString(), totalDelay / delayFlights);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            int mostDelay = getmax(mostDelayMap);
            int averageDelay = getmax(averageDelayMap);

            for (String key : mostDelayMap.keySet()) {
                if (mostDelayMap.get(key) == mostDelay) {
                    context.write(new Text("Most delays carriers: " + key), new IntWritable(mostDelay));
                }
            }

            for (String key : averageDelayMap.keySet()) {
                if (averageDelayMap.get(key) == averageDelay) {
                    context.write(new Text("Higest average delays carriers: " + key), new IntWritable(averageDelay));
                }
            }

        }
    }

    public static int getmax(HashMap<String, Integer> map) {
        int max = (int) Collections.max(map.values());
        return max;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(q4.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        //job1.setCombinerClass(MyCombiner.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
