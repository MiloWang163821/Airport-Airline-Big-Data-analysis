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

public class q5 {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split(",");
            if (!token[0].equals("Year")) {
                String flightYear = token[0];
                String tailNum = token[10];
                if (!token[14].equals("NA") && !tailNum.equals("NA")) {
                    int arrdelay = Integer.parseInt(token[14]);
                    if (arrdelay < 0) {
                        context.write(new Text(tailNum), new Text("o" + flightYear));
                    }
                    //   else{
                    //     context.write(new Text(tailNum), new Text("d" + flightYear));
                    //   }
                }
            }
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split(",");
            if (token.length != 9)
                return;
            if (!token[0].equals("tailnum")) {
                String tailNum = token[0];
                if (!token[8].equals("None")) {
                    int year = Integer.parseInt(token[8]);
                    context.write(new Text(tailNum), new Text("m" + year));
                }
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, IntWritable> {

        HashMap<String, Integer> planesDataMap = new HashMap<String, Integer>(); //<tailNum, mYear>
        HashMap<String, HashMap<Integer, Integer>> flightsOntimeMap = new HashMap<String, HashMap<Integer, Integer>>(); // <tailNum, <oYear, oCount>>

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int oCount = 1;
            Iterator<Text> valuesIterator = values.iterator();
            while (valuesIterator.hasNext()) {
                String value = valuesIterator.next().toString();
                if (value.substring(0, 1).equals("m")) {
                    int mYear = Integer.parseInt(value.replaceAll("[\\D]", ""));
                    //context.write(new Text(key.toString()), new IntWritable(mYear));
                    planesDataMap.put(key.toString(), mYear);
                } 
                else if (value.substring(0, 1).equals("o")) {
                    int oYear = Integer.parseInt(value.replaceAll("[\\D]", ""));
                    if (!flightsOntimeMap.containsKey(key.toString())) {
                        HashMap<Integer, Integer> m = new HashMap<Integer, Integer>();
                        m.put(oYear, oCount);
                        flightsOntimeMap.put(key.toString(), m);
                    } else {
                        if (flightsOntimeMap.get(key.toString()).containsKey(oYear)) {
                            flightsOntimeMap.get(key.toString()).put(oYear,
                                    flightsOntimeMap.get(key.toString()).get(oYear) + 1);
                        } else {
                            flightsOntimeMap.get(key.toString()).put(oYear, oCount);
                        }
                    }
                }
                else return;
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            int oldPlaneDelayCount = 0;
            int newPlaneDelayCount = 0;

            // context.write(new Text(flightsOntimeMap.keySet().toString()), new IntWritable(1));
            //context.write(new Text(planesDataMap.keySet().toString()), new IntWritable(1));

            for(String tailNum : flightsOntimeMap.keySet()){
                if(planesDataMap.containsKey(tailNum)){
                    int mYear = planesDataMap.get(tailNum);
                    for(int oYear : flightsOntimeMap.get(tailNum).keySet()){
                        if( oYear - mYear > 20 ){
                            oldPlaneDelayCount += flightsOntimeMap.get(tailNum).get(oYear);
                        }
                        else{
                            newPlaneDelayCount += flightsOntimeMap.get(tailNum).get(oYear);
                        }
                    }
                }
            } 
            context.write(new Text("OldPlaneDelayCount:"), new IntWritable(oldPlaneDelayCount));
            context.write(new Text("NewPlaneDelayCount:"), new IntWritable(newPlaneDelayCount));

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(q5.class);
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
