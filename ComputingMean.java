package dblab;
	
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ComputingMean {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      private Text word = new Text("Map");
      private Text num = new Text();

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String tuple = value.toString();
        num.set(tuple);
        output.collect(word, num);
      }
    }


    public static class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      private Text sum_cnt = new Text();
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        int sum = 0;
        int cnt = 0;
        String temp_sum;
        while (values.hasNext()) {
          temp_sum=values.next().toString();
          sum += Integer.parseInt(temp_sum);
          cnt += 1;
        }
        sum_cnt.set(sum+"_"+cnt);
        output.collect(key, sum_cnt);
      }
    }


    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      private Text mean_final=new Text();
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        int sum = 0;
        int cnt = 0;
        int temp_sum;
        int temp_cnt;
        while (values.hasNext()) {
          String sum_cnt = values.next().toString();
          String[] tokens = sum_cnt.split("_");
          temp_sum = Integer.parseInt(tokens[0]);
          temp_cnt = Integer.parseInt(tokens[1]);

          sum += temp_sum;
          cnt += temp_cnt;
        }
        double mean = (double)sum/(double)cnt;
        mean_final.set(Double.toString(mean));
        output.collect(key, mean_final);
      }
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(ComputingMean.class);
      conf.setJobName("computing mean");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Combiner.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
   }
}
