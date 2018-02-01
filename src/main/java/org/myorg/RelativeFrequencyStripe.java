package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RelativeFrequencyStripe {

  public static class Map extends
      Mapper<LongWritable, Text, Text, java.util.Map<String, IntWritable>> {

    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      java.util.Map<String, IntWritable> strip = new HashMap<>();
      while (tokenizer.hasMoreTokens()) {
        String nextToken = tokenizer.nextToken();
        if (strip.containsKey(nextToken)) {
          strip.put(nextToken, new IntWritable(strip.get(nextToken).get() + one.get()));
        } else {
          strip.put(nextToken, one);
        }
        context.write(new Text(nextToken), strip);
      }
    }
  }

  public static class Reduce extends
      Reducer<Text, java.util.Map<String, IntWritable>, Text, java.util.Map<Text, java.util.Map<String, DoubleWritable>>> {

    @Override
    protected void reduce(Text key, Iterable<java.util.Map<String, IntWritable>> values,
        Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      java.util.Map<Text, java.util.Map<String, DoubleWritable>> hf = new HashMap<>();

      Iterator<java.util.Map<String, IntWritable>> iterator = values.iterator();
      while (iterator.hasNext()) {
        java.util.Map<String, IntWritable> strip = iterator.next();
        Iterator<IntWritable> iteratorSum = strip.values().iterator();
        while (iteratorSum.hasNext()) {
          sum = +iteratorSum.next().get();
        }
      }

      iterator = values.iterator();
      while (iterator.hasNext()) {
        java.util.Map<String, IntWritable> strip = iterator.next();
        java.util.Map<String, DoubleWritable> stripDouble = new HashMap<>();
        Set<Entry<String, IntWritable>> entries = strip.entrySet();
        for (Entry<String, IntWritable> entry : entries) {
          stripDouble.put(entry.getKey(), new DoubleWritable(entry.getValue().get() / sum));
        }
        hf.put(key, stripDouble);
      }

      context.write(key, hf);
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "wordcount");
    job.setJarByClass(RelativeFrequencyStripe.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(java.util.Map.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }


}

