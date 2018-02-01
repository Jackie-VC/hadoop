package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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

      // put all the words in a list
      List<String> wordList = new ArrayList<>();
      while (tokenizer.hasMoreTokens()) {
        String nextToken = tokenizer.nextToken();
        wordList.add(nextToken);
      }

      // find the corresponding neighbors of every word in list
      // put neighbors and their frequency into a map
      for (int i = 0; i < wordList.size(); i++) {
        int count = 0;
        java.util.Map<String, IntWritable> strip = new HashMap<>();
        for (int j = i + 1; count < 2 && j < wordList.size() - i - 1; j++) {
          if (!wordList.get(i).equals(wordList.get(j))) {
            String currentWord = wordList.get(j);
            if (strip.containsKey(currentWord)) {
              strip.put(currentWord, new IntWritable(strip.get(currentWord).get() + one.get()));
            } else {
              strip.put(currentWord, one);
            }
            count++;
          }
        }

        // output the word as key, neighbors map as value
        context.write(new Text(wordList.get(i)), strip);
      }
    }
  }

  public static class Reduce extends
      Reducer<Text, java.util.Map<String, IntWritable>, Text, java.util.Map<String, DoubleWritable>> {

    @Override
    protected void reduce(Text key, Iterable<java.util.Map<String, IntWritable>> values,
        Context context)
        throws IOException, InterruptedException {

      double sum = 0d;
      java.util.Map<String, DoubleWritable> newMap = new HashMap<>();
      Iterator<java.util.Map<String, IntWritable>> mapIterator = values.iterator();
      while (mapIterator.hasNext()) {
        java.util.Map<String, IntWritable> next = mapIterator.next();
        Iterator<Entry<String, IntWritable>> iterator = next.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<String, IntWritable> next1 = iterator.next();
          sum += next1.getValue().get();
          if (newMap.containsKey(next1.getKey())) {
            double newValue = newMap.get(next1.getKey()).get() + next1.getValue().get();
            newMap.put(next1.getKey(), new DoubleWritable(newValue));
          }
        }
      }

      for (Entry<String, DoubleWritable> entry : newMap.entrySet()) {
        newMap.put(entry.getKey(), new DoubleWritable(entry.getValue().get() / sum));
      }
      context.write(key, newMap);

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

