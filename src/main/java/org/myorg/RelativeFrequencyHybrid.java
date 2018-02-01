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

public class RelativeFrequencyHybrid {

  public static class Map extends Mapper<LongWritable, Text, Pair<String, String>, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      List<String> wordList = new ArrayList<>();
      while (tokenizer.hasMoreTokens()) {
        String nextToken = tokenizer.nextToken();
        wordList.add(nextToken);
      }

      for (int i = 0; i < wordList.size(); i++) {
        for (int j = 0; j < 2 && j < wordList.size() - i - 1; j++) {
          if (wordList.get(i).equals(wordList.get(j))) {
            Pair<String, String> pair = new Pair<>();
            pair.setKey(wordList.get(i));
            pair.setValue(wordList.get(j));
            context.write(pair, one);
          }
        }
      }
    }
  }

  public static class Reduce extends
      Reducer<Pair<String, String>, IntWritable, Text, java.util.Map<String, Double>> {

    private static double sum = 0d;
    private static String prevWord = "";
    private static java.util.Map<String, Double> wordCount = new HashMap<>();

    @Override
    protected void reduce(Pair<String, String> key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      Double valueTotal = 0d;
      Iterator<IntWritable> iterator = values.iterator();
      while (iterator.hasNext()) {
        valueTotal += iterator.next().get();
      }

      if (prevWord.isEmpty() || key.getKey().equals(prevWord)) {
        sum += valueTotal;
        wordCount.put(key.getValue(), valueTotal);
      } else if (!key.getKey().equals(prevWord)) {
        Iterator<Double> doubleIterator = wordCount.values().iterator();
        Double doubleTotal = 0d;
        while (doubleIterator.hasNext()) {
          doubleTotal += doubleIterator.next();
        }
        Iterator<Entry<String, Double>> entryIterator = wordCount.entrySet().iterator();
        while (entryIterator.hasNext()) {
          Entry<String, Double> entry = entryIterator.next();
          wordCount.put(entry.getKey(), entry.getValue() / doubleTotal);
        }
        context.write(new Text(key.getKey()), wordCount);
        wordCount = new HashMap<>();
      }

      prevWord = key.getKey();

    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "wordcount");
    job.setJarByClass(RelativeFrequencyHybrid.class);

    job.setOutputKeyClass(Pair.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }


}

