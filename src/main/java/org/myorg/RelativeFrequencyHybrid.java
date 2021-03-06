package org.myorg;

import com.fasterxml.jackson.databind.ObjectMapper;
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

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

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

      ObjectMapper objectMapper = new ObjectMapper();

      for (int i = 0; i < wordList.size(); i++) {
        int count = 0;
        for (int j = i + 1; count < 2 && j < wordList.size() - i - 1; j++) {
          if (!wordList.get(i).equals(wordList.get(j))) {
            Pair<String, String> pair = new Pair<>();
            pair.setKey(wordList.get(i));
            pair.setValue(wordList.get(j));
            word = new Text(objectMapper.writeValueAsString(pair));
            context.write(word, one);
            count++;
          }
        }
      }
    }
  }

  public static class Reduce extends
      Reducer<Text, IntWritable, Text, java.util.Map<String, Double>> {

    private static double sum = 0d;
    private static String prevWord = "";
    private static java.util.Map<String, Double> wordCount = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      Double valueTotal = 0d;
      Iterator<IntWritable> iterator = values.iterator();
      while (iterator.hasNext()) {
        valueTotal += iterator.next().get();
      }

      ObjectMapper objectMapper = new ObjectMapper();
      Pair<String, String> pair = objectMapper.readValue(key.toString(), Pair.class);

      if (prevWord.isEmpty() || pair.getKey().equals(prevWord)) {
        // if the key of pair is the same, we should consider all of them as a group
        // and compute their total value sum.
        sum += valueTotal;

        // the value of pair indicates the neighbor, so we use neighborCount to record
        // neighbor's frequency
        wordCount.put(pair.getValue(), valueTotal);
      } else if (!pair.getKey().equals(prevWord)) {
        Iterator<Entry<String, Double>> entryIterator = wordCount.entrySet().iterator();
        while (entryIterator.hasNext()) {
          Entry<String, Double> entry = entryIterator.next();
          wordCount.put(entry.getKey(), entry.getValue() / sum);
        }
        context.write(new Text(pair.getKey()), wordCount);
        wordCount = new HashMap<>();
      }

      prevWord = pair.getKey();

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

