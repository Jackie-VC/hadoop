package org.myorg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

public class RelativeFrequencyPair {

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
            pair.setValue("*");
            word = new Text(objectMapper.writeValueAsString(pair));
            context.write(word, one);
            count++;
          }
        }
      }
    }
  }

  public static class Reduce extends
      Reducer<Text, IntWritable, Pair<String, String>, DoubleWritable> {

    private static double total = 1d;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int s = 0;
      Iterator<IntWritable> iterator = values.iterator();
      while (iterator.hasNext()) {
        IntWritable c = iterator.next();
        s += c.get();
      }
      ObjectMapper objectMapper = new ObjectMapper();
      Pair<String, String> pair = objectMapper.readValue(key.toString(), Pair.class);

      if (pair.getValue().equals("*")) {
        total = s;
      } else {
        context.write(pair, new DoubleWritable(s / total));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "wordcount");
    job.setJarByClass(RelativeFrequencyPair.class);

    job.setOutputKeyClass(Text.class);
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

