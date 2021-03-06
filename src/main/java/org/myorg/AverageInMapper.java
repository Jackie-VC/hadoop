package org.myorg;

import java.io.IOException;
import java.util.HashMap;
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

public class AverageInMapper {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private String firstToken;
    private int lastToken;
    private int beforeLastToken;
    private static java.util.Map<String, Integer> combiner = new HashMap<>();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String nextToken = "";
      while (tokenizer.hasMoreTokens()) {
        nextToken = tokenizer.nextToken();
        if (firstToken == null) {
          firstToken = nextToken;
        }
        beforeLastToken = lastToken;
        try{
        	lastToken = Integer.parseInt(nextToken);
        }catch(NumberFormatException e){
      	  	lastToken = beforeLastToken;
        }
      }
      if (combiner.containsKey(firstToken)) {
        lastToken = lastToken + combiner.get(firstToken);
      }
      combiner.put(firstToken, lastToken);
      word.set(firstToken);
      context.write(word, new IntWritable(combiner.get(firstToken)));
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int average = 0;
      int count = 1;
      for (IntWritable val : values) {
        average = (average + val.get()) / (count + 1);
        count++;
      }
      result.set(average);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "wordcount");
    job.setJarByClass(AverageInMapper.class);

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

