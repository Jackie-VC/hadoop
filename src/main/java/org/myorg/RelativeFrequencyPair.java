package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RelativeFrequencyPair {

  public static class Map extends Mapper<LongWritable, Text, MapPairString, IntWritable> {

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

      for (int i = 0; i < wordList.size(); i++) {
        int count = 0;
        for (int j = i + 1; count < 2 && j < wordList.size(); j++) {
          if (!wordList.get(i).equals(wordList.get(j))) {
            MapPairString pair = new MapPairString();
            pair.key = new Text(wordList.get(i));
            pair.value = new Text(wordList.get(j));
            context.write(pair, one);
            pair.value = new Text("*");
            context.write(pair, one);
            count++;
          } else {
            break;
          }
        }
      }
    }
  }

  public static class Reduce extends
      Reducer<MapPairString, IntWritable, MapPairString, DoubleWritable> {

    private static double total = 1d;

    @Override
    protected void reduce(MapPairString key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int s = 0;
      for (IntWritable c : values) {
        s += c.get();
      }

      if (key.value.toString().equals("*")) {
        total = s;
      } else {
        context.write(key, new DoubleWritable(s / total));
      }
    }
  }

  static class MapPair extends Pair<Text, IntWritable> implements WritableComparable<MapPair> {

    public MapPair() {
    }

    @Override
    public int compareTo(MapPair o) {
      if (o == null) {
        return 1;
      }
      if (key.compareTo(o.key) == 0) {
        return value.compareTo(o.value);
      } else {
        return key.compareTo(o.key);
      }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      key.write(dataOutput);
      value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      key.readFields(dataInput);
      value.readFields(dataInput);
    }
  }

  static class MapPairString extends Pair<Text, Text> implements WritableComparable<MapPairString> {

    public MapPairString() {
      this.key = new Text();
      this.value = new Text();
    }

    @Override
    public int compareTo(MapPairString o) {
      if (o == null) {
        return 1;
      }
      if (key.compareTo(o.key) == 0) {
        return value.compareTo(o.value);
      } else {
        return key.compareTo(o.key);
      }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.value.readFields(dataInput);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "relativefrequency");
    job.setJarByClass(RelativeFrequencyPair.class);

    job.setOutputKeyClass(MapPairString.class);
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

