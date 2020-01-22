package com.yunpengn.assignment1.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class Step4_1 {
  public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {
    // you can solve the co-occurrence Matrix/left matrix and score matrix/right matrix separately
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      FileSplit split = (FileSplit) context.getInputSplit();
      String flag = split.getPath().getParent().getName();// data set
    }

    @Override
    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
      String[] tokens = Recommend.DELIMITER.split(values.toString());
      // TODO
    }
  }

  public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // TODO
    }
  }

  public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    //get configuration info
    Configuration conf = Recommend.config();
    // get I/O path
    Path input1 = new Path(path.get("Step4_1Input1"));
    Path input2 = new Path(path.get("Step4_1Input2"));
    Path output = new Path(path.get("Step4_1Output"));
    // delete last saved output
    HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
    hdfs.delFile(output);
    // set job
    Job job = Job.getInstance(conf);
    job.setJarByClass(Step4_1.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Step4_1.Step4_PartialMultiplyMapper.class);
    job.setReducerClass(Step4_1.Step4_AggregateReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, input1, input2);
    FileOutputFormat.setOutputPath(job, output);

    job.waitForCompletion(true);
  }
}
