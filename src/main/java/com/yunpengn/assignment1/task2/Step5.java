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

public class Step5 {
  public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String flag;
    private Text k;
    private Text v;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
      FileSplit split = (FileSplit) context.getInputSplit();

      // dataset
      flag = split.getPath().getParent().getName();
    }

    @Override
    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
      //you can use provided SortHashMap.java or design your own code.
      // TODO
    }
  }

  public static class Step5_FilterSortReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      //you can use provided SortHashMap.java or design your own code.
      // TODO
    }
  }

  public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    //get configuration info
    Configuration conf = Recommend.config();
    // I/O path
    Path input1 = new Path(path.get("Step5Input1"));
    Path input2 = new Path(path.get("Step5Input2"));
    Path output = new Path(path.get("Step5Output"));
    // delete last saved output
    HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
    hdfs.delFile(output);
    // set job
    Job job = Job.getInstance(conf);
    job.setJarByClass(Step5.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Step5_FilterSortMapper.class);
    job.setReducerClass(Step5_FilterSortReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, input1, input2);
    FileOutputFormat.setOutputPath(job, output);

    job.waitForCompletion(true);
  }
}
