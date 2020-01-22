package com.yunpengn.assignment1.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class Step2 {
  public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static Text k = new Text();
    private final static IntWritable v = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
      String[] tokens = Recommend.DELIMITER.split(values.toString());
      // TODO
    }
  }

  public static class Step2_UserVectorToConoccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      // TODO
    }
  }

  public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    //get configuration info
    Configuration conf = Recommend.config();
    //get I/O path
    Path input = new Path(path.get("Step2Input"));
    Path output = new Path(path.get("Step2Output"));
    //delete last saved output
    HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
    hdfs.delFile(output);
    //set job
    Job job = Job.getInstance(conf, "Step2");
    job.setJarByClass(Step2.class);

    job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
    job.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
    job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    //run job
    job.waitForCompletion(true);
  }
}
