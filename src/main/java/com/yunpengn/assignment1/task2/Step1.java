package com.yunpengn.assignment1.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class Step1 {
  public static class Step1_ToItemPreMapper extends Mapper<Object, Text, IntWritable, Text> {
    private final static IntWritable k = new IntWritable();
    private final static Text v = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] tokens = Recommend.DELIMITER.split(value.toString());
      int userID = Integer.parseInt(tokens[0]);
      String itemID = tokens[1];
      String pref = tokens[2];
      k.set(userID);
      v.set(itemID+":"+pref);
      context.write(k, v);

    }
  }

  public static class Step1_ToUserVectorReducer extends Reducer <IntWritable, Text, IntWritable, Text> {
    private final static Text v = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
        Reducer<IntWritable, Text, IntWritable, Text>.Context context)
        throws IOException, InterruptedException {
      //Auto-generated method stub
      StringBuilder sb = new StringBuilder();
      for (Text value:values) {
        sb.append("," + value.toString());
      }
      v.set(sb.toString().replaceFirst(",", ""));
      context.write(key, v);
    }

  }

  public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    //get configuration info
    Configuration conf = Recommend.config();
    //get I/O path
    Path input = new Path(path.get("Step1Input"));
    Path output = new Path(path.get("Step1Output"));
    //upload local file

    HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
    hdfs.delFile(input);
    hdfs.mkDir(input);
    hdfs.copyLocalToHdfs(new Path(path.get("data")), input);

    //set job
    Job job = Job.getInstance(conf,"Step1");
    job.setJarByClass(Step1.class);

    job.setMapperClass(Step1_ToItemPreMapper.class);
    job.setCombinerClass(Step1_ToUserVectorReducer.class);
    job.setReducerClass(Step1_ToUserVectorReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job,input);
    FileOutputFormat.setOutputPath(job,output);
    //run the job
    job.waitForCompletion(true);
  }
}
