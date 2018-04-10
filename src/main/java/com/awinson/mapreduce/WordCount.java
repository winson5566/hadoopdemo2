package com.awinson.mapreduce;// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv WordCount
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static void main(String[] args) throws Exception {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(WordCount.class);

    FileInputFormat.setInputPaths(job, new Path("/test/word*.txt"));
    FileOutputFormat.setOutputPath(job, new Path("/test/result"));

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setCombinerClass(WordCountReducer.class);

    // 执行提交job方法，verbose=true:打印进度和详情
    job.waitForCompletion(true);
    System.out.println("Finished");
  }
}
