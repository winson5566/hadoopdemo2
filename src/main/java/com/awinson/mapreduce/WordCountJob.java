package com.awinson.mapreduce;// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv WordCountJob
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountJob {
  public static void main(String[] args) throws Exception {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(WordCountJob.class);

    //如果output的路径已存在则报错
    FileInputFormat.setInputPaths(job, new Path("/test/word*.txt"));
    FileOutputFormat.setOutputPath(job, new Path("/test/result"));

    //设置Map和Reduce的类
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    //如果Mapper的输出和Reduce的函数相同时,可以不set()
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    //map后进行combiner,减少带宽压力
    job.setCombinerClass(WordCountReducer.class);

    //提交作业并等待执行完成
    //verbose=true 生成详细输出
    System.exit(job.waitForCompletion(true)?0:1);
  }
}
