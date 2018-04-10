package com.awinson.mapreduce;// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv WordCountMapper

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split("\\W+");
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
