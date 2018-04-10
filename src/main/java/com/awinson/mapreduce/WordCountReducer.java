package com.awinson.mapreduce;// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv WordCountReducer

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key
            , Iterable<LongWritable> values
            ,Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable i : values) {
            sum += i.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
