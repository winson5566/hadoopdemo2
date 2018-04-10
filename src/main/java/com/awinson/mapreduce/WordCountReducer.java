package com.awinson.mapreduce;// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv WordCountReducer

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
    /*
     * 继承Reducer类需要定义四个输出、输出类型泛型：
     * 四个泛型类型分别代表：
     * KeyIn        Reducer的输入数据的Key，单词
     * ValueIn      Reducer的输入数据的Value，这里是每个单词出现的次数
     * KeyOut       Reducer的输出数据的Key，单词
     * ValueOut     Reducer的输出数据的Value，每个单词出现的总次数
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,
                          Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable i : values) {
            sum += i.get();
        }
        context.write(key, new LongWritable(sum));
    }

}
