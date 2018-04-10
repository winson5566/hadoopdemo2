package com.awinson.mapreduce;// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv WordCountMapper

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /*
     * 继承Mapper类需要定义四个输出、输出类型泛型：
     * 四个泛型类型分别代表：
     * KeyIn        Mapper的输入数据的Key
     * ValueIn      Mapper的输入数据的Value，这里是每个文件的文本
     * KeyOut       Mapper的输出数据的Key，这里是每个单词
     * ValueOut     Mapper的输出数据的Value，这里是每个单词出现的次数
     *
     * Writable接口是一个实现了序列化协议的序列化对象。
     * 在Hadoop中定义一个结构化对象都要实现Writable接口，使得该结构化对象可以序列化为字节流，字节流也可以反序列化为结构化对象。
     * LongWritable类型:Hadoop.io对Long类型的封装类型
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split("\\W+");
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
