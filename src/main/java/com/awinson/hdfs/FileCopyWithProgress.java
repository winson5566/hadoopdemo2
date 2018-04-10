package com.awinson.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class FileCopyWithProgress {
  public static void main(String[] args) throws Exception {
    //配置FileSystem
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    //InputStream
    String localSrc = "/home/winson/Downloads/Hadoop-WordCount/input/Word_Count_input.txt";
    InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

    //OutputStream
    String dst = "hdfs://hadoop2:9000/test/words_java.txt";
    OutputStream out = fs.create(new Path(dst), new Progressable() {
      public void progress() {
        System.out.print(".");
      }
    });

    //copy
    IOUtils.copyBytes(in, out, 4096, true);
  }
}
