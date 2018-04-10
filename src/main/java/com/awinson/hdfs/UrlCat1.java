package com.awinson.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

public class UrlCat1 {

  /**
   * FileSystem的open方法返回FSDataInputStream,可访问任意的位置
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop2:9000"),conf,"winson");
    FSDataInputStream in = null;
    try {
      String uri = "hdfs://hadoop2:9000/test/words1.txt";
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
      in.seek(0);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
