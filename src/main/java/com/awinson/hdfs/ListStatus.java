package com.awinson.hdfs;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ListStatus {

  public static void main(String[] args) throws Exception {
    String uri = "hdfs://hadoop2:9000/";
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);


    FileStatus[] status = fs.listStatus(new Path("/test"));
    Path[] listedPaths = FileUtil.stat2Paths(status);
    for (Path p : listedPaths) {
      System.out.println(p);

    }
    //globStatust通配
    status = fs.globStatus(new Path("/test"));
    listedPaths = FileUtil.stat2Paths(status);
    for (Path p : listedPaths) {
      System.out.println(p);
    }

  }
}

