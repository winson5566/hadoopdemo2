package com.awinson.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Mkdir {
    public static void main(String[] args) throws Exception {
        String uri = "hdfs://hadoop2:9000/";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        //创建
        if (fs.mkdirs(new Path("/test1/result1"))) {
            System.out.println("创建成功");
        } else {
            System.out.println("创建失败");
        }
        //删除
        fs.delete(new Path("/test1/result1"),true);
    }
}
