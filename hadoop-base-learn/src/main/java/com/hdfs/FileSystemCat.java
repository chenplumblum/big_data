package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/4/2 16:53
 */
public class FileSystemCat {

    private static final String fs_default_name = "fs.default.name";

    private static final String url = "hdfs://localhost:8000";

    private static final String pathString = "/my_test/input.txt";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(fs_default_name,url);
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(pathString));
            IOUtils.copyBytes(in, System.out, 4096, false);
            System.out.println(in);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}


