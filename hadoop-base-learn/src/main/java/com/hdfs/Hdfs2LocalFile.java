package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/4/2 17:25
 */
public class Hdfs2LocalFile {
    private static final String fs_default_name = "fs.default.name";

    private static final String url = "hdfs://localhost:8000";

    private static final String localPathName = "/my_test/input.txt";

    private static final String hadoopPathName = "/my_test/input_file.txt";

    public static void main(String[] args) throws Exception {


        FSDataInputStream in = null;
        OutputStream out = null;
        Configuration conf = new Configuration();
        conf.set(fs_default_name,url);
        try {
            FileSystem fs = FileSystem.get(URI.create(url), conf);
            in = fs.open(new Path(hadoopPathName));
            out = new FileOutputStream(localPathName);

            byte[] buffer = new byte[20];
            in.skip(100);
            int bytesRead = in.read(buffer);
            if (bytesRead >= 0) {
                out.write(buffer, 0, bytesRead);
            }
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
