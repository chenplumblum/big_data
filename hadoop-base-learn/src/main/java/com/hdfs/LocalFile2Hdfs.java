package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/4/2 17:04
 */
public class LocalFile2Hdfs {

    private static final String fs_default_name = "fs.default.name";

    private static final String url = "hdfs://localhost:8000";

    private static final String localPathName = "/my_test/input.txt";

    private static final String hadoopPathName = "/my_test/input_file.txt";

    public static void main(String[] args) throws Exception {

        // 获取读取源文件和目标文件位置参数

        FileInputStream in = null;
        OutputStream out = null;
        Configuration conf = new Configuration();
        conf.set(fs_default_name,url);
        try {
            // 获取读入文件数据

            // 获取目标文件信息
            FileSystem fs = FileSystem.get(URI.create(url), conf);
            out = fs.create(new Path(hadoopPathName), new Progressable() {
                @Override
                public void progress() {
                    System.out.println("*");
                }
            });
            in = new FileInputStream(new File(localPathName));

            // 跳过前100个字符
            in.skip(10);
            byte[] buffer = new byte[20];

            // 从101的位置读取20个字符到buffer中
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
