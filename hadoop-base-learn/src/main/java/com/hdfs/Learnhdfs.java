package com.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/4/1 16:11
 */
public class Learnhdfs {

    private static final String fs_default_name = "fs.default.name";

    private static final String url = "hdfs://localhost:8000";

    private static final String localPathName = "/my_test/input.txt";

    private static final String hadoopPathName = "/my_test/input_file.txt";

    private static final String hadoopFileFolder = "/my_test";

    /**
     * 配置
     */
    private Configuration conf = null;
    /**
     * 文件操作器:需要详细的看api
     */
    private FileSystem fs = null;



    @Before
    public void conn() throws Exception {
        conf = new Configuration(false);
        conf.set(fs_default_name, url);
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws Exception {
        fs.close();
    }


    @Test
    /**
     * 创建文件夹
     */
    public void mkdir() throws Exception {
        Path dir = new Path(hadoopFileFolder);
        if (!fs.exists(dir)) {
            fs.mkdirs(dir);
        }
    }


    @Test
    /**
     * 创建文件
     */
    public void uploadFile() throws Exception {
        Path file = new Path(hadoopPathName);
        FSDataOutputStream output = fs.create(file);
        InputStream input = new BufferedInputStream(new FileInputStream(new File(localPathName)));
        IOUtils.copyBytes(input, output, conf, true);
    }

    @Test
    /**
     * 读取txt文件
     */
    public void blk() throws Exception {
        Path file = new Path("/user/root/test.txt");
        FileStatus ffs = fs.getFileStatus(file);
        BlockLocation[] blks = fs.getFileBlockLocations(ffs, 0, ffs.getLen());
        for (BlockLocation b : blks) {
            System.out.println(b);
            HdfsBlockLocation hbl = (HdfsBlockLocation) b;
            System.out.println(hbl.getLocatedBlock().getBlock().getBlockId());
        }
        FSDataInputStream input = fs.open(file);

        System.out.println((char) input.readByte());

        input.seek(1048576);

        System.out.println((char) input.readByte());

    }


    @Test
    /**
     * 读取seq文件
     */
    public void seqfile() throws Exception {
        Path value = new Path("/haha.seq");
        IntWritable key = new IntWritable();
        Text val = new Text();
        SequenceFile.Writer.Option file = SequenceFile.Writer.file(value);
        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(val.getClass());

        SequenceFile.Writer writer = SequenceFile.createWriter(conf, file, keyClass, valueClass);

        for (int i = 0; i < 10; i++) {
            key.set(i);
            val.set("sxt..." + i);
            writer.append(key, val);

        }
        writer.hflush();
        writer.close();


        SequenceFile.Reader.Option infile = SequenceFile.Reader.file(value);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, infile);

        String name = reader.getKeyClassName();
        System.out.println(name);
    }
}
