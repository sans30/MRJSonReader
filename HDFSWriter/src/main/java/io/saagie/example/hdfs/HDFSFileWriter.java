package io.saagie.example.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by santhoshpc on 06-03-2018.
 */
public class HDFSFileWriter extends  Configured implements Tool {

    public static final String FS_PARAM_NAME = "fs.defaultFS";

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("HdfsWriter [local input path] [hdfs output path]");
            return 1;
        }
        String hdfsuri = "http://hadoop2-m:9870";
        String localInputPath = args[0];
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        // Set FileSystem URI
        //conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        //conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        FileSystem fs = FileSystem.get(conf);
        //FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
        if (fs.exists(outputPath)) {
            System.err.println("output path exists");
            return 1;
        }
        OutputStream os = fs.create(outputPath);
        InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
        IOUtils.copyBytes(is, os, conf);
        return 0;
    }

    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HDFSFileWriter(), args);
        System.exit(returnCode);
    }
}
